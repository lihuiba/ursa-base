#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <inttypes.h> 
#include <stdbool.h>
#include <errno.h>
#include <st.h>
#include <hiredis/hiredis.h>
#include <hiredis/async.h>
#include "st-redis.h"
#include "log.h"


static void connectCallback(const redisAsyncContext *c, int status) 
{
	if (unlikely(status != REDIS_OK)) 
	{
		LOG_ERROR("redis connect error: %s", c->errstr);
		return;
	}
	LOG_DEBUG("redis connected");
}

static void disconnectCallback(const redisAsyncContext *c, int status) 
{
	((struct RedisSTAttach*)c->ev.data)->c = NULL;

	if (unlikely(status != REDIS_OK)) 
	{
		LOG_ERROR("Redis disconnect error: %s", c->errstr);
		return;
	}
	LOG_DEBUG("redis disconnected");
}

struct Issue
{
	st_thread_t thread;
	struct redisReply* reply;
};

static void commandCallback(redisAsyncContext* c, void* reply, void* privdata) 
{
	(void)c;
	struct Issue* issue = privdata;
	issue->reply = reply;
	st_thread_interrupt(issue->thread);	// interrupt caller' sleep (resume)
}

struct redisReply* redisSyncCommandv(struct RedisSTAttach *ac, const char* format, va_list arglist)
{
	struct Issue issue;
	issue.reply = NULL;

	if (unlikely(NULL == ac) || unlikely(NULL == ac->c))
	{ 
		LOG_ERROR("redisSyncCommandv process error, return");
		return NULL;
	}

	int ret = redisvAsyncCommand(ac->c, &commandCallback, &issue, format, arglist);
	if (unlikely(ret != REDIS_OK))
		return NULL;

	issue.thread = st_thread_self();
	st_sleep(-1);	// sleep forever (suspend) to wait for reply
	return issue.reply;
}

struct redisReply* redisSyncCommand(struct RedisSTAttach *ac, const char* format, ...)
{
	va_list arglist;
	va_start( arglist, format );
	struct redisReply* ret = redisSyncCommandv(ac, format, arglist);
	va_end( arglist );
	return ret;
}

static void* redis_st_polling_thread(void* privdata)
{
	struct pollfd pd;
	struct RedisSTAttach* ra = privdata;
	ra->polling_thread = st_thread_self();
	pd.fd = ra->c->c.fd;

	while(ra->running)
	{
		pd.revents = 0;
		pd.events = 0;
		if (ra->reading)
			pd.events |= POLLIN;
		if (ra->writing)
			pd.events |= POLLOUT;
		
		if (unlikely(pd.events == 0))
		{
			int ret = st_sleep(-1);
			if (likely(ret < 0 && errno == EINTR))
				continue;

			LOG_ERROR("redis st event loop error: st_sleep() should not timeout!!");
			st_usleep(10*1000);
			continue;
		}

		int ret = st_poll(&pd, 1, ST_UTIME_NO_TIMEOUT);
		if (unlikely(ret < 0))
		{
			if (likely(errno == EINTR))		// interrupted by st_thread_interrupt()
			{
				LOG_DEBUG("errno == EINTR");
				continue;
			}
			LOG_ERROR("ret %d < 0 pd.revents %x pd.events %d errno %d %m", ret, pd.revents, pd.events, errno);
			pd.revents = pd.events;

//			if (likely(pd.revents & POLLNVAL))
//			{
//				LOG_ERROR("POLL error got, redis down?");
//				errno = EBADF;
//				return NULL;
//			}
//			if (likely(pd.revents & POLLERR))
//			{
//				LOG_ERROR("POLL error got, POLLERR");
//				errno = EBADF;
//				return NULL;
//			}
//			if (likely(pd.revents & POLLHUP))
//			{
//				LOG_ERROR("POLL error got, POLLHUP");
//				errno = EBADF;
//				return NULL;
//			}
//			LOG_ERROR("ret %d < 0 pd.revents %x", ret, pd.revents);
//			return NULL;
		}
		if (unlikely(ret == 0)) 
		{
			LOG_ERROR("redis st event loop error: st_pool() should not timeout!!");
			st_usleep(10*1000);
			continue;
		}

		if (pd.revents & POLLIN)
		{
			if(ra->c) // check about
				redisAsyncHandleRead(ra->c);
		}
		if (pd.revents & POLLOUT)
		{
			if(ra->c)
				redisAsyncHandleWrite(ra->c);
		}
	}
	LOG_INFO("redis_st_polling_thread exit");
	return NULL;
}


static void redis_st_add_read(void *privdata)
{
	struct RedisSTAttach* ra = privdata;
	if (unlikely(ra->reading))
		return;

	ra->reading = true;
	st_thread_interrupt(ra->polling_thread);
}

static void redis_st_del_read(void *privdata)
{
	struct RedisSTAttach* ra = privdata;
	if (unlikely(!ra->reading))
		return;

	ra->reading = false;
	st_thread_interrupt(ra->polling_thread);
}

static void redis_st_add_write(void *privdata)
{
	struct RedisSTAttach* ra = privdata;
	if (unlikely(ra->writing))
		return;

	ra->writing = true;
	st_thread_interrupt(ra->polling_thread);
}

static void redis_st_del_write(void *privdata)
{
	struct RedisSTAttach* ra = privdata;
	if (unlikely(!ra->writing))
		return;

	ra->writing = false;
	st_thread_interrupt(ra->polling_thread);
}

static void redis_st_cleanup(void *privdata)
{
	struct RedisSTAttach* ra = privdata;
	if (unlikely(!ra->reading && !ra->writing))
		return;

	ra->reading = ra->writing = false;
	st_thread_interrupt(ra->polling_thread);
}

void st_redis_freeObject(struct RedisSTAttach* ra, void* obj)
{
	if (likely(ra && ra->saved_freeObject))
		ra->saved_freeObject(obj);
	else 
		free(obj);
}

static void freeObject_noop(void* data) 
{
	(void)data;
	//LOG_DEBUG("freeObject: noop");
}

struct RedisSTAttach* redis_st_attach(struct redisAsyncContext* ac)
{
	/* Nothing should be attached when something is already attached */
	if (unlikely(ac->ev.data != NULL))
		return NULL;

	struct RedisSTAttach* ra = malloc(sizeof(*ra));
	ra->reading = false;
	ra->writing = false;
	ra->c = ac;
	ra->ip = ac->c.tcp.host;
	ra->port = ac->c.tcp.port;
	ra->err = ac->c.err;
	ra->errstr = ac->c.errstr;

	ra->saved_functions = ac->c.reader->fn;
	ra->saved_freeObject = ac->c.reader->fn->freeObject;
	// replace existing freeObject() by noop, and the users 
	// should use st_redis_freeObject() instead
	memcpy(&ra->redis_functions, ac->c.reader->fn, sizeof(ra->redis_functions));
	ra->redis_functions.freeObject = &freeObject_noop;
	ac->c.reader->fn = &ra->redis_functions;

	/* Register functions to start/stop listening for events */
	ac->ev.addRead = redis_st_add_read;
	ac->ev.delRead = redis_st_del_read;
	ac->ev.addWrite = redis_st_add_write;
	ac->ev.delWrite = redis_st_del_write;
	ac->ev.cleanup = redis_st_cleanup;
	ac->ev.data = ra;

	ra->running = true;
	ra->polling_thread = st_thread_create(redis_st_polling_thread, ra, 0, ST_STACK_SIZE);

	redisAsyncSetConnectCallback(ac, connectCallback);
	redisAsyncSetDisconnectCallback(ac, disconnectCallback);	

	return ra;
}

void redis_st_deattach(struct RedisSTAttach* ra)
{
	ra->running = false;
	ra->ip = NULL;
	ra->port = 0;
	ra->err = 0;
	ra->errstr = NULL;
	if(ra->c)
		ra->c->c.reader->fn = ra->saved_functions;
	st_thread_interrupt(ra->polling_thread);
	st_sleep(0);
	free(ra);
}

struct RedisSTAttach* redis_sync_connect_elastic(char* ip, uint16_t port)
{
	struct RedisSTAttach *attach = NULL;
	MAKE_FD_ELASTIC(
		attach = redis_sync_connect(ip, port),
		"FAILED TO CONNECT TO REDIS SERVER", NULL
	);

	return attach;
}

struct RedisSTAttach* redis_sync_connect(char* ip, uint16_t port)
{
	struct RedisSTAttach *attach = NULL;
	struct redisReply *r = NULL;
	redisAsyncContext* ac = redisAsyncConnect(ip, port);

	if (unlikely(!ac)) {
		LOG_ERROR("redisAsyncConnect return NULL");
		return NULL;
	}
	if (ac->err) {
		LOG_ERROR("error ac->err %d %m", ac->err);
		redisAsyncFree(ac);
		return NULL;
	}
	
	attach = redis_st_attach(ac);
	r = redisSyncCommand(attach, "TIME");
	if (unlikely(!r)) {
		attach->c = NULL;
		redis_sync_free(attach);
		return NULL;
	}
	st_redis_freeObject(attach, r);

	return attach;
}

void redis_sync_free(struct RedisSTAttach* ra)
{
	if(ra->c){
		redisAsyncFree(ra->c);
		ra->c = NULL;
	}
	redis_st_deattach(ra);
}


