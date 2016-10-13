#ifndef __ST_REDIS__
#define __ST_REDIS__


#include <stdbool.h>
#include <st.h>
#include <hiredis/hiredis.h>
#include <hiredis/async.h>

struct redisAsyncContext;

struct RedisSTAttach
{
	char *ip;
	uint16_t port;
	int err;
	char *errstr;
	struct redisAsyncContext* c;
	st_thread_t polling_thread;
	redisReplyObjectFunctions redis_functions;
	redisReplyObjectFunctions* saved_functions;
	void (*saved_freeObject)(void*);
	bool running, reading, writing;
};

struct RedisSTAttach* redis_st_attach(struct redisAsyncContext* c);
struct RedisSTAttach* redis_sync_connect(char* ip, uint16_t port);
struct RedisSTAttach* redis_sync_connect_elastic(char* ip, uint16_t port);

struct redisReply* redisSyncCommandv(struct RedisSTAttach *ac, const char* format, va_list arglist);
struct redisReply* redisSyncCommand(struct RedisSTAttach *ac, const char* format, ...);
void st_redis_freeObject(struct RedisSTAttach* ra, void* obj);

void redis_st_deattach(struct RedisSTAttach* rc);
void redis_sync_free(struct RedisSTAttach* ra);

#endif

