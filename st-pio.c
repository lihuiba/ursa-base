#include <stdlib.h>
#include <inttypes.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <libaio.h>
#include <sys/eventfd.h>
#include <st.h>
#include "st_pio.h"
#include "log.h"

#define IODEPTH 64

static int efd, running;
static io_context_t aio_ctx;
static st_thread_t polling_thread;
static st_cond_t on_complete;

struct my_iocb
{
	struct iocb io;
	ssize_t ret;
};

static ssize_t st_io_submit(struct my_iocb* iocb)
{
	io_set_eventfd(&iocb->io, efd);
	iocb->io.data = st_thread_self();
	struct iocb *piocb = &iocb->io;

	while(true)
	{
		int ret = io_submit(aio_ctx, 1, &piocb);
		if (ret < 0)
		{
			if (ret == -EAGAIN)
			{
				st_cond_wait(on_complete);	// wait for some io completion
				continue;
			}
			if (ret == -EBADF)
			{
				LOG_ERROR("ret == -EBADF %d %m", iocb->io.aio_fildes);
				return ret;
			}
			if (ret == -EFAULT)
			{
				LOG_ERROR("ret == -EFAULT %d %m %p", iocb->io.aio_fildes, iocb->io.u.c.buf);
				return ret;
			}
			if (ret == -EINVAL)
			{
				LOG_ERROR("ret == -EINVAL");
				return ret;
			}
			if (errno == EINTR)
			{
				st_sleep(1);
				LOG_WARN("iocb->aio_lio_opcode %d, errno == EINTR, continue", iocb->io.aio_lio_opcode);
				LOG_WARN("error io_submit ret is %d, %m iocb->u.c.offset %ld iocb->u.c.nbytes %ld, errno is %d", ret, iocb->io.u.c.offset, iocb->io.u.c.nbytes, errno);
				continue;
			}
			LOG_ERROR("error io_submit ret is %d, %m iocb->u.c.offset %ld iocb->u.c.nbytes %ld, errno is %d", ret, iocb->io.u.c.offset, iocb->io.u.c.nbytes, errno);
			return ret;
		}
		break;
	}

#ifndef ST_IO_TIMEOUT
	st_sleep(-1);
#else
	int r = st_sleep(6); // todo 6s ?
	if(unlikely(r == 0)){
		LOG_WARN("io submit fd %d, offset %ld nbyte %ld timeout_6s!!!!!", iocb->io.aio_fildes, iocb->io.u.c.offset, iocb->io.u.c.nbytes);
		struct io_event cancel_ret;
		int rc = io_cancel(aio_ctx, piocb, &cancel_ret);
		LOG_INFO("io_cancel ret %d", rc);
		return -1;
	}
#endif

	return iocb->ret;
}

ssize_t st_pread(int fd, void* buf, size_t count, off_t offset)
{
	struct my_iocb my_iocb;
	io_prep_pread(&my_iocb.io, fd, buf, count, offset);
	return st_io_submit(&my_iocb);
}

ssize_t st_pwrite(int fd, const void *buf, size_t count, off_t offset)
{
	struct my_iocb my_iocb;
	io_prep_pwrite(&my_iocb.io, fd, (void*)buf, count, offset);
	return st_io_submit(&my_iocb);
}

ssize_t st_preadv(int fd, const struct iovec *iov, int iovcnt, off_t offset)
{
	struct my_iocb my_iocb;
	io_prep_preadv(&my_iocb.io, fd, iov, iovcnt, offset);
	return st_io_submit(&my_iocb);
}

ssize_t st_pwritev(int fd, const struct iovec *iov, int iovcnt, off_t offset)
{
	struct my_iocb my_iocb;
	io_prep_pwritev(&my_iocb.io, fd, iov, iovcnt, offset);
	return st_io_submit(&my_iocb);
}

int st_fsync(int fd)
{
	struct my_iocb my_iocb;
	io_prep_fsync(&my_iocb.io, fd);
	return st_io_submit(&my_iocb);
}

static inline ssize_t io_event_ret(struct io_event *ev)
{
    return (ssize_t)(((uint64_t)ev->res2 << 32) | ev->res);
}

static void* st_pio_polling()
{
	running = 1;
	while(running == 1)
	{
		struct pollfd pd;
		pd.fd = efd;
		pd.revents = 0;
		pd.events = POLLIN;
		int ret = st_poll(&pd, 1, ST_UTIME_NO_TIMEOUT);
		if (unlikely(ret < 0))
		{
			if (likely(errno == EINTR))		// interrupted by st_thread_interrupt()
			{
				continue;
			}
			if (likely(pd.revents & POLLNVAL)) 
			{
				LOG_ERROR("POLL error got, st_io down?");
				return NULL;
			}
		}
		if (unlikely(ret == 0)) 
		{
			LOG_ERROR("st_pio event loop error: st_pool() should not timeout!!");
			st_usleep(10*1000);
			continue;
		}

		assert(ret == 1);
		if (unlikely(!(pd.revents & POLLIN)))
		{
			LOG_ERROR("st_pio event loop error: (pd.revents & POLLIN) should be true!!");
			st_usleep(10*1000);
			continue;
		}

		uint64_t nevents;
		ret = read(efd, &nevents, sizeof(nevents));
		if (unlikely(ret < (int)sizeof(nevents)))
		{
			LOG_ERROR("st_pio event loop error: read() from efd should return %d, actually %d!!", sizeof(nevents), ret);
			st_usleep(10*1000);
			continue;
		}
		LOG_DEBUG("read event fd is %lu", nevents);
		if(nevents > IODEPTH){
			LOG_FATAL("read event fd is %lu", nevents);
		}

		while (nevents > 0)
		{
			struct io_event events[IODEPTH];
			int n = io_getevents(aio_ctx, 1, MIN(nevents, IODEPTH), events, NULL);
			if (n < 0)
			{
				if (likely(errno == EINTR))
					continue;
				LOG_ERROR("st_pio event loop error: io_getevents() returned %d, errno=%d (%s)", n, errno, strerror(errno));
	 			st_usleep(10*1000);
				continue;
			}
			LOG_DEBUG("io_getevents ret n is %d", n);
			for (int i=0; i<n; ++i)
			{
				struct my_iocb* my_iocb = container_of(events[i].obj, struct my_iocb, io);
				my_iocb->ret = io_event_ret(&events[i]);
				st_thread_interrupt(events[i].data);
				st_sleep(0);
			}
			nevents -= n;
		}
		st_cond_broadcast(on_complete);
	}

	goto exit;
exit:
	running = -1;
	return NULL;
}

int st_pio_init()
{
	int ret;
	efd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
	if (efd < 0)
	{
		LOG_ERROR("%s(): failed to create eventfd %d (%s)", __func__, errno, strerror(errno));
		return efd;
	}

	ret = io_setup(IODEPTH, &aio_ctx);
	if (ret < 0)
	{
		LOG_ERROR("%s(): failed to create aio context by io_setup() %d (%s)", __func__, errno, strerror(errno));
		close(efd);
		return ret;
	}
	on_complete = st_cond_new();
	if (!on_complete)
	{
		// should not be failed
	}

	polling_thread = st_thread_create(st_pio_polling, NULL, 0, ST_STACK_SIZE);
	if (!polling_thread)
	{
		// should not be failed
	}
	return 0;
}

void st_pio_fini()
{
	if (running != 1 || polling_thread == NULL)
		return;

	LOG_INFO("start");
	running = 0;
	st_thread_interrupt(polling_thread);
	while (running != -1){		// wait polling thread to exit
		LOG_INFO("running != -1 running is %d", running);
		st_usleep(10*1000);
	}

	st_cond_destroy(on_complete);

	io_destroy(aio_ctx);
	close(efd);
}

