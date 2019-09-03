//
// Created by leipeng on 2019-08-22.
//

#include "fiber_aio.hpp"
#include <boost/fiber/all.hpp>

#if BOOST_OS_LINUX
  #include <libaio.h> // linux native aio
#endif

#if BOOST_OS_WINDOWS
  // not supported now
#else
  #include <aio.h> // posix aio
  #include <sys/types.h>
  #include <sys/mman.h>
#endif

namespace terark {

#if defined(__GNUC__)
    #pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#endif

#if BOOST_OS_LINUX

///@returns
//  0: posix aio
//  1: linux aio immediate mode (default)
//  2: linux aio batch     mode
//  3: io uring (not supported now)
static int get_aio_method() {
  const char* env = getenv("aio_method");
  if (env) {
    int val = atoi(env);
    return val;
  }
  return 1; // default
}
static int g_aio_method = get_aio_method();

static std::atomic<size_t> g_ft_num;

class io_fiber_context {
  static const int io_batch = 128;
  static const int reap_batch = 32;
  enum class state {
    ready,
    running,
    stopping,
    stopped,
  };
  volatile state       m_state;
  size_t               ft_num;
  size_t               idle_cnt = 0;
  unsigned long long   counter;
  boost::fibers::fiber io_fiber;
  io_context_t         io_ctx;
  volatile size_t      io_reqnum = 0;
  struct io_event      io_events[reap_batch];
  struct iocb*         io_reqvec[io_batch];

  struct io_return {
    intptr_t len;
    int err;
    bool done;
  };

  void fiber_proc() {
    m_state = state::running;
    if (1 == g_aio_method)
      fiber_proc_immediate_mode();
    else if (2 == g_aio_method)
      fiber_proc_batch_mode();
    else {
      fprintf(stderr, "ERROR: bad aio_method = %d\n", g_aio_method);
      exit(1);
    }
    assert(state::stopping == m_state);
    m_state = state::stopped;
  }
  void fiber_proc_immediate_mode() {
    while (state::running == m_state) {
      io_reap();
      boost::this_fiber::yield();
      counter++;
    }
  }
  void fiber_proc_batch_mode() {
    for (; state::running == m_state; counter++) {
      if (counter % 2 == 0) {
        batch_submit();
      } else {
        io_reap();
      }
      boost::this_fiber::yield();
    }
  }
  void batch_submit() {
    if (io_reqnum) {
      // should io_reqvec keep valid before reaped?
      int ret = io_submit(io_ctx, io_reqnum, io_reqvec);
      if (ret < 0) {
        int err = -ret;
        fprintf(stderr, "ERROR: ft_num = %zd, io_submit(nr=%zd) = %s\n", ft_num, io_reqnum, strerror(err));
      }
      else if (size_t(ret) == io_reqnum) {
        //fprintf(stderr, "INFO: ft_num = %zd, io_submit(nr=%zd) = %d, graceful\n", ft_num, io_reqnum, ret);
        io_reqnum = 0; // reset
      }
      else {
        fprintf(stderr, "WARN: ft_num = %zd, io_submit(nr=%zd) = %d\n", ft_num, io_reqnum, ret);
        assert(size_t(ret) < io_reqnum);
        memmove(io_reqvec, io_reqvec + ret, io_reqnum - ret);
        io_reqnum -= ret;
      }
      idle_cnt = 0;
    }
    else {
      idle_cnt++;
      if (idle_cnt > 128) {
        //fprintf(stderr, "INFO: fiber_proc: ft_num = %zd, idle_cnt = %zd, counter = %llu\n", ft_num, idle_cnt, counter);
        //std::this_thread::yield();
        usleep(100); // 100 us
        idle_cnt = 0;
      }
    }
  }

  void io_reap() {
    for (;;) {
      int ret = io_getevents(io_ctx, 0, reap_batch, io_events, NULL);
      if (ret < 0) {
        int err = -ret;
        fprintf(stderr, "ERROR: ft_num = %zd, io_getevents(nr=%d) = %s\n", ft_num, io_batch, strerror(err));
      }
      else {
        for (int i = 0; i < ret; i++) {
          io_return* ior = (io_return*)(io_events[i].data);
          ior->len = io_events[i].res;
          ior->err = io_events[i].res2;
          ior->done = true;
        }
        if (ret < reap_batch)
          break;
      }
    }
  }

public:
  intptr_t io_pread(int fd, void* buf, size_t len, off_t offset) {
    io_return io_ret = {0, -1, false};
    struct iocb io = {0};
    io.data = &io_ret;
    io.aio_lio_opcode = IO_CMD_PREAD;
    io.aio_fildes = fd;
    io.u.c.buf = buf;
    io.u.c.nbytes = len;
    io.u.c.offset = offset;
    if (1 == g_aio_method) {
      struct iocb* iop = &io;
      int ret = io_submit(io_ctx, 1, &iop);
      if (ret < 0) {
        int err = -ret;
        fprintf(stderr, "ERROR: ft_num = %zd, io_submit(nr=1) = %s\n", ft_num, strerror(err));
        errno = err;
        return -1;
      }
    }
    else {
        while (terark_unlikely(io_reqnum >= io_batch)) {
          fprintf(stderr, "WARN: ft_num = %zd, io_reqnum = %zd >= io_batch = %d, yield fiber\n", ft_num, io_reqnum, io_batch);
          boost::this_fiber::yield();
        }
        io_reqvec[io_reqnum++] = &io; // submit to user space queue
        //fprintf(stderr, "INFO: ft_num = %zd, io_reqnum = %zd, yield for io fiber submit\n", ft_num, io_reqnum);
    }
    do {
      boost::this_fiber::yield();
    } while (!io_ret.done);

    if (io_ret.err) {
      errno = io_ret.err;
    }
    return io_ret.len;
  }

  io_fiber_context()
    : io_fiber(std::bind(&io_fiber_context::fiber_proc, this))
  {
    ft_num = g_ft_num++;
    //fprintf(stderr, "INFO: io_fiber_context::io_fiber_context(): ft_num = %zd\n", ft_num);
    int maxevents = io_batch*2 - 1;
    int err = io_setup(maxevents, &io_ctx);
    if (err) {
      perror("io_setup");
      exit(3);
    }
    m_state = state::ready;
    counter = 0;
  }

  ~io_fiber_context() {
#if 0
    fprintf(stderr,
            "INFO: io_fiber_context::~io_fiber_context(): ft_num = %zd, counter = %llu ...\n",
            ft_num, counter);
#endif
    m_state = state::stopping;
    while (state::stopping == m_state) {
#if 0
      fprintf(stderr,
            "INFO: io_fiber_context::~io_fiber_context(): ft_num = %zd, counter = %llu, yield ...\n",
            ft_num, counter);
#endif
      boost::this_fiber::yield();
    }
    assert(state::stopped == m_state);
    io_fiber.join();

#if 0
    fprintf(stderr,
            "INFO: io_fiber_context::~io_fiber_context(): ft_num = %zd, counter = %llu, done\n",
            ft_num, counter);
#endif
    int err = io_destroy(io_ctx);
    if (err) {
      perror("io_destroy");
    }
  }
};
#endif

TERARK_DLL_EXPORT
ssize_t fiber_aio_read(int fd, void* buf, size_t len, off_t offset) {
#if BOOST_OS_LINUX
  if (1 == g_aio_method || 2 == g_aio_method) {
    static thread_local io_fiber_context io_fiber;
    return io_fiber.io_pread(fd, buf, len, offset);
  }
#endif
#if BOOST_OS_WINDOWS
  THROW_STD(invalid_argument, "Not Supported for Windows");
#else
  struct aiocb acb = {0};
  acb.aio_fildes = fd;
  acb.aio_offset = offset;
  acb.aio_buf = buf;
  acb.aio_nbytes = len;
  int err = aio_read(&acb);
  if (err) {
    return -1;
  }
  do {
    boost::this_fiber::yield();
    err = aio_error(&acb);
  } while (EINPROGRESS == err);

  if (err) {
    return -1;
  }
  return aio_return(&acb);
#endif
}

static const size_t PAGE_SIZE = 4096;

TERARK_DLL_EXPORT
void fiber_aio_need(const void* buf, size_t len) {
#if BOOST_OS_WINDOWS
#else
    len += size_t(buf) & (PAGE_SIZE-1);
    buf  = (const void*)(size_t(buf) & ~(PAGE_SIZE-1));
    size_t len2 = std::min<size_t>(len, 8*PAGE_SIZE);
    union {
        uint64_t val;
    #if BOOST_OS_LINUX
        unsigned char  vec[8];
    #else
        char  vec[8];
    #endif
    } uv;  uv.val = 0x0101010101010101ULL;
    int err = mincore((void*)buf, len2, uv.vec);
    if (0 == err) {
        if (0x0101010101010101ULL != uv.val) {
            posix_madvise((void*)buf, len, POSIX_MADV_WILLNEED);
        }
        if (0 == uv.vec[0]) {
            boost::this_fiber::yield(); // just yield once
        }
    }
#endif
}

} // namespace terark
