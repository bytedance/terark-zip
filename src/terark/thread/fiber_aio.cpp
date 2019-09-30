//
// Created by leipeng on 2019-08-22.
//

#include "fiber_aio.hpp"
#include "fiber_yield.hpp"
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
//  1: linux aio(default)
//  2: io uring (not supported now)
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

#define aio_debug(...)
//#define aio_debug(fmt, ...) fprintf(stderr, "DEBUG: %s:%d:%s: " fmt "\n", __FILE__, __LINE__, BOOST_CURRENT_FUNCTION, ##__VA_ARGS__)

class io_fiber_context {
  static const int reap_batch = 32;
  enum class state {
    ready,
    running,
    stopping,
    stopped,
  };
  FiberYield           m_fy;
  volatile state       m_state;
  size_t               ft_num;
  unsigned long long   counter;
  boost::fibers::fiber io_fiber;
  io_context_t         io_ctx;
  volatile size_t      io_reqnum = 0;
  struct io_event      io_events[reap_batch];

  struct io_return {
    boost::fibers::context* fctx;
    intptr_t len;
    int err;
    bool done;
  };

  void fiber_proc() {
    m_state = state::running;
    while (state::running == m_state) {
      io_reap();
      yield();
      counter++;
    }
    assert(state::stopping == m_state);
    m_state = state::stopped;
  }

  void io_reap() {
  /*
    if (io_reqnum < 1) {
        return;
    }
    if (io_reqnum == 1) {
      // block/wait if there is just one io request
      int ret = io_getevents(io_ctx, 1, 1, io_events, NULL);
      if (ret < 0) {
        int err = -ret;
        fprintf(stderr, "ERROR: ft_num = %zd, io_getevents(one) = %s\n", ft_num, strerror(err));
      }
      else {
        io_return* ior = (io_return*)(io_events->data);
        ior->len = io_events->res;
        ior->err = io_events->res2;
        ior->done = true;
        m_fy.unchecked_notify(&ior->fctx);
        io_reqnum = 0;
      }
      return;
    }
  */
    for (;;) {
      int ret = io_getevents(io_ctx, 0, reap_batch, io_events, NULL);
      if (ret < 0) {
        int err = -ret;
        fprintf(stderr, "ERROR: ft_num = %zd, io_getevents(nr=%d) = %s\n", ft_num, reap_batch, strerror(err));
      }
      else {
        for (int i = 0; i < ret; i++) {
          io_return* ior = (io_return*)(io_events[i].data);
          ior->len = io_events[i].res;
          ior->err = io_events[i].res2;
          ior->done = true;
          m_fy.unchecked_notify(&ior->fctx);
        }
        io_reqnum -= ret;
        if (ret < reap_batch)
          break;
      }
    }
  }

public:
  void yield() { m_fy.unchecked_yield(); }

  intptr_t io_pread(int fd, void* buf, size_t len, off_t offset) {
    io_return io_ret = {nullptr, 0, -1, false};
    struct iocb io = {0};
    io.data = &io_ret;
    io.aio_lio_opcode = IO_CMD_PREAD;
    io.aio_fildes = fd;
    io.u.c.buf = buf;
    io.u.c.nbytes = len;
    io.u.c.offset = offset;
    struct iocb* iop = &io;
    while (true) {
      int ret = io_submit(io_ctx, 1, &iop);
      if (ret < 0) {
        int err = -ret;
        if (EAGAIN == err) {
          yield();
          continue;
        }
        fprintf(stderr, "ERROR: ft_num = %zd, io_submit(nr=1) = %s\n", ft_num, strerror(err));
        errno = err;
        return -1;
      }
      break;
    }
    io_reqnum++;
    m_fy.unchecked_wait(&io_ret.fctx);
    assert(io_ret.done);
    if (io_ret.err) {
      errno = io_ret.err;
    }
    return io_ret.len;
  }

  io_fiber_context(boost::fibers::context** pp)
    : m_fy(pp)
    , io_fiber(std::bind(&io_fiber_context::fiber_proc, this))
  {
    ft_num = g_ft_num++;
    aio_debug("ft_num = %zd", ft_num);
    int maxevents = reap_batch*4 - 1;
    int err = io_setup(maxevents, &io_ctx);
    if (err) {
      perror("io_setup");
      exit(3);
    }
    m_state = state::ready;
    counter = 0;
  }

  ~io_fiber_context() {
    aio_debug("ft_num = %zd, counter = %llu ...", ft_num, counter);

    m_state = state::stopping;
    while (state::stopping == m_state) {
      aio_debug("ft_num = %zd, counter = %llu, yield ...", ft_num, counter);
      yield();
    }
    assert(state::stopped == m_state);
    io_fiber.join();

    aio_debug("ft_num = %zd, counter = %llu, done", ft_num, counter);

    assert(0 == io_reqnum);

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
  if (1 == g_aio_method) {
    using boost::fibers::context;
    static thread_local io_fiber_context io_fiber(context::active_pp());
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
