//
// Created by leipeng on 2019-08-22.
//

#include "fiber_aio.hpp"
#include <boost/predef.h>

#if BOOST_OS_LINUX
  #include <libaio.h> // linux native aio
#endif

#if BOOST_OS_WINDOWS
	#define NOMINMAX
	#define WIN32_LEAN_AND_MEAN
	#include <io.h>
	#include <Windows.h>
#else
  #include <aio.h> // posix aio
  #include <sys/types.h>
  #include <sys/mman.h>
#endif

#include "fiber_yield.hpp"
#include <terark/stdtypes.hpp>
#include <terark/fstring.hpp>
#include <terark/util/atomic.hpp>
#include <terark/util/throw.hpp>
#include <boost/fiber/all.hpp>
#include <boost/lockfree/queue.hpp>

namespace terark {

#if defined(__GNUC__)
    #pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#endif

#if BOOST_OS_LINUX

///@returns
//  0: posix aio
//  1: linux aio(default)
//  2: io uring (not supported now)
static int g_aio_method = (int)getEnvLong("aio_method", 1);

static std::atomic<size_t> g_ft_num;

#define aio_debug(...)
//#define aio_debug(fmt, ...) fprintf(stderr, "DEBUG: %s:%d:%s: " fmt "\n", __FILE__, __LINE__, BOOST_CURRENT_FUNCTION, ##__VA_ARGS__)

#define FIBER_AIO_VERIFY(expr) \
  do { \
    int ret = expr; \
    if (ret) TERARK_DIE("%s = %s", #expr, strerror(-ret)); \
  } while (0)

struct io_return {
  boost::fibers::context* fctx;
  intptr_t len;
  int err;
  bool done;
};

typedef boost::lockfree::queue<struct iocb*, boost::lockfree::fixed_sized<true>>
        io_queue_t;
io_queue_t* dt_io_queue();

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
  io_context_t         io_ctx = 0;
  volatile size_t      io_reqnum = 0;
  struct io_event      io_events[reap_batch];

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
    for (;;) {
      int ret = io_getevents(io_ctx, 0, reap_batch, io_events, NULL);
      if (ret < 0) {
        int err = -ret;
        if (EAGAIN == err)
          yield();
        else
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

  intptr_t exec_io(int fd, void* buf, size_t len, off_t offset, int cmd) {
    io_return io_ret = {nullptr, 0, -1, false};
    struct iocb io = {0};
    io.data = &io_ret;
    io.aio_lio_opcode = cmd;
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

  intptr_t dt_exec_io(int fd, void* buf, size_t len, off_t offset, int cmd) {
    io_return io_ret = {nullptr, 0, -1, false};
    struct iocb io = {0};
    io.data = &io_ret;
    io.aio_lio_opcode = cmd;
    io.aio_fildes = fd;
    io.u.c.buf = buf;
    io.u.c.nbytes = len;
    io.u.c.offset = offset;
    auto queue = dt_io_queue();
    while (!queue->bounded_push(&io)) yield();
    size_t loop = 0;
    do {
      // io is performed in another thread, we don't know when it's finished,
      // so we poll the flag by yield fiber or yield thread
      if (++loop % 256)
        yield();
      else
        std::this_thread::yield();
    } while (!as_atomic(io_ret.done).load(std::memory_order_acquire));
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
    FIBER_AIO_VERIFY(io_setup(maxevents, &io_ctx));
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
    TERARK_VERIFY(state::stopped == m_state);
    io_fiber.join();
    aio_debug("ft_num = %zd, counter = %llu, done", ft_num, counter);
    TERARK_VERIFY(0 == io_reqnum);
    FIBER_AIO_VERIFY(io_destroy(io_ctx));
  }
};

static io_fiber_context& tls_io_fiber() {
  using boost::fibers::context;
  static thread_local io_fiber_context io_fiber(context::active_pp());
  return io_fiber;
}

// dt_ means 'dedicated thread'
struct DT_ResetOnExitPtr {
  std::atomic<io_queue_t*> ptr;
  DT_ResetOnExitPtr();
  ~DT_ResetOnExitPtr() { ptr = nullptr; }
};
static void dt_func(DT_ResetOnExitPtr* p_tls) {
  io_queue_t queue(1023);
  p_tls->ptr = &queue;
  io_context_t io_ctx = 0;
  constexpr int batch = 64;
  FIBER_AIO_VERIFY(io_setup(batch*4 - 1, &io_ctx));
  struct iocb*    io_batch[batch];
  struct io_event io_events[batch];
  intptr_t req = 0, submits = 0, reaps = 0;
  while (p_tls->ptr.load(std::memory_order_relaxed)) {
    while (req < batch && queue.pop(io_batch[req])) req++;
    int works = 0;
    if (req) {
      int ret = io_submit(io_ctx, req, io_batch);
      if (ret < 0) {
        int err = -ret;
        if (EAGAIN != err)
          TERARK_DIE("io_submit(nr=%zd) = %s\n", req, strerror(err));
      }
      else if (ret > 0) {
        submits += ret;
        works += ret;
        req -= ret;
        if (req)
          std::copy_n(io_batch + ret, req, io_batch);
      }
    }
    if (reaps < submits) {
      int ret = io_getevents(io_ctx, 1, batch, io_events, NULL);
      if (ret < 0) {
        int err = -ret;
        if (EAGAIN != err)
          fprintf(stderr, "ERROR: %s:%d: io_getevents(nr=%d) = %s\n", __FILE__, __LINE__, batch, strerror(err));
      }
      else {
        for (int i = 0; i < ret; i++) {
          io_return* ior = (io_return*)(io_events[i].data);
          ior->len = io_events[i].res;
          ior->err = io_events[i].res2;
          as_atomic(ior->done).store(true, std::memory_order_release);
        }
        reaps += ret;
        works += ret;
      }
    }
    if (0 == works)
      std::this_thread::yield();
  }
  FIBER_AIO_VERIFY(io_destroy(io_ctx));
}
DT_ResetOnExitPtr::DT_ResetOnExitPtr() {
  ptr = nullptr;
  std::thread(std::bind(&dt_func, this)).detach();
}
io_queue_t* dt_io_queue() {
  static DT_ResetOnExitPtr p_tls;
  io_queue_t* q;
  while (nullptr == (q = p_tls.ptr.load())) {
    std::this_thread::yield();
  }
  return q;
}

#endif

TERARK_DLL_EXPORT
intptr_t fiber_aio_read(int fd, void* buf, size_t len, off_t offset) {
#if BOOST_OS_LINUX
  if (1 == g_aio_method) {
    return tls_io_fiber().exec_io(fd, buf, len, offset, IO_CMD_PREAD);
  }
#endif
#if BOOST_OS_WINDOWS
  TERARK_DIE("Not Supported for Windows");
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

static const size_t MY_AIO_PAGE_SIZE = 4096;

TERARK_DLL_EXPORT
void fiber_aio_need(const void* buf, size_t len) {
#if BOOST_OS_WINDOWS
		WIN32_MEMORY_RANGE_ENTRY vm;
		vm.VirtualAddress = (void*)buf;
		vm.NumberOfBytes  = len;
		PrefetchVirtualMemory(GetCurrentProcess(), 1, &vm, 0);
#elif !defined(__CYGWIN__)
    len += size_t(buf) & (MY_AIO_PAGE_SIZE-1);
    buf  = (const void*)(size_t(buf) & ~(MY_AIO_PAGE_SIZE-1));
    size_t len2 = std::min<size_t>(len, 8*MY_AIO_PAGE_SIZE);
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

TERARK_DLL_EXPORT
intptr_t fiber_aio_write(int fd, const void* buf, size_t len, off_t offset) {
#if BOOST_OS_LINUX
  if (1 == g_aio_method) {
    return tls_io_fiber().exec_io(fd, (void*)buf, len, offset, IO_CMD_PWRITE);
  }
#endif
#if BOOST_OS_WINDOWS
  TERARK_DIE("Not Supported for Windows");
#else
  struct aiocb acb = {0};
  acb.aio_fildes = fd;
  acb.aio_offset = offset;
  acb.aio_buf = (void*)buf;
  acb.aio_nbytes = len;
  int err = aio_write(&acb);
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

TERARK_DLL_EXPORT
intptr_t fiber_put_write(int fd, const void* buf, size_t len, off_t offset) {
#if BOOST_OS_LINUX
  if (1 == g_aio_method) {
    return tls_io_fiber().dt_exec_io(fd, (void*)buf, len, offset, IO_CMD_PWRITE);
  }
  TERARK_DIE("Not Supported aio_method = %d", g_aio_method);
#else
  TERARK_DIE("Not Supported platform");
#endif
}

} // namespace terark
