/* vim: set tabstop=4 : */

#include "pipeline.hpp"
#include <terark/circular_queue.hpp>
#include <terark/num_to_str.hpp>
//#include <terark/util/compare.hpp>
#include <terark/valvec.hpp>
//#include <deque>
//#include <boost/circular_buffer.hpp>
#include <terark/util/atomic.hpp>
#include <terark/util/concurrent_queue.hpp>
#include <stdio.h>
#include <iostream>
#include <boost/fiber/all.hpp>
#include "fiber_yield.hpp"

// http://predef.sourceforge.net/

#if defined(TERARK_CONCURRENT_QUEUE_USE_BOOST)
	#include <boost/thread/mutex.hpp>
	#include <boost/thread/lock_guard.hpp>
	#include <boost/thread.hpp>
	#include <boost/bind.hpp>
	#if defined(_WIN32) || defined(_WIN64) || defined(_MSC_VER)
		#define NOMINMAX
		#define WIN32_LEAN_AND_MEAN
		#include <Windows.h>
		#undef min
		#undef max
	#else
		#include <unistd.h>
	#endif
#else
#endif

namespace terark {

using namespace std;

PipelineTask::~PipelineTask()
{
	// do nothing...
}

//typedef concurrent_queue<std::deque<PipelineQueueItem> > base_queue;
typedef util::concurrent_queue<circular_queue<PipelineQueueItem> > base_queue;

class PipelineStage::queue_t {
public:
    virtual ~queue_t() {}
    virtual void push_back(const PipelineQueueItem& x, FiberYield*) = 0;
    virtual bool push_back(const PipelineQueueItem&, int timeout, FiberYield*) = 0;
    virtual bool pop_front(PipelineQueueItem& x, int timeout, FiberYield*) = 0;
    virtual bool empty() = 0;
    virtual size_t size() = 0;
    virtual size_t peekSize() const = 0;
};

class BlockQueue : public PipelineStage::queue_t {
    base_queue q;
public:
	BlockQueue(size_t size) : q(size) {
		q.queue().init(size + 1);
	}
	void push_back(const PipelineQueueItem& x, FiberYield*) final {
	    q.push_back(x);
	}
    bool push_back(const PipelineQueueItem& x, int timeout, FiberYield*) final {
	    return q.push_back(x, timeout);
	}
    bool pop_front(PipelineQueueItem& x, int timeout, FiberYield*) final {
        return q.pop_front(x, timeout);
    }
    bool empty() final {return q.empty(); }
    size_t size() final { return q.size(); }
    size_t peekSize() const final { return q.peekSize(); }
};

class FiberQueue : public PipelineStage::queue_t {
    circular_queue<PipelineQueueItem> q;
public:
    FiberQueue(size_t size) : q(size) {}
	void push_back(const PipelineQueueItem& x, FiberYield* fy) final {
        while (q.full()) {
            fy->yield();
        }
        q.push_back(x);
	}
    bool push_back(const PipelineQueueItem& x, int timeout, FiberYield* fy) final {
        int estimate_loops = timeout * 8; // assume 8 scheduals takes 1 ms
        for (int i = 0; i < estimate_loops; i++) {
            if (!q.full()) {
                q.push_back(x);
                return true;
            }
            fy->yield();
        }
        return false;
    }
    bool pop_front(PipelineQueueItem& x, int timeout, FiberYield* fy) final {
        int estimate_loops = timeout * 8; // assume 8 scheduals takes 1 ms
        for (int i = 0; i < estimate_loops; i++) {
            if (!q.empty()) {
                x = q.front();
                q.pop_front();
                return true;
            }
            fy->yield();
        }
        return false;
    }
    bool empty() final {return q.empty(); }
    size_t size() final { return q.size(); }
    size_t peekSize() const final { return q.size(); }
};

class MixedQueue : public PipelineStage::queue_t {
    base_queue q;
public:
	MixedQueue(size_t size) : q(size) {
		q.queue().init(size + 1);
	}
	void push_back(const PipelineQueueItem& x, FiberYield* fy) final {
		if (q.peekFull()) {
			fy->yield();
		}
	    q.push_back(x);
	}
    bool push_back(const PipelineQueueItem& x, int timeout, FiberYield* fy) final {
		if (q.peekFull()) {
			fy->yield();
		}
        return q.push_back(x, timeout);
	}
    bool pop_front(PipelineQueueItem& x, int timeout, FiberYield* fy) final {
		if (q.peekEmpty()) {
			fy->yield();
		}
        return q.pop_front(x, timeout);
    }
    bool empty() final { return q.peekEmpty(); }
    size_t size() final { return q.peekSize(); }
    size_t peekSize() const final { return q.peekSize(); }
};

static
PipelineStage::queue_t*
NewQueue(PipelineProcessor::EUType euType, size_t size) {
	switch (euType) {
	default: abort();
	case PipelineProcessor::EUType::fiber : return new FiberQueue(size);
	case PipelineProcessor::EUType::thread: return new BlockQueue(size);
	case PipelineProcessor::EUType::mixed : return new MixedQueue(size);
	}
}

class PipelineStage::ExecUnit {
public:
    virtual ~ExecUnit() {}
    virtual void join() = 0;
    virtual bool joinable() const = 0;
    virtual FiberYield* get_fiber_yield() = 0;
};
class ThreadExecUnit : public PipelineStage::ExecUnit {
    thread thr;
public:
    template<class Func>
    ThreadExecUnit(Func&& f) : thr(std::move(f)) {}

    void join() final { thr.join(); }
    bool joinable() const final { return thr.joinable(); }
    FiberYield* get_fiber_yield() override { return NULL; }
};
class FiberExecUnit : public PipelineStage::ExecUnit {
    boost::fibers::fiber fib;
    FiberYield fy;
public:
    template<class Func>
    FiberExecUnit(Func&& f) : fib(std::move(f)) {}

    void join() final { fib.join(); }
    bool joinable() const final { return fib.joinable(); }
    FiberYield* get_fiber_yield() final { return &fy; }
};
class MixedExecUnit : public ThreadExecUnit {
    FiberYield fy;
public:
    template<class MakeFunc>
    MixedExecUnit(int nfib, MakeFunc mkfn)
 : ThreadExecUnit([=]() {
        fy.init_in_fiber_thread();
        const int spawn_num = nfib-1;
        valvec<boost::fibers::fiber> fibs(spawn_num, valvec_reserve());
        for (int i = 0; i < spawn_num; ++i) {
            fibs.unchecked_emplace_back(mkfn());
        }
        mkfn()(); // use main fiber as last worker
        for (int i = 0; i < spawn_num; ++i) {
            fibs[i].join();
        }
    }){
        assert(nfib > 0);
    }
    FiberYield* get_fiber_yield() final { return &fy; }
};
template<class Function>
PipelineStage::ExecUnit* NewExecUnit(bool fiberMode, Function&& func) {
    if (fiberMode)
        return new FiberExecUnit(std::move(func));
    else
        return new ThreadExecUnit(std::move(func));
}

PipelineStage::ThreadData::ThreadData() {
	m_thread = NULL;
    m_live_fibers = 0;
}
PipelineStage::ThreadData::~ThreadData() {
	delete m_thread;
}

//////////////////////////////////////////////////////////////////////////

PipelineStage::PipelineStage(int thread_count)
: PipelineStage(thread_count, 1) {}

PipelineStage::PipelineStage(int thread_count, int fibers_per_thread)
: m_owner(NULL)
{
	if (0 == thread_count)
	{
		m_pl_enum = ple_keep;
		thread_count = 1;
	}
	else if (-1 == thread_count)
	{
		m_pl_enum = ple_generate;
		thread_count = 1;
	}
	else
	{
		m_pl_enum = ple_none;
	}
	assert(0 < thread_count);
	m_plserial = 0;
	m_prev = m_next = NULL;
	m_out_queue = NULL;
	m_threads.resize(thread_count);
	m_fibers_per_thread = std::max(fibers_per_thread, 1);
	m_running_exec_units = 0;
}

PipelineStage::~PipelineStage()
{
	delete m_out_queue;
	for (size_t threadno = 0; threadno != m_threads.size(); ++threadno)
	{
		assert(!m_threads[threadno].m_thread->joinable());
	}
}

size_t PipelineStage::getInputQueueSize() const {
	assert(m_prev->m_out_queue);
	return m_prev->m_out_queue->size();
}

size_t PipelineStage::getOutputQueueSize() const {
	assert(this->m_out_queue);
	return this->m_out_queue->size();
}

void PipelineStage::createOutputQueue(size_t size) {
	if (size > 0) {
		assert(NULL == this->m_out_queue);
		this->m_out_queue = NewQueue(m_owner->m_EUType, size);
	}
}

const std::string& PipelineStage::err(int threadno) const
{
	return m_threads[threadno].m_err_text;
}

int PipelineStage::step_ordinal() const
{
	return m_owner->step_ordinal(this);
}

std::string PipelineStage::msg_leading(int threadno) const
{
	char buf[256];
	int len = sprintf(buf, "step[%d], threadno[%d]", step_ordinal(), threadno);
	return std::string(buf, len);
}

mutex* PipelineStage::getMutex() const
{
	return m_owner->getMutex();
}

void PipelineStage::stop()
{
	m_owner->stop();
}

inline bool PipelineStage::isPrevRunning()
{
	return m_owner->isRunning() || m_prev->isRunning() || !m_prev->m_out_queue->empty();
}

void PipelineStage::wait()
{
	for (size_t t = 0; t != m_threads.size(); ++t)
		m_threads[t].m_thread->join();
}

void PipelineStage::start(int queue_size)
{
	assert(NULL != m_owner);
	const auto euType = m_owner->m_EUType;
	const bool fiberMode = euType == PipelineProcessor::EUType::fiber;

	if (this != m_owner->m_head->m_prev) { // is not last step
		if (NULL == m_out_queue)
			m_out_queue = NewQueue(euType, queue_size);
	}
	if (m_threads.size() == 0) {
		throw std::runtime_error("thread count = 0");
	}
	m_running_exec_units = 0;

	for (size_t threadno = 0; threadno != m_threads.size(); ++threadno)
	{
		// 在 PipelineThread 保存 auto_ptr<thread> 指针
		// 如果直接内嵌一个 thread 实例，那么在 new PipelineThread 时，该线程就开始运行
		// 有可能在 PipelineThread 运行结束时，m_threads[threadno] 还未被赋值，导致程序崩溃
		// 所以分两步，先构造对象，并且对 m_threads[threadno] 赋值，然后再运行线程
		//
		//
		// hold auto_ptr<thread> in PipelineThread
		//
		// if direct embed a thread in PipelineThread, when new PipelineThread,
		// the thread will running, then, maybe after PipelineThread was completed,
		// m_thread[threadno] was not be assigned the PipelineThread* pointer,
		// the app maybe crash in this case.
		//
		// so make 2 stage:
		// first:  construct the PipelineThread and assign it to m_threads[threadno]
		// second: start the thread
		//
		assert(m_threads[threadno].m_thread == nullptr);
		if (euType == PipelineProcessor::EUType::mixed) {
			auto mkfn = [this,threadno]() {
				return bind(&PipelineStage::run_wrapper, this, threadno);
			};
			m_threads[threadno].m_thread =
				new MixedExecUnit(m_fibers_per_thread, mkfn);
		}
		else {
			m_threads[threadno].m_thread = NewExecUnit(fiberMode,
				bind(&PipelineStage::run_wrapper, this, threadno));
		}
	}
}

void PipelineStage::onException(int threadno, const std::exception& exp)
{
	static_cast<string_appender<>&>(m_threads[threadno].m_err_text = "")
		<< "exception class=" << typeid(exp).name() << ", what=" << exp.what();

//	error message will be printed in this->clean()
//
// 	PipelineLockGuard lock(*m_owner->m_mutex);
// 	std::cerr << "step[" << step_ordinal() << "]: " << m_step_name
// 			  << ", threadno[" << threadno
// 			  << "], caught: " << m_threads[threadno].m_err_text
// 			  << std::endl;
}

void PipelineStage::setup(int threadno)
{
	if (m_owner->m_logLevel >= 1) {
        size_t live_fibers = m_threads[threadno].m_live_fibers;
		printf("start step[ordinal=%d, threadno=%d, live_fibers=%zd]: %s\n",
		        step_ordinal(), threadno, live_fibers, m_step_name.c_str());
	}
}

void PipelineStage::clean(int threadno)
{
    size_t live_fibers = m_threads[threadno].m_live_fibers;
	if (err(threadno).empty()) {
		if (m_owner->m_logLevel >= 1)
			printf("ended step[ordinal=%d, threadno=%d, live_fibers=%zd]: %s\n"
				, step_ordinal(), threadno, live_fibers, m_step_name.c_str());
	} else {
		fprintf(stderr, "ended step[ordinal=%d, threadno=%d, live_fibers=%zd]: %s; error: %s\n"
				, step_ordinal(), threadno, live_fibers, m_step_name.c_str()
				, err(threadno).c_str());
	}
}

void PipelineStage::run_wrapper(int threadno)
{
	as_atomic(m_running_exec_units)++;
	m_threads[threadno].m_live_fibers++;
	bool setup_successed = false;
	try {
		setup(threadno);
		setup_successed = true;
		run(threadno);
		clean(threadno);
		assert(m_prev == m_owner->m_head || m_prev->m_out_queue->empty());
	}
	catch (const std::exception& exp)
	{
		onException(threadno, exp);
		if (setup_successed)
			clean(threadno);

		m_owner->stop();

		if (m_prev != m_owner->m_head)
		{ // 不是第一个 step, 清空前一个 step 的 out_queue
		    FiberYield* fy = m_threads[threadno].m_thread->get_fiber_yield();
			while (!m_prev->m_out_queue->empty() || m_prev->isRunning())
			{
				PipelineQueueItem item;
				if (m_prev->m_out_queue->pop_front(item, m_owner->m_queue_timeout, fy))
				{
					if (item.task)
						m_owner->destroyTask(item.task);
				}
			}
		}
	}
	m_owner->stop();
	m_threads[threadno].m_live_fibers--;
	as_atomic(m_running_exec_units)--;
}

void PipelineStage::run(int threadno)
{
	assert(m_owner->total_steps() >= 1);
	if (this == m_owner->m_head->m_next
		&&
		NULL == m_owner->m_head->m_out_queue // input feed is from 'run_step_first'
	) {
		run_step_first(threadno);
	} else {
		// when NULL != m_owner->m_head->m_out_queue, input feed is from external
		switch (m_pl_enum) {
		default:
			assert(0);
			break;
		case ple_none:
		case ple_generate:
			if (this == m_owner->m_head->m_prev)
				run_step_last(threadno);
			else
				run_step_mid(threadno);
			break;
		case ple_keep:
			assert(m_threads.size() == 1);
			run_serial_step_fast(threadno);
			break;
		}
	}
}

void PipelineStage::run_step_first(int threadno)
{
	assert(ple_none == m_pl_enum || ple_generate == m_pl_enum);
	assert(this->m_threads.size() == 1);
	FiberYield* fy = m_threads[threadno].m_thread->get_fiber_yield();
	while (m_owner->isRunning())
	{
		PipelineQueueItem item;
		process(threadno, &item);
		if (item.task)
		{
			if (ple_generate == m_pl_enum)
			{
 			//	queue_t::MutexLockSentry lock(*m_out_queue); // not need lock
				item.plserial = ++m_plserial;
			}
			if (m_owner->m_logLevel >= 3) {
                while (!m_out_queue->push_back(item, m_owner->m_queue_timeout, fy)) {
                    fprintf(stderr, "Pipeline: first_step(%s): tno=%d, wait push timeout, retry ...\n", m_step_name.c_str(), threadno);
                }
			} else {
			    m_out_queue->push_back(item, fy);
			}
		}
	}
}

void PipelineStage::run_step_last(int threadno)
{
	assert(ple_none == m_pl_enum);

	FiberYield* fy = m_threads[threadno].m_thread->get_fiber_yield();
	while (isPrevRunning())
	{
		PipelineQueueItem item;
		if (m_prev->m_out_queue->pop_front(item, m_owner->m_queue_timeout, fy))
		{
			if (item.task)
				process(threadno, &item);
			if (item.task)
				m_owner->destroyTask(item.task);
		}
		else {
		    if (m_owner->m_logLevel >= 3) {
		        fprintf(stderr, "Pipeline: last_step(%s): tno=%d, wait pop timeout, retry ...\n", m_step_name.c_str(), threadno);
		    }
		}
	}
}

void PipelineStage::run_step_mid(int threadno)
{
	assert(ple_none == m_pl_enum || (ple_generate == m_pl_enum && m_threads.size() == 1));

	if (m_owner->m_logLevel >= 4) {
			fprintf(stderr, "Pipeline: run_step_mid(tno=%d), fiber_no = %zd, enter\n", threadno, m_threads[threadno].m_live_fibers);
	}

	FiberYield* fy = m_threads[threadno].m_thread->get_fiber_yield();
	while (isPrevRunning())
	{
		PipelineQueueItem item;
		if (m_prev->m_out_queue->pop_front(item, m_owner->m_queue_timeout, fy))
		{
			if (ple_generate == m_pl_enum) {
			// only 1 thread, do not mutex lock
			// use m_out_queue mutex
 			//	queue_t::MutexLockSentry lock(*m_out_queue);
				item.plserial = ++m_plserial;
 			}
			if (item.task)
				process(threadno, &item);
			if (item.task || m_owner->m_keepSerial)
				m_out_queue->push_back(item, fy);
		}
		else {
		    if (m_owner->m_logLevel >= 3) {
		        fprintf(stderr, "Pipeline: mid_step(%s): tno=%d, wait push timeout, retry ...\n", m_step_name.c_str(), threadno);
		    }
		}
	}

	if (m_owner->m_logLevel >= 4) {
			fprintf(stderr, "Pipeline: run_step_mid(tno=%d), fiber_no = %zd, leave\n", threadno, m_threads[threadno].m_live_fibers);
	}
}

namespace pipeline_detail {
//SAME_NAME_MEMBER_COMPARATOR_EX(plserial_greater, uintptr_t, uintptr_t, .plserial, std::greater<uintptr_t>)
//SAME_NAME_MEMBER_COMPARATOR_EX(plserial_less   , uintptr_t, uintptr_t, .plserial, std::less   <uintptr_t>)

struct plserial_greater {
	template<class T>
	bool operator()(const T& x, const T& y) const {
		return x.plserial > y.plserial;
	}
};
struct plserial_less {
	template<class T>
	bool operator()(const T& x, const T& y) const {
		return x.plserial < y.plserial;
	}
};
}
using namespace pipeline_detail;

#if defined(_DEBUG) || !defined(NDEBUG)
# define CHECK_SERIAL() assert(item.plserial >= m_plserial);
#else
# define CHECK_SERIAL() \
	if (item.plserial < m_plserial) { \
		string_appender<> oss; \
        oss << "fatal at: " << __FILE__ << ":" << __LINE__ \
            << ", function=" << BOOST_CURRENT_FUNCTION \
            << ", item.plserial=" << item.plserial \
            << ", m_plserial=" << m_plserial; \
        throw std::runtime_error(oss);  \
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#endif

void PipelineStage::run_serial_step_slow(int threadno,
								   void (PipelineStage::*fdo)(PipelineQueueItem&)
								  )
{
	assert(ple_keep == m_pl_enum);
	assert(m_threads.size() == 1);
	assert(0 == threadno);
	using namespace std;
	FiberYield* fy = m_threads[threadno].m_thread->get_fiber_yield();
	valvec<PipelineQueueItem> cache;
	m_plserial = 1;
	while (isPrevRunning()) {
		PipelineQueueItem item;
		if (!m_prev->m_out_queue->pop_front(item, m_owner->m_queue_timeout, fy))
        {
		    if (m_owner->m_logLevel >= 3) {
		        fprintf(stderr, "Pipeline: serial_step_slow(%s): tno=%d, wait pop timeout, retry ...\n", m_step_name.c_str(), threadno);
		    }
			continue;
		}
		CHECK_SERIAL()
		if (item.plserial == m_plserial)
		{Loop:
			if (item.task)
				process(threadno, &item);
			(this->*fdo)(item);
			++m_plserial;
			if (!cache.empty() && (item = cache[0]).plserial == m_plserial) {
				pop_heap(cache.begin(), cache.end(), plserial_greater());
				cache.pop_back();
				goto Loop;
			}
		}
		else { // plserial out of order
			cache.push_back(item);
			push_heap(cache.begin(), cache.end(), plserial_greater());
		}
	}
	std::sort(cache.begin(), cache.end(), plserial_less());
	for (valvec<PipelineQueueItem>::iterator i = cache.begin(); i != cache.end(); ++i) {
		if (i->task)
			process(threadno, &*i);
		(this->*fdo)(*i);
	}
}

// use a local 'cache' to cache the received tasks, if tasks are out of order,
// hold them in the cache, until next received task is just the expected task.
// if the next received task's serial number is out of the cache's range,
// put it to the overflow vector...
void PipelineStage::run_serial_step_fast(int threadno)
{
	assert(ple_keep == m_pl_enum);
	assert(m_threads.size() == 1);
	assert(0 == threadno);
	using namespace std;
	const bool is_last = this == m_owner->m_head->m_prev;
	FiberYield* fy = m_threads[threadno].m_thread->get_fiber_yield();
	const ptrdiff_t nlen = TERARK_IF_DEBUG(4, 64); // should power of 2
	ptrdiff_t head = 0;
	valvec<PipelineQueueItem> cache(nlen), overflow;
	m_plserial = 1; // this is expected_serial
	while (isPrevRunning()) {
		PipelineQueueItem item;
		if (!m_prev->m_out_queue->pop_front(item, m_owner->m_queue_timeout, fy))
        {
		    if (m_owner->m_logLevel >= 3) {
		        fprintf(stderr, "Pipeline: serial_step_fast(%s): tno=%d, prev.live_exec = %d, wait pop timeout, retry ...\n", m_step_name.c_str(), threadno, m_prev->m_running_exec_units);
		    }
			continue;
		}
		CHECK_SERIAL()
		ptrdiff_t diff = item.plserial - m_plserial; // diff is in [0, nlen)
		//  not all equivalent to cycle queue, so it is not 'diff < nlen-1'
		// if plserial is in [1, nlen) as TCP, it should be 'diff < nlen-1'
		if (terark_likely(diff < nlen)) {
			int index = (head + diff) % nlen;
			cache[index] = item;
		}
		else { // very rare case
			overflow.push_back(item);
			if (overflow.size() >= 2) {
				push_heap(overflow.begin(), overflow.end(), plserial_greater());
				do {
					diff = overflow[0].plserial - m_plserial;
					if (diff < nlen) {
						int index = (head + diff) % nlen;
						cache[index] = overflow[0];
						pop_heap(overflow.begin(), overflow.end(), plserial_greater());
						overflow.pop_back();
					} else
						break;
				} while (!overflow.empty());
			}
		}
		while (cache[head].plserial == m_plserial) {
			if (cache[head].task)
				process(threadno, &cache[head]);

			if (terark_likely(is_last)) {
                if (terark_likely(NULL != item.task))
                    m_owner->destroyTask(item.task);
			}
			else {
            	m_out_queue->push_back(item, fy);
			}

			cache[head] = PipelineQueueItem(); // clear
			++m_plserial;
			head = (head + 1) % nlen;
			if (terark_unlikely(!overflow.empty() && overflow[0].plserial == m_plserial)) {
				// very rare case
				cache[head] = overflow[0];
				pop_heap(overflow.begin(), overflow.end(), plserial_greater());
				overflow.pop_back();
			}
		}
	}
	for (ptrdiff_t i = 0; i < nlen; ++i) {
		if (cache[i].plserial)
			overflow.push_back(cache[i]);
	}
	std::sort(overflow.begin(), overflow.end(), plserial_less());
	for (size_t j = 0; j != overflow.size(); ++j) {
		PipelineQueueItem* i = &overflow[j];
		if (i->task)
			process(threadno, i);

        if (terark_likely(is_last)) {
            if (terark_likely(NULL != i->task))
                m_owner->destroyTask(i->task);
        }
        else {
            m_out_queue->push_back(*i, fy);
        }
	}
}

//////////////////////////////////////////////////////////////////////////

FunPipelineStage::FunPipelineStage(int thread_count,
				const function<void(PipelineStage*, int, PipelineQueueItem*)>& fprocess,
				const std::string& step_name)
	: PipelineStage(thread_count)
	, m_process(fprocess)
{
	m_step_name = (step_name);
}

FunPipelineStage::FunPipelineStage(int thread_count, int fibers_per_thread,
				const function<void(PipelineStage*, int, PipelineQueueItem*)>& fprocess,
				const std::string& step_name)
	: PipelineStage(thread_count, fibers_per_thread)
	, m_process(fprocess)
{
	m_step_name = (step_name);
}

FunPipelineStage::~FunPipelineStage() {}

void FunPipelineStage::process(int threadno, PipelineQueueItem* task)
{
	m_process(this, threadno, task);
}

//////////////////////////////////////////////////////////////////////////

class Null_PipelineStage : public PipelineStage
{
public:
	Null_PipelineStage() : PipelineStage(0) { m_threads.clear(); }
protected:
	virtual void process(int /*threadno*/, PipelineQueueItem* /*task*/)
	{
		assert(0);
	}
};

int PipelineProcessor::sysCpuCount() {
#if defined(TERARK_CONCURRENT_QUEUE_USE_BOOST)
  #if defined(_WIN32) || defined(_WIN64) || defined(_MSC_VER)
	SYSTEM_INFO sysInfo;
	GetSystemInfo(&sysInfo);
	return sysInfo.dwNumberOfProcessors;
  #else
	return (int)sysconf(_SC_NPROCESSORS_ONLN);
  #endif
#else
	return (int)std::thread::hardware_concurrency();
#endif
}

PipelineProcessor::PipelineProcessor()
  : m_destroyTask(&PipelineProcessor::defaultDestroyTask)
{
	m_queue_size = 10;
	m_queue_timeout = 20;
	m_head = new Null_PipelineStage;
	m_head->m_prev = m_head->m_next = m_head;
	m_mutex = NULL;
	m_is_mutex_owner = false;
	m_keepSerial = false;
	m_run = false;
	m_logLevel = 1;
	m_EUType = EUType::thread;
}

PipelineProcessor::~PipelineProcessor()
{
	clear();

	delete m_head;
	if (m_is_mutex_owner)
		delete m_mutex;
}

void PipelineProcessor::defaultDestroyTask(PipelineTask* task)
{
	delete task;
}
void PipelineProcessor::destroyTask(PipelineTask* task)
{
	m_destroyTask(task);
}

void PipelineProcessor::setMutex(mutex* pmutex)
{
	if (m_run) {
		throw std::logic_error("can not setMutex after PipelineProcessor::start()");
	}
	m_mutex = pmutex;
	m_is_mutex_owner = false;
}

const char* PipelineProcessor::euTypeName() const {
	if (m_EUType > EUType::mixed) {
		return "invalid";
	}
	const char* names[] = {
			"thread",
			"fiber",
			"mixed",
	};
	return names[int(m_EUType)];
}

std::string PipelineProcessor::queueInfo()
{
	string_appender<> oss;
	const PipelineStage* p = m_head->m_next;
	oss << "QueueSize: ";
	while (p != m_head->m_prev) {
		oss << "(" << p->m_step_name << "=" << p->m_out_queue->peekSize() << "), ";
		p = p->m_next;
	}
	oss.resize(oss.size()-2);
	return std::move(oss);
}

int PipelineProcessor::step_ordinal(const PipelineStage* step) const
{
	int ordinal = 0;
	const PipelineStage* p = m_head->m_next;
	while (p != step) {
		p = p->m_next;
		++ordinal;
	}
	return ordinal;
}

int PipelineProcessor::total_steps() const
{
	return step_ordinal(m_head);
}

/// for users, this function is used for self-driven pipeline
///
/// and this function will also be called by compile() for enqueue-driven
/// pipeline: tasks are enqueue'ed by users calling pipeline.enqueue(task)
///
/// self-driven and enqueue-driven are exclusive
///
void PipelineProcessor::start()
{
	assert(m_head);
	assert(total_steps() >= 2 || (total_steps() >= 1 && NULL != m_head->m_out_queue));

// start() will be called at the end of compile(), so:
// if compile() was called, then start() should not be called
// Begin check for double start
	for (PipelineStage* s = m_head->m_next; s != m_head; s = s->m_next) {
		assert(s->m_threads.size() > 0);
		for (size_t i = 0; i < s->m_threads.size(); ++i) {
			// if pipeline was compiled, it should not call start
			TERARK_RT_assert(NULL == s->m_threads[i].m_thread, std::invalid_argument);
		}
	}
// End check for double start

	m_run = true;

	if (NULL == m_mutex)
	{
		m_mutex = new mutex;
		m_is_mutex_owner = true;
	}
	int plgen = -1, plkeep = -1, nth = 0;
	TERARK_UNUSED_VAR(plgen);
	for (PipelineStage* s = m_head->m_next; s != m_head; s = s->m_next)
	{
		if (PipelineStage::ple_generate == s->m_pl_enum)
		{
			assert(-1 == plgen);  // only one generator
			assert(-1 == plkeep); // keep must after generator
			plgen = nth;
		}
		else if (PipelineStage::ple_keep == s->m_pl_enum)
		{
			// must have generator before
			// m_head->m_out_queue is not NULL when data source is external
			assert(-1 != plgen || m_head->m_out_queue);
			plkeep = nth;
		}
		++nth;
	}
	if (-1 != plkeep)
		this->m_keepSerial = true;

	for (PipelineStage* s = m_head->m_next; s != m_head; s = s->m_next)
		s->start(m_queue_size);
}

//! this is the recommended usage
//!
//! compile means no generator step
//! compile just create the pipeline which is ready to consume input
//! input is from out of pipeline, often driven by main thread
void PipelineProcessor::compile()
{
	compile(m_queue_size);
}
void PipelineProcessor::compile(int input_feed_queue_size)
{
// start() will be called at the end of compile(), so:
// if compile() was called, then start() should not be called
// Begin check for double start
	for (PipelineStage* s = m_head->m_next; s != m_head; s = s->m_next) {
		assert(s->m_threads.size() > 0);
		for (size_t i = 0; i < s->m_threads.size(); ++i) {
			// if pipeline was compiled, it should not call start
			TERARK_RT_assert(NULL == s->m_threads[i].m_thread, std::invalid_argument);
		}
	}
// End check for double start
	m_head->m_out_queue = NewQueue(m_EUType, input_feed_queue_size);
	start();
}

void PipelineProcessor::enqueue(PipelineTask* task)
{
    if (EUType::fiber == m_EUType) {
        enqueue_impl(task);
    }
    else {
    	PipelineLockGuard lock(m_mutexForInqueue);
        enqueue_impl(task);
    }
}

void PipelineProcessor::enqueue_impl(PipelineTask* task) {
    FiberYield fy;
	PipelineQueueItem item(++m_head->m_plserial, task);
    if (m_logLevel >= 3) {
        while (!m_head->m_out_queue->push_back(item, m_queue_timeout, &fy)) {
            fprintf(stderr,
                    "Pipeline: enqueue_1: wait push timeout, serial = %lld, retry ...\n",
                    (llong)item.plserial);
        }
    }
    else {
        m_head->m_out_queue->push_back(item, &fy);
    }
}

void PipelineProcessor::enqueue(PipelineTask** tasks, size_t num) {
    if (EUType::fiber == m_EUType) {
        enqueue_impl(tasks, num);
    }
    else {
        PipelineLockGuard lock(m_mutexForInqueue);
        enqueue_impl(tasks, num);
    }
}
void PipelineProcessor::enqueue_impl(PipelineTask** tasks, size_t num) {
    FiberYield fy;
	uintptr_t plserial = m_head->m_plserial;
	auto queue = m_head->m_out_queue;
    if (m_logLevel >= 3) {
        for (size_t i = 0; i < num; ++i) {
            PipelineQueueItem item(++plserial, tasks[i]);
            while (!queue->push_back(item, m_queue_timeout, &fy)) {
                fprintf(stderr,
                        "Pipeline: enqueue(num=%zd): nth=%zd, wait push timeout, serial = %lld, retry ...\n",
                        num, i, (llong)item.plserial);
            }
        }
    }
    else {
        for (size_t i = 0; i < num; ++i) {
            queue->push_back(PipelineQueueItem(++plserial, tasks[i]), &fy);
        }
    }
	m_head->m_plserial = plserial;
}

void PipelineProcessor::wait()
{
	if (NULL != m_head->m_out_queue) {
		assert(!this->m_run); // user must call stop() before wait
	}
	for (PipelineStage* s = m_head->m_next; s != m_head; s = s->m_next)
		s->wait();
}

void PipelineProcessor::add_step(PipelineStage* step)
{
	step->m_owner = this;
	step->m_prev = m_head->m_prev;
	step->m_next = m_head;
	m_head->m_prev->m_next = step;
	m_head->m_prev = step;
}

PipelineProcessor&
PipelineProcessor::operator|(std::pair<intptr_t, function<void(PipelineTask*)> >&& s) {
  *this | new FunPipelineStage(s.first,
  [s=std::move(s)](PipelineStage*, int, PipelineQueueItem* item){
    s.second(item->task);
  });
  return *this;
}

PipelineProcessor&
PipelineProcessor::operator|(std::tuple<int,int, function<void(PipelineTask*)> >&& s) {
  *this | new FunPipelineStage(std::get<0>(s), std::get<1>(s),
  [s=std::move(s)](PipelineStage*, int, PipelineQueueItem* item){
    std::get<2>(s)(item->task);
  });
  return *this;
}

void PipelineProcessor::clear()
{
	assert(!m_run);

	PipelineStage* curr = m_head->m_next;
	while (curr != m_head)
	{
		PipelineStage* next = curr->m_next;
		delete curr;
		curr = next;
	}
	m_head->m_prev = m_head->m_next = m_head;
}

size_t PipelineProcessor::getInputQueueSize(size_t step_no) const {
	PipelineStage* step = m_head;
	for (size_t i = 0; i < step_no; ++i) {
		step = step->m_next;
		assert(step != m_head); // step_no too large
		if (step == m_head) {
			char msg[128];
			sprintf(msg, "invalid step_no=%zd", step_no);
			throw std::invalid_argument(msg);
		}
	}
	return step->m_out_queue->size();
}

} // namespace terark

