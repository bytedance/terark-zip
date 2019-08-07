/* vim: set tabstop=4 : */
#ifndef __terark_pipeline_hpp__
#define __terark_pipeline_hpp__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
# pragma warning(push)
# pragma warning(disable: 4018)
# pragma warning(disable: 4267)
#endif

#include <stddef.h>
#include <string>
#include <type_traits>
#include <terark/config.hpp>
#include <terark/valvec.hpp>
#include <terark/util/function.hpp>

#if defined(TERARK_CONCURRENT_QUEUE_USE_BOOST)
	namespace boost {
		// forward declaration
		// avoid app compile time dependency to <boost/thread.hpp>
		class thread;
		class mutex;
	}
#else
	#include <mutex>
	#include <thread>
#endif

namespace terark {

#if defined(TERARK_CONCURRENT_QUEUE_USE_BOOST)
	using boost::thread;
	using boost::mutex;
	typedef boost::lock_guard<boost::mutex> PipelineLockGuard;
#else
	using std::thread;
	using std::mutex;
	typedef std::lock_guard<std::mutex> PipelineLockGuard;
#endif

class TERARK_DLL_EXPORT PipelineTask
{
public:
	virtual ~PipelineTask();
};

class TERARK_DLL_EXPORT PipelineQueueItem
{
public:
	uintptr_t plserial;
	PipelineTask* task;

	PipelineQueueItem(uintptr_t plserial, PipelineTask* task)
		: plserial(plserial)
		, task(task)
	{}

	PipelineQueueItem()
		: plserial(0)
		, task(0)
	{}
};

class TERARK_DLL_EXPORT PipelineProcessor;

class TERARK_DLL_EXPORT PipelineStage : boost::noncopyable
{
	friend class PipelineProcessor;

public:
	class queue_t;
	class ExecUnit; // ExecUnit is thread or fiber

protected:
	queue_t* m_out_queue;

	PipelineStage *m_prev, *m_next;
	PipelineProcessor* m_owner;

	struct ThreadData : boost::noncopyable {
		std::string m_err_text;
		ExecUnit*  m_thread;
		volatile size_t m_run; // size_t is a CPU word, should be bool
		ThreadData();
		~ThreadData();
	};
	valvec<ThreadData> m_threads;
	enum {
		ple_none,
		ple_generate,
		ple_keep
	} m_pl_enum;
	uintptr_t m_plserial;
	int m_fibers_per_thread;
	volatile int m_running_exec_units;

	void run_wrapper(int threadno);

	void run_step_first(int threadno);
	void run_step_last(int threadno);
	void run_step_mid(int threadno);

	void run_serial_step_slow(int threadno, void (PipelineStage::*fdo)(PipelineQueueItem&));
	void run_serial_step_fast(int threadno, void (PipelineStage::*fdo)(PipelineQueueItem&));
	void serial_step_do_mid(PipelineQueueItem& item);
	void serial_step_do_last(PipelineQueueItem& item);

	bool isPrevRunning();
	bool isRunning() { return 0 != m_running_exec_units; }
	void start(int queue_size);
	void wait();
	void stop();

protected:
	virtual void process(int threadno, PipelineQueueItem* task) = 0;

	virtual void setup(int threadno);
	virtual void clean(int threadno);

	virtual void run(int threadno);
	virtual void onException(int threadno, const std::exception& exp);

public:
	std::string m_step_name;

	//! @param thread_count 0 indicate keepSerial, -1 indicate generate serial
	explicit PipelineStage(int thread_count);
	PipelineStage(int thread_count, int fibers_per_thread);

	virtual ~PipelineStage();

	int step_ordinal() const;
	const std::string& err(int threadno) const;

	// helper functions:
	std::string msg_leading(int threadno) const;
	mutex* getMutex() const;
	size_t getInputQueueSize()  const;
	size_t getOutputQueueSize() const;
	void createOutputQueue(size_t size);
};

class TERARK_DLL_EXPORT FunPipelineStage : public PipelineStage
{
	function<void(PipelineStage*, int, PipelineQueueItem*)> m_process; // take(this, threadno, task)

protected:
	void process(int threadno, PipelineQueueItem* task);

public:
	FunPipelineStage(int thread_count,
					 const function<void(PipelineStage*, int, PipelineQueueItem*)>&,
					 const std::string& step_name = "");
	FunPipelineStage(int thread_count, int fibers_per_thread,
					 const function<void(PipelineStage*, int, PipelineQueueItem*)>&,
					 const std::string& step_name = "");
	~FunPipelineStage(); // move destructor into libterark-thread*
};

class TERARK_DLL_EXPORT PipelineProcessor
{
	friend class PipelineStage;

	PipelineStage *m_head;
	int m_queue_size;
	int m_queue_timeout;
	function<void(PipelineTask*)> m_destroyTask;
	mutex* m_mutex;
	mutex  m_mutexForInqueue;
	volatile size_t m_run; // size_t is CPU word, should be bool
	bool m_is_mutex_owner;
	bool m_keepSerial;
	signed char m_logLevel;
	bool m_fiberMode;

protected:
	static void defaultDestroyTask(PipelineTask* task);
	virtual void destroyTask(PipelineTask* task);

	void add_step(PipelineStage* step);
	void clear();

	void enqueue_impl(PipelineTask* task);
	void enqueue_impl(PipelineTask** tasks, size_t num);

public:
	static int sysCpuCount();

	PipelineProcessor();

	virtual ~PipelineProcessor();

	bool isRunning() const { return 0 != m_run; }

    void setLogLevel(int level) { m_logLevel = (signed char)level; }
    int  getLogLevel() const { return m_logLevel; }

    void setFiberMode(bool fiberMode) { m_fiberMode = fiberMode; }
    bool isFiberMode() const { return m_fiberMode; }

	void setQueueSize(int queue_size) { m_queue_size = queue_size; }
	int  getQueueSize() const { return m_queue_size; }
	void setQueueTimeout(int queue_timeout) { m_queue_timeout = queue_timeout; }
	int  getQueueTimeout() const { return m_queue_timeout; }
	void setDestroyTask(const function<void(PipelineTask*)>& fdestory) { m_destroyTask = fdestory; }
	void setMutex(mutex* pmutex);
	mutex* getMutex() { return m_mutex; }

	std::string queueInfo();

	int step_ordinal(const PipelineStage* step) const;
	int total_steps() const;

	PipelineProcessor& operator| (PipelineStage* step) { this->add_step(step); return *this; }
	PipelineProcessor& operator>>(PipelineStage* step) { this->add_step(step); return *this; }

	PipelineProcessor& operator| (std::pair<intptr_t, function<void(PipelineTask*)> >&&);
	PipelineProcessor& operator>>(std::pair<intptr_t, function<void(PipelineTask*)> >&&s) { return *this | std::move(s);}

	PipelineProcessor& operator| (std::tuple<int,int, function<void(PipelineTask*)> >&&);
	PipelineProcessor& operator>>(std::tuple<int,int, function<void(PipelineTask*)> >&&s) { return *this | std::move(s);}

	void start();
	void compile(); // input feed from external, not first step
	void compile(int input_feed_queue_size /* default = m_queue_size */);

	void enqueue(PipelineTask* task);
	void enqueue(PipelineTask** tasks, size_t num);

	template<class Task>
	typename
	std::enable_if<std::is_base_of<PipelineTask, Task>::value, void>::type
	enqueue(Task** tasks, size_t num) {
	    static_assert(static_cast<Task*>((PipelineTask*)0) == 0,
	                  "PipelineTask must be first base of Task");
        enqueue(reinterpret_cast<PipelineTask**>(tasks), num);
	}

	void stop() { m_run = false; }
	void wait();
	size_t getInputQueueSize(size_t step_no) const;
};

#define PPL_STAGE(pObject, Class, MemFun, thread_count, ...) \
	new terark::FunPipelineStage(thread_count\
		, terark::bind(&Class::MemFun, pObject, _1, _2, _3,##__VA_ARGS__) \
		, BOOST_STRINGIZE(Class::MemFun)\
		)
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

} // namespace terark

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma warning(pop)
#endif

#endif // __terark_pipeline_hpp__

