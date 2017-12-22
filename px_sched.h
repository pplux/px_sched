/* -----------------------------------------------------------------------------
Copyright (c) 2017 Jose L. Hidalgo (PpluX)

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
----------------------------------------------------------------------------- */

#ifndef PX_SCHED
#define PX_SCHED

// -- Job Object ----------------------------------------------------------
// if you want to use your own job objects, define the following
// macro and provide a struct px::Job object with an operator() method.
//
// Example, a C-like pointer to function:
//
//    #define PX_SCHED_CUSTOM_JOB_DEFINITION
//    namespace px {
//      struct Job {
//        void (*func)(void *arg);
//        void *arg;
//        void operator()() { func(arg); }
//      };
//    } // px namespace
//
//  By default Jobs are simply std::function<void()>
//
#ifndef PX_SCHED_CUSTOM_JOB_DEFINITION
#include <functional>
namespace px {
  typedef std::function<void()> Job;
} // px namespace
#endif
// -----------------------------------------------------------------------------

// -- Backend selection --------------------------------------------------------
// Right now there is only two backends(single-threaded, and regular threads),
// in the future we will add windows-fibers and posix-ucontext. Meanwhile try
// to avoid waitFor(...) and use more runAfter if possible. Try not to suspend
// threads on external mutexes.
#if !defined(PX_SCHED_CONFIG_SINGLE_THREAD)  && \
    !defined(PX_SCHED_CONFIG_REGULAR_THREADS)
# define PX_SCHED_CONFIG_REGULAR_THREADS 1
#endif

#ifdef PX_SCHED_CONFIG_REGULAR_THREADS
#  define PX_SCHED_IMP_REGULAR_THREADS 1
#else
#  define PX_SCHED_IMP_REGULAR_THREADS 0
#endif

#ifdef PX_SCHED_CONFIG_SINGLE_THREAD
#  define PX_SCHED_IMP_SINGLE_THREAD 1
#else
#  define PX_SCHED_IMP_SINGLE_THREAD 0
#endif

#if ( PX_SCHED_IMP_SINGLE_THREAD   \
    + PX_SCHED_IMP_REGULAR_THREADS \
    ) != 1
#error "PX_SCHED: Only one backend must be enabled (and at least one)"
#endif

// -----------------------------------------------------------------------------

// some checks, can be ommited if you're confident there is no
// missuse of the library.
#ifndef PX_SCHED_CHECK
#  include <cstdlib>
#  include <cstdio>
#  define PX_SCHED_CHECK(cond, ...) \
      if (!(cond)) { \
        printf("-- PX_SCHED ERROR: -------------------\n"); \
        printf("-- %s:%d\n", __FILE__, __LINE__); \
        printf("--------------------------------------\n"); \
        printf(__VA_ARGS__);\
        printf("\n--------------------------------------\n"); \
        abort(); \
      } 
#endif

#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <cstring>
#include <memory>

namespace px {

  // Sync object
  class Sync {
    uint32_t hnd = 0;
    friend class Scheduler;
  };

  struct SchedulerParams {
    uint16_t num_threads = 16;        // num OS threads created 
    uint16_t max_running_threads = 0; // 0 --> will be set to max hardware concurrency
    uint16_t max_number_tasks = 1024; // max number of simultaneous tasks
    uint16_t thread_num_tries_on_idle = 16;   // number of tries before suspend the thread
    uint32_t thread_sleep_on_idle_in_microseconds = 5; // time spent waiting between tries
    // TODO:
    // uint16_t max_number_fibers = 128; // number of fibers (not all platforms)
  };


  // -- ObjectPool -------------------------------------------------------------
  // holds up to 2^20 objects with ref counting and versioning
  // used internally by the Scheduler for tasks and counters, but can also
  // be used as a thread-safe object pool

  template<class T>
  struct ObjectPool {
    const uint32_t kPosMask = 0x000FFFFF; // 20 bits
    const uint32_t kRefMask = kPosMask;   // 20 bits
    const uint32_t kVerMask = 0xFFF00000; // 12 bits
    const uint32_t kVerDisp = 20;

    void init(uint32_t count);
    void reset();

    // only access objects you've previously referenced
    T& get(uint32_t hnd);

    // only access objects you've previously referenced
    const T& get(uint32_t hnd) const;

    // given a position inside the pool returns the handler and also current ref
    // count and current version number (only used for debugging)
    uint32_t info(size_t pos, uint32_t *count, uint32_t *ver) const;

    // max number of elements hold by the object pool
    uint32_t size() const { return count_; }

    // retunrs the handler of an object in the pool that can be used
    // it also increments in one the number of references (no need to call ref)
    uint32_t adquireAndRef();

    void unref(uint32_t hnd) const;

    // decrements the counter, if the object is no longer valid (last ref)
    // the given function will be executed with the element
    template<class F>
    void unref(uint32_t hnd, F f) const;

    // returns true if the given position was a valid object
    bool ref(uint32_t hnd) const;

  private:
    struct D {
      T element;
      mutable std::atomic<uint32_t> state = {0};
      uint32_t version = 0;
      char padding[64]; // Avoid false sharing between threads
    };
    std::unique_ptr<D[]> data_;
    std::atomic<uint32_t> next_;
    size_t count_ = 0;
  };


  class Scheduler {
  public:
    Scheduler();
    ~Scheduler();

    void init(const SchedulerParams &params = SchedulerParams());
    void stop();

    void run(const Job &job, Sync *out_sync_obj = nullptr);
    void runAfter(Sync sync,const Job &job, Sync *out_sync_obj = nullptr);
    void waitFor(Sync sync); //< suspend current thread 
    void getDebugStatus(char *buffer, size_t buffer_size) const;

    // manually increment the value of a Sync object. Sync objects triggers
    // when they reach 0.
    // *WARNING*: calling increment without a later decrement might leave
    //            tasks waiting forever, and will leak resources.
    void incrementSync(Sync *s);

    // manually decrement the value of a Sync object.
    // *WARNING*: never call decrement on a sync object wihtout calling 
    //            @incrementSync first.
    void decrementSync(Sync *s);

    // By default workers will be named as Worker-id
    static void set_current_thread_name(const char *name);
    static const char *current_thread_name();

    // Call this method before a mutex/lock/etc... to notify the scheduler
    static void CurrentThreadSleeps();

    // call this again to notify the thread is again running
    static void CurrentThreadWakesUp();

  private:
    struct TLS;
    static TLS* tls();
    void wakeUpOneThread();
    SchedulerParams params_;
    std::atomic<uint16_t> active_threads_;
    bool running_ = false;

#ifdef PX_SCHED_IMP_REGULAR_THREADS
    struct IndexQueue {
      void init(uint16_t max) {
        size_ = max;
        in_use_ = 0;
        lock_ = false;
        list_ = std::unique_ptr<uint32_t[]>(new uint32_t[size_]);
      }
      void push(uint32_t p) {
        for(;;) {
          bool expected = false;
          if (lock_.compare_exchange_strong(expected, true)) break;
        }
        list_[in_use_] = p;
        in_use_++;
        lock_ = false;
      }
      uint16_t in_use() const { return in_use_; }
      bool pop(uint32_t *res) {
       for(;;) {
          bool expected = false;
          if (lock_.compare_exchange_strong(expected, true)) break;
        }
        bool result = false;
        if (in_use_) {
          in_use_--;
          if (res) *res = list_[in_use_];
          result = true;
        }
        lock_ = false;
        return result;
      }
      uint16_t size_;
      volatile uint16_t in_use_;
      std::unique_ptr<uint32_t[]> list_;
      std::atomic<bool> lock_;
    };

    struct Counter;
    struct Task;
    struct Wait;

    struct Task {
      Job job;
      uint32_t counter_id;
      std::atomic<uint32_t> next_sibling_task;
    };

    struct WaitFor {
      explicit WaitFor() 
        : owner(std::this_thread::get_id())
        , ready(false) {}
      void wait() {
        PX_SCHED_CHECK(this, "Invalid WaitFor Object");
        PX_SCHED_CHECK(std::this_thread::get_id() == owner,
            "WaitFor::wait can only be invoked from the thread "
            "that created the object");
        std::unique_lock<std::mutex> lk(mutex);
        if(!ready) {
          condition_variable.wait(lk);
        }
      }
      void signal() {
        if (owner != std::this_thread::get_id()) {
          std::lock_guard<std::mutex> lk(mutex);
          ready = true;
          condition_variable.notify_all();
        } else {
          ready = true;
        }
      }
    private:
      std::thread::id const owner;
      std::mutex mutex;
      std::condition_variable condition_variable;
      bool ready;
    };

    struct Worker {
      std::thread thread;
       // setted by the thread when is sleep
      std::atomic<WaitFor*> wake_up = {nullptr};
    };

    struct Counter {
      std::atomic<uint32_t> task_id;
      std::atomic<uint32_t> user_count;
      WaitFor *wait_ptr = nullptr;
    };

    uint16_t wakeUpThreads(uint16_t max_num_threads);
    uint32_t createTask(const Job &job, Sync *out_sync_obj);
    uint32_t createCounter();
    void unrefCounter(uint32_t counter_hnd);

    std::unique_ptr<Worker[]> workers_;
    ObjectPool<Task> tasks_;
    ObjectPool<Counter> counters_;
    IndexQueue ready_tasks_;

    static void WorkerThreadMain(Scheduler *schd, uint16_t id);
#endif 

  };

  //-- Object pool implementation ----------------------------------------------
  template<class T>
  inline void ObjectPool<T>::init(uint32_t count) {
    data_ = std::unique_ptr<D[]>(new D[count]);
    for(uint32_t i = 0; i < count; ++i) {
      data_[i].state = 0xFFFu<< kVerDisp;
    }
    count_ = count;
    next_ = 0;
  }

  template<class T>
  inline void ObjectPool<T>::reset() {
    count_ = 0;
    next_ = 0;
    data_.reset();
  }

  // only access objects you've previously referenced
  template<class T>
  inline T& ObjectPool<T>::get(uint32_t hnd) {
    uint32_t pos = hnd & kPosMask;
    PX_SCHED_CHECK(pos < count_, "Invalid access to pos %u from %zu", pos, count_);
    return data_[pos].element;
  }

  // only access objects you've previously referenced
  template< class T>
  inline const T&  ObjectPool<T>::get(uint32_t hnd) const {
    uint32_t pos = hnd & kPosMask;
    PX_SCHED_CHECK(pos < count_, "Invalid access to pos %zu from %zu", pos, count_);
    return data_[pos].element;
  }

  template< class T>
  inline uint32_t ObjectPool<T>::info(size_t pos, uint32_t *count, uint32_t *ver) const {
    PX_SCHED_CHECK(pos < count_, "Invalid access to pos %zu from %zu", pos, count_);
    uint32_t s = data_[pos].state.load();
    if (count) *count = (s & kRefMask);
    if (ver) *ver = (s & kVerMask) >> kVerDisp;
    return (s&kVerMask) | pos;
  }

  template<class T>
  inline uint32_t ObjectPool<T>::adquireAndRef() {
    uint32_t tries = 0;
    for(;;) {
      uint32_t pos = (next_.fetch_add(1)%count_);
      D& d = data_[pos];
      uint32_t version = (d.state.load() & kVerMask) >> kVerDisp;
      // note: avoid 0 as version
      uint32_t newver = (version+1) & 0xFFF;
      if (newver == 0) newver = 1;
      // instead of using 1 as initial ref, we use 2, when we see 1
      // in the future we know the object must be freed, but it wont
      // be actually freed until it reaches 0
      uint32_t newvalue = (newver << kVerDisp) + 2;
      uint32_t expected = version << kVerDisp;
      if (d.state.compare_exchange_strong(expected, newvalue)) {
        return (newver << kVerDisp) | (pos & kPosMask);
      }
      tries++;
#if 1
      PX_SCHED_CHECK(tries < count_*count_, "It was not possible to find a valid index after %u tries", tries);
#else
      if (tries > count_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(tries-count_));
      }
      if (tries % 20) {
        printf("Could not adquire value = %x[%lu] num tries %u\n", expected, pos%count_, tries); 
      }
#endif
    }
  }

  template< class T>
  inline void ObjectPool<T>::unref(uint32_t hnd) const {
    uint32_t pos = hnd & kPosMask;
    uint32_t ver = (hnd & kVerMask);
    D& d = data_[pos];
    for(;;) {
      uint32_t prev = d.state.load();
      uint32_t next = prev - 1;
      PX_SCHED_CHECK((prev & kVerMask) == ver,
          "Invalid unref HND = %u(%u), Versions: %u vs %u",
          pos, hnd, prev & kVerMask, ver);
      PX_SCHED_CHECK((prev & kRefMask) > 1,
          "Invalid unref HND = %u(%u), invalid ref count",
          pos, hnd);
      if (d.state.compare_exchange_strong(prev, next)) {
        if ((next & kRefMask) == 1) {
          d.state = 0;
        }
        return;
      }
    }
  }

  // decrements the counter, if the objet is no longer valid (las ref)
  // the given function will be executed with the element
  template<class T>
  template<class F>
  inline void ObjectPool<T>::unref(uint32_t hnd, F f) const {
    uint32_t pos = hnd & kPosMask;
    uint32_t ver = (hnd & kVerMask);
    D& d = data_[pos];
    for(;;) {
      uint32_t prev = d.state.load();
      uint32_t next = prev - 1;
      PX_SCHED_CHECK((prev & kVerMask) == ver,
          "Invalid unref HND = %u(%u), Versions: %u vs %u",
          pos, hnd, prev & kVerMask, ver);
      PX_SCHED_CHECK((prev & kRefMask) > 1,
          "Invalid unref HND = %u(%u), invalid ref count",
          pos, hnd);
      if (d.state.compare_exchange_strong(prev, next)) {
        if ((next & kRefMask) == 1) {
          f(d.element);
          d.state = 0;
        }
        return;
      }
    }
  }

  template< class T>
  inline bool ObjectPool<T>::ref(uint32_t hnd) const{
    if (!hnd) return false;
    uint32_t pos = hnd & kPosMask;
    uint32_t ver = (hnd & kVerMask);
    D& d = data_[pos];
    for (;;) {
      uint32_t prev = d.state.load();
      uint32_t next_c =((prev & kRefMask) +1);
      if ((prev & kVerMask) != ver || next_c <= 2) return false;
      PX_SCHED_CHECK(next_c  == (next_c & kRefMask), "Too many references...");
      uint32_t next = (prev & kVerMask) | next_c ;
      if (d.state.compare_exchange_strong(prev, next)) {
        return true;
      }
    }
  }


} // end of px namespace

#ifdef PX_SCHED_IMPLEMENTATION

#ifdef PX_SCHED_CUSTOM_TLS
// used to store custom TLS... some platfor might have problems with
// TLS (iOS usted to be one)
#include <unordered_map>
#endif

namespace px {

  struct Scheduler::TLS {
    std::unique_ptr<char[]> name;
    Scheduler *scheduler = {nullptr};
  };

  Scheduler::TLS* Scheduler::tls() {
#ifdef PX_SCHED_CUSTOM_TLS
    static std::unordered_map<std::thread::id, TLS> data;
    static std::atomic<bool> in_use = {false};
    for(;;) {
      bool expected = false;
      if (in_use.compare_exchange_weak(expected, true)) break;
    }
    auto result = &data[std::this_thread::get_id()];
    in_use = false;
    return result;
#else
    static thread_local TLS tls;
    return &tls;
#endif
  }

  void Scheduler::set_current_thread_name(const char *name) {
    TLS *d = tls();
    if (name) {
      size_t len = strlen(name)+1;
      d->name = std::unique_ptr<char[]>(new char[len]);
      memcpy(d->name.get(), name, len);
    } else {
      d->name.reset();
    }
  }

  const char *Scheduler::current_thread_name() {
    TLS *d = tls();
    return d->name.get();
  }

  void Scheduler::CurrentThreadSleeps() {
    TLS *d = tls();
    if (d->scheduler) {
      d->scheduler->active_threads_.fetch_sub(1);
      d->scheduler->wakeUpOneThread();
    }
  }

  void Scheduler::CurrentThreadWakesUp() {
    TLS *d = tls();
    if (d->scheduler) {
      d->scheduler->active_threads_.fetch_add(1);
    }
  }
}

#if PX_SCHED_IMP_SINGLE_THREAD
// Implementation with no threads (single threaded) every added Job it is
// executed inmediately
namespace px {
  Scheduler::Scheduler() {}
  Scheduler::~Scheduler() {}
  void Scheduler::init(const SchedulerParams &) {}
  void Scheduler::stop() {}
  void Scheduler::run(const Job &job, Sync *) { Job j(job); j(); }
  void Scheduler::runAfter(Sync s, const Job &job, Sync *) { Job j(job); j();}
  void Scheduler::waitFor(Sync) {}
  void Scheduler::getDebugStatus(char *buffer, size_t buffer_size) const {
    if (buffer_size) buffer[0] = 0;
  }
  void Scheduler::incrementSync(Sync *s) {}
  void Scheduler::decrementSync(Sync *s) {}
  void Scheduler::wakeUpOneThread() {}
} // end of px namespace
#endif // PX_SCHED_IMP_SINGLE_THREAD

#if PX_SCHED_IMP_REGULAR_THREADS
// Default implementation using threads 
#include <thread>
namespace px {
  Scheduler::Scheduler() {
    active_threads_ = 0;
  }

  Scheduler::~Scheduler() { stop(); }

  void Scheduler::init(const SchedulerParams &params) {
    stop();
    running_ = true;
    params_ = params;
    if (params_.max_running_threads == 0) {
      params_.max_running_threads = std::thread::hardware_concurrency();
    }
    // create tasks
    tasks_.init(params_.max_number_tasks);
    counters_.init(params_.max_number_tasks);
    ready_tasks_.init(params_.max_number_tasks);
    workers_ = std::unique_ptr<Worker[]>(new Worker[params_.num_threads]);
    PX_SCHED_CHECK(active_threads_.load() == 0, "Invalid active threads num");
    for(uint16_t i = 0; i < params_.num_threads; ++i) {
      workers_[i].thread = std::thread(WorkerThreadMain, this, i);
    }
  }

  void Scheduler::stop() {
    if (running_) {
      running_ = false;
      for(uint16_t i = 0; i < params_.num_threads; ++i) {
        wakeUpThreads(params_.num_threads);
      }
      for(uint16_t i = 0; i < params_.num_threads; ++i) {
        workers_[i].thread.join();
      }
      workers_.reset();
      tasks_.reset();
      PX_SCHED_CHECK(active_threads_.load() == 0, "Invalid active threads num --> %u", active_threads_.load());
    }
  }
  
  void Scheduler::getDebugStatus(char *buffer, size_t buffer_size) const {
    size_t p = 0;
    int n = 0;
    #define _ADD(...) {p += static_cast<size_t>(n); (p < buffer_size) && (n = snprintf(buffer+p, buffer_size-p,__VA_ARGS__));}
    _ADD("CPUS:0    5    10   15   20   25   30   35   40   45   50   55   60   65   70   75\n");
    _ADD("%4u:", active_threads_.load());
    for(size_t i = 0; i < params_.num_threads; ++i) {
      _ADD( (workers_[i].wake_up.load() == nullptr)?"*":".");
    }
    _ADD("\nReady:   ");
    for(size_t i = 0; i < ready_tasks_.in_use(); ++i) {
      _ADD("%d,",ready_tasks_.list_[i]);
    }
    _ADD("\nTasks:   ");
    for(size_t i = 0; i < tasks_.size(); ++i) {
      uint32_t c,v;
      uint32_t hnd = tasks_.info(i, &c, &v);
      if (c>0) { _ADD("%u,",hnd); }
    }
    _ADD("\nCounters:");
    for(size_t i = 0; i < counters_.size(); ++i) {
      uint32_t c,v;
      uint32_t hnd = counters_.info(i, &c, &v);
      if (c>0) { _ADD("%u,",hnd); }
    }
    _ADD("\n");
    #undef _ADD
  }

  uint32_t Scheduler::createCounter() {
    uint32_t hnd = counters_.adquireAndRef();
    Counter *c = &counters_.get(hnd);
    c->task_id = 0;
    c->user_count = 0;
    c->wait_ptr = nullptr;
    return hnd;
  }

  uint32_t Scheduler::createTask(const Job &job, Sync *sync_obj) {
    uint32_t ref = tasks_.adquireAndRef();
    Task *task = &tasks_.get(ref);
    task->job = job;
    task->counter_id = 0;
    task->next_sibling_task = 0;
    if (sync_obj) {
      bool new_counter = !counters_.ref(sync_obj->hnd);
      if (new_counter) {
        sync_obj->hnd = createCounter();
      }
      task->counter_id = sync_obj->hnd;
    }
    return ref;
  }

  uint16_t Scheduler::wakeUpThreads(uint16_t max_num_threads) {
    uint16_t woken_up = 0;
    for(uint32_t i = 0; (i < params_.num_threads) && (woken_up < max_num_threads); ++i) {
      WaitFor *wake_up = workers_[i].wake_up.exchange(nullptr);
      if (wake_up) {
        wake_up->signal();
        woken_up++;
      }
    }
    return woken_up;
  }

  void Scheduler::wakeUpOneThread() {
    for(;;) {
      uint32_t active = active_threads_.load();
      if ((active >= params_.max_running_threads) ||
          (ready_tasks_.in_use() < active) ||
          wakeUpThreads(1)) return;
    }
  }

  void Scheduler::run(const Job &job, Sync *sync_obj) {
    PX_SCHED_CHECK(running_, "Scheduler not running");
    uint32_t t_ref = createTask(job, sync_obj);
    ready_tasks_.push(t_ref);
    wakeUpOneThread();
  }

  void Scheduler::runAfter(Sync _trigger, const Job& _job, Sync* _sync_obj) {
    PX_SCHED_CHECK(running_, "Scheduler not running");
    uint32_t trigger = _trigger.hnd;
    uint32_t t_ref = createTask(_job, _sync_obj);
    bool valid = counters_.ref(trigger);
    if (valid) {
      Counter *c = &counters_.get(trigger);
      for(;;) {
        uint32_t current = c->task_id.load();
        if (c->task_id.compare_exchange_strong(current, t_ref)) {
          Task *task = &tasks_.get(t_ref);
          task->next_sibling_task = current;
          break;
        }
      }
      unrefCounter(trigger);
    } else {
      ready_tasks_.push(t_ref);
      wakeUpOneThread();
    }
  }

  void Scheduler::waitFor(Sync s) {
    if (counters_.ref(s.hnd)) {
      Counter &counter = counters_.get(s.hnd);
      PX_SCHED_CHECK(counter.wait_ptr == nullptr, "Sync object already used for waitFor operation, only one is permited");
      WaitFor wf;
      counter.wait_ptr = &wf;
      unrefCounter(s.hnd);
      CurrentThreadSleeps(); 
      wf.wait();
      CurrentThreadWakesUp(); 
    }
  }

  void Scheduler::unrefCounter(uint32_t hnd) {
    if (counters_.ref(hnd)) {
      counters_.unref(hnd);
      Scheduler *schd = this;
      counters_.unref(hnd, [schd](Counter &c) {
        // wake up all tasks 
        uint32_t tid = c.task_id;
        while (schd->tasks_.ref(tid)) {
          Task &task = schd->tasks_.get(tid);
          uint32_t next_tid = task.next_sibling_task; 
          task.next_sibling_task = 0;
          schd->ready_tasks_.push(tid);
          schd->wakeUpOneThread();
          schd->tasks_.unref(tid);
          tid = next_tid;
        }
        if (c.wait_ptr) {
          c.wait_ptr->signal();
        }
      });
    }
  }

  void Scheduler::incrementSync(Sync *s) {
    if (!counters_.ref(s->hnd)) {
      s->hnd = createCounter();
    }
    Counter &c = counters_.get(s->hnd);
    uint32_t prev = c.user_count.fetch_add(1);
    // first one will leave the ref count incremented
    if (prev != 0) {
      unrefCounter(s->hnd);
    }
  }

  void Scheduler::decrementSync(Sync *s) {
    if (counters_.ref(s->hnd)) {
      Counter &c = counters_.get(s->hnd);
      uint32_t prev = c.user_count.fetch_sub(1);
      if (prev == 1) {
        // last one should unref twice
        unrefCounter(s->hnd);
      }
      unrefCounter(s->hnd);
    }
  }

  void Scheduler::WorkerThreadMain(Scheduler *schd, uint16_t id) {
    char buffer[32];

    auto const ttl_wait = schd->params_.thread_sleep_on_idle_in_microseconds;
    auto const ttl_value = schd->params_.thread_num_tries_on_idle? schd->params_.thread_num_tries_on_idle:1;
    schd->active_threads_.fetch_add(1);
    tls()->scheduler = schd;
    for(;;) {
      { // wait for new activity
        auto current_num = schd->active_threads_.fetch_sub(1);
        if (!schd->running_) return;
        if (schd->ready_tasks_.in_use() == 0 ||
            current_num > schd->params_.max_running_threads) {
          WaitFor wf;
          schd->workers_[id].wake_up = &wf;
          snprintf(buffer, 32, "Worker-%u [SLEEP]", id);
          schd->set_current_thread_name(buffer);
          wf.wait();
          if (!schd->running_) return;
        }
        schd->active_threads_.fetch_add(1);
        schd->workers_[id].wake_up = nullptr;
        snprintf(buffer,32,"Worker-%u", id);
        schd->set_current_thread_name(buffer);
      }
      auto ttl = ttl_value;
      { // do some work
        uint32_t task_ref;
        while (ttl && schd->running_ ) {
          if (!schd->ready_tasks_.pop(&task_ref)) {
            ttl--;
            if (ttl_wait) std::this_thread::sleep_for(std::chrono::microseconds(ttl_wait));
            continue;
          }
          ttl = ttl_value;
          Task *t = &schd->tasks_.get(task_ref);
          t->job();
          uint32_t counter = t->counter_id;
          //printf("* Task %u --> %u\n", task_ref, counter);
          schd->tasks_.unref(task_ref);
          schd->unrefCounter(counter);
        }
      }
    }
    tls()->scheduler = nullptr;
  }
} // end of px namespace
#endif // PX_SCHED_IMP_REGULAR_THREADS

#endif // PX_SCHED_IMPLEMENTATION
#endif // PX_SCHED
