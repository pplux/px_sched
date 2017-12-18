# px_sched
Single Header C++ Task Scheduler 

Written in C++11, with no dependency, and easy to integrate. See the [examples](https://github.com/pplux/px_sched/tree/master/examples).

## API:

```cpp
int main(int, char **) {
  px::Scheduler schd;
  schd.init();

  px::Sync s1,s2,s3;
  for(size_t i = 0; i < 10; ++i) {
    auto job = [i] {
      printf("Phase 1: Task %zu completed from %s\n",
       i, px::Scheduler::current_thread_name());
    };
    schd.run(job, &s1);
  }

  for(size_t i = 0; i < 10; ++i) {
    auto job = [i] {
      printf("Phase 2: Task %zu completed from %s\n",
       i, px::Scheduler::current_thread_name());
    };
    schd.runAfter(s1, job, &s2);
  }

  for(size_t i = 0; i < 10; ++i) {
    auto job = [i] {
      printf("Phase 3: Task %zu completed from %s\n",
       i, px::Scheduler::current_thread_name());
    };
    schd.runAfter(s2, job, &s3);
  }

  px::Sync last = s3;
  printf("Waiting for tasks to finish...\n");
  schd.waitFor(last); // wait for all tasks to finish
  printf("Waiting for tasks to finish...DONE \n");

  return 0;
}
```

`px::Scheduler` is the main object, normally you should only instance one, but
that's up to you. There are two methods to launch tasks:

* `run(const px::Job &job, px::Sync *optional_sync_object = nullptr)`
* `runAfter(const px::Sync trigger, const px::Job &job, px::Sync *out_optional_sync_obj = nullptr)`

Both run methods receive a `Job` object, by default it is a `std::function<void()>` but you can [customize](https://github.com/pplux/px_sched/blob/master/examples/example2.cpp) to fit your needs. 

Both run methods receive an optional output argument, a `Sync` object. `Sync` objects are used to coordinate dependencies between groups of tasks (or single tasks). The simplest case is to wait for a group of tasks to finish:

```cpp
px::Sync s;
for(size_t i = 0; i < 128; ++i) {
  schd.run([i]{printf("Task %zu\n",i);}, &s);
}
schd.waitFor(s);
```

`Sync` objects can also be used to launch tasks when a group of tasks have finished with the method `px::Scheduler::runAfter`:

```cpp
px::Sync s;
for(size_t i = 0; i < 128; ++i) {
  schd.run([i]{printf("Task %zu\n",i);}, &s);
}
px::Sync last;
schd.runAfter(s, []{printf("Last Task\n");}, last);

schd.waitFor(last);
```

## TODO's
* [  ] improve documentation
* [  ] Add support for Windows Fibers on windows
* [  ] Add support for ucontext on Posix

