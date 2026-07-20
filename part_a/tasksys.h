#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>


/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */


struct state {
    TaskID taskID;
    int total_tasks;
    IRunnable* runner;
    int finished_subtasks;
    int remaining_deps;
    std::vector<TaskID> dependents;

    state(TaskID id, int total, IRunnable* runnable)
        : taskID(id),
          total_tasks(total),
          runner(runnable),
          finished_subtasks(0),
          remaining_deps(0) {
    }

    state(const state&) = delete;
    state& operator=(const state&) = delete;
};

struct WorkItem {
    state* group;
    int task_index;

    WorkItem(state* task_group = NULL, int index = -1)
        : group(task_group),
          task_index(index) {
    }
};

class TaskSystemParallelThreadPoolSleeping : public ITaskSystem {
public:
    TaskSystemParallelThreadPoolSleeping(int num_threads);
    ~TaskSystemParallelThreadPoolSleeping();

    const char* name();

    void run(
        IRunnable* runnable,
        int num_total_tasks
    );

    TaskID runAsyncWithDeps(
        IRunnable* runnable,
        int num_total_tasks,
        const std::vector<TaskID>& deps
    );

    void sync();

private:
    // --- declaration order matches the .cpp initializer list below ---
    // (C++ always initializes members in declaration order, not
    // initializer-list order, so keeping these aligned avoids
    // -Wreorder / -Werror failures and keeps the two files easy to
    // read side by side.)

    int num_threads_;
    bool shutdown;
    TaskID curr;
    int demanded;
    int completed;

    std::vector<std::unique_ptr<state> > states;
    std::vector<bool> ready;
    std::queue<WorkItem> ready_queue;
    std::vector<std::thread> workers;

    std::mutex mtx_process;
    std::condition_variable cv_work;
    std::condition_variable cv_sync;

    void execute(int thread_id);
    void atomic_flow(state& task, int task_index);
    void enqueue_group_locked(state& task);
    void complete_group_locked(state& task);
};
#endif