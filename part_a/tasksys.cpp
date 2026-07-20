#include "tasksys.h"

#include <bits/stdc++.h>
#include <memory>
using namespace std;
IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */
///This code is for Asynchronous -->> design 2 // simple design

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::
TaskSystemParallelThreadPoolSleeping(int num_threads)
    : ITaskSystem(num_threads),
      num_threads_(num_threads),
      shutdown(false),
      curr(0),
      demanded(0),
      completed(0) {

    workers.reserve(num_threads_);

    for (int i = 0; i < num_threads_; ++i) {
        workers.push_back(
            std::thread(
                &TaskSystemParallelThreadPoolSleeping::execute,
                this,
                i
            )
        );
    }
}

TaskSystemParallelThreadPoolSleeping::
~TaskSystemParallelThreadPoolSleeping() {
    sync();

    {
        std::lock_guard<std::mutex> lock(mtx_process);
        shutdown = true;
    }

    cv_work.notify_all();

    for (size_t i = 0; i < workers.size(); ++i) {
        if (workers[i].joinable()) {
            workers[i].join();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(
    IRunnable* runnable,
    int num_total_tasks
) {
    runAsyncWithDeps(
        runnable,
        num_total_tasks,
        std::vector<TaskID>()
    );

    sync();
}

void TaskSystemParallelThreadPoolSleeping::enqueue_group_locked(
    state& task
) {
    for (int i = 0; i < task.total_tasks; ++i) {
        ready_queue.push(
            WorkItem(
                &task,
                i
            )
        );
    }
}

void TaskSystemParallelThreadPoolSleeping::complete_group_locked(
    state& task
) {
    if (ready[task.taskID]) {
        return;
    }

    ready[task.taskID] = true;
    ++completed;

    for (size_t i = 0; i < task.dependents.size(); ++i) {
        TaskID child_id = task.dependents[i];
        state& child = *states[child_id];

        --child.remaining_deps;

        if (child.remaining_deps == 0) {
            if (child.total_tasks == 0) {
                complete_group_locked(child);
            } else {
                enqueue_group_locked(child);
            }
        }
    }

    task.dependents.clear();
}

void TaskSystemParallelThreadPoolSleeping::execute(
    int threadID
) {
    (void)threadID;

    while (true) {
        WorkItem work;

        {
            std::unique_lock<std::mutex> lock(mtx_process);

            cv_work.wait(
                lock,
                [this]() {
                    return shutdown || !ready_queue.empty();
                }
            );

            if (shutdown && ready_queue.empty()) {
                return;
            }

            work = ready_queue.front();
            ready_queue.pop();
        }

        atomic_flow(
            *work.group,
            work.task_index
        );
    }
}

void TaskSystemParallelThreadPoolSleeping::atomic_flow(
    state& task,
    int task_index
) {
    task.runner->runTask(
        task_index,
        task.total_tasks
    );

    bool finished_group = false;

    {
        std::lock_guard<std::mutex> lock(mtx_process);

        ++task.finished_subtasks;

        if (task.finished_subtasks == task.total_tasks) {
            complete_group_locked(task);
            finished_group = true;
        }
    }

    if (finished_group) {
        cv_work.notify_all();
        cv_sync.notify_all();
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(
    IRunnable* runnable,
    int num_total_tasks,
    const std::vector<TaskID>& deps
) {
    TaskID new_id;
    bool notify_workers = false;
    bool notify_sync = false;

    {
        std::lock_guard<std::mutex> lock(mtx_process);

        new_id = curr;
        ++curr;
        ++demanded;

        states.push_back(
            std::unique_ptr<state>(
                new state(
                    new_id,
                    num_total_tasks,
                    runnable
                )
            )
        );

        ready.push_back(false);

        state& new_task = *states[new_id];

        std::unordered_set<TaskID> unique_deps;

        for (size_t i = 0; i < deps.size(); ++i) {
            unique_deps.insert(deps[i]);
        }

        for (
            std::unordered_set<TaskID>::const_iterator it =
                unique_deps.begin();
            it != unique_deps.end();
            ++it
        ) {
            TaskID parent_id = *it;

            if (ready[parent_id]) {
                continue;
            }

            ++new_task.remaining_deps;

            states[parent_id]->dependents.push_back(
                new_id
            );
        }

        if (new_task.remaining_deps == 0) {
            if (new_task.total_tasks == 0) {
                complete_group_locked(new_task);
                notify_sync = true;
                notify_workers = true;
            } else {
                enqueue_group_locked(new_task);
                notify_workers = true;
            }
        }
    }

    if (notify_workers) {
        cv_work.notify_all();
    }

    if (notify_sync) {
        cv_sync.notify_all();
    }

    return new_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> lock(mtx_process);

    int target = demanded;

    cv_sync.wait(
        lock,
        [this, target]() {
            return completed >= target;
        }
    );
}