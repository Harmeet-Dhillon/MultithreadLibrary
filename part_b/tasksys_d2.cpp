#include "tasksys.h"
#include <memory>
#include <thread>
#include <mutex>
#include <iostream>
#include <bits/stdc++.h>
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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
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

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads),num_threads_(num_threads),counter(0) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}
/// @brief controlled_flow is a static assignment
/// @param start 
/// @param num_threads 
/// @param num_total_tasks 
/// @param runnable 
void TaskSystemParallelSpawn::controlled_flow(int start,int num_threads,int num_total_tasks,IRunnable* runnable){
    ///this is static approach
   
    /* this is one of grouping tasks
    int groups=(num_total_tasks+num_threads-1)/num_threads;
    for(int g=0;g<groups;g++){
        int idx=num_threads*g+start;
        if(idx>num_total_tasks-1)continue;

        // mtx.lock();
        runnable->runTask(idx,num_total_tasks);
       

    }*/
    for(int idx=start;idx<num_total_tasks;idx+=num_threads){
        runnable->runTask(idx,num_total_tasks);
    }

    return ;
}

void TaskSystemParallelSpawn::atomic_flow(int num_total_tasks,IRunnable* runnable){
    while(true){
        int curr=counter.fetch_add(1,std::memory_order_relaxed);
        if(curr>=num_total_tasks)break;
        runnable->runTask(curr,num_total_tasks);

    }
    return;
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    

    ////we already know number of threads  
    
    ///ok I choose it one by one

    std::vector<std::thread> workers(num_threads_);
    counter=0;
    //=================================
    // this is static assignment
    //=================================
    
    // for(int i=1;i<num_threads_;i++){
    //     workers[i]=std::thread(&TaskSystemParallelSpawn::controlled_flow,this,i,num_threads_,num_total_tasks,runnable);
    // }
    // controlled_flow(0,num_threads_,num_total_tasks,runnable);
    // for(int i=1;i<num_threads_;i++){
    //     workers[i].join();
    // }
    //=================================
    // this is dynamic assignment
    //=================================
    
    for(int i=1;i<num_threads_;i++){
        workers[i]=std::thread(&TaskSystemParallelSpawn::atomic_flow,this,num_total_tasks,runnable);
    }
    atomic_flow(num_total_tasks,runnable);
    for(int i=1;i<num_threads_;i++){
        workers[i].join();
    }
    return ;    
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads),counter(0),total_tasks(0),num_threads_(num_threads-1),runnable_(nullptr),done(false),first(true),work_available(false) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    /// here spool threads at the time of construction
    workers=new std::thread[num_threads_];
    launchpad = new std::atomic<bool>[num_threads_];
    for (int i = 0; i < num_threads_; ++i) {
        workers[i] = std::thread(
            &TaskSystemParallelThreadPoolSpinning::execute,
            this,
            i
        );}
    
}
///this is the main picture

void TaskSystemParallelThreadPoolSpinning::atomic_flow(int num_total_tasks,int thread_id){
    ///this is static approach
    while(true){
            int curr=counter.fetch_add(1,memory_order_relaxed);
            if(curr>=total_tasks){
               break; /// this thread has reached
            }
            runnable_->runTask(curr,total_tasks);
  
    }
    // mtx.lock();
    // std::cout<<thread_id<<endl;
    // mtx.unlock();
    reach.fetch_add(1,memory_order_relaxed);


    return ;
}
 void TaskSystemParallelThreadPoolSpinning::execute(int thread_id){

    while(true){
            if(done)break;
            // std::unique_lock<std::mutex>lock(mtx);
            // cv.wait(lock,[&]{return work_available || done;});
            // lock.unlock();
            bool already_launched=launchpad[thread_id].exchange(true,memory_order_acquire);
            if(!already_launched){
                atomic_flow(total_tasks,thread_id);
            }
    }
    return ;
    //// 
    //main thread should gather cv_  clears signal for all -- all threadids
    //// first id then send message, to run 
 }

/// @brief this is doubtful
TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    done=true;
    for(int i=0;i<num_threads_;i++){
        if (workers[i].joinable()) {
            workers[i].join();
        }
    }
    delete[] workers;
}

void TaskSystemParallelThreadPoolSpinning::run(
    IRunnable* runnable,
    int num_total_tasks)
{
    if (num_total_tasks <= 0) {
        return;
    }

    // Prepare all shared launch information first.
    runnable_ = runnable;
    total_tasks = num_total_tasks;

    counter.store(0, std::memory_order_relaxed);
    reach.store(0, std::memory_order_relaxed);

    // Publish new work to each worker.
    for (int i = 0; i < num_threads_; ++i) {
        launchpad[i].store(
            false,
            std::memory_order_release
        );
    }
    // work_available=true;
    // cv.notify_all();

    // Calling thread also participates.
    atomic_flow(total_tasks,num_threads_);

    while (reach.load(std::memory_order_acquire)
           < num_threads_ + 1) {
        // Spin.
    }
    // work_available=false;
    // cv.notify_all();
    return ;
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */



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
        /// *** NOTE: this scope is ended before going to atomic function
        /// it just let 1 thread hold mutex at one time and then just take the work & workindex to process
        /// this way it is similar to design one of atomic approach 
        /// cv_wait gets noticed -when new task or new enqueue 
        ///
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