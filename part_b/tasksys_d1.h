#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <atomic>
#include <mutex>
#include <memory>
#include <thread>
#include <condition_variable>
#include <unordered_map>
#include <unordered_set>
#include <bits/stdc++.h>
/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
using namespace std;
struct  state{
    int taskID;
    int total_tasks;
    IRunnable* runner;
    atomic<int> index;
    atomic<int> score;
    state(int tid,int tt,IRunnable* runn):taskID(tid),total_tasks(tt),runner(runn),index(0),score(0){}
    state(const state& other){
        this->taskID=other.taskID;
        this->total_tasks=other.total_tasks;
        this->runner=other.runner;
        this->index.store(0);
        this->score.store(0);
    }
    state& operator=(const state&other){
        this->taskID=other.taskID;
        this->total_tasks=other.total_tasks;
        this->runner=other.runner;
        this->index.store(0);
        this->score.store(0);
        return *this;
    }
    };
    

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
        void controlled_flow(int start,int num_threads,int num_total_tasks,IRunnable* runnable);
        void atomic_flow(int num_total_tasks,IRunnable* runnable);
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);                      
        void sync();
    private:
        const int num_threads_;
        std::atomic<int> counter;
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
        void atomic_flow(int num_total_tasks,int thread_id);
        void execute(int thread_id);
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
    private:
    std::atomic<int> counter;
    IRunnable* runnable_; // everything in runnable is on stack so no need of deep copy;
    int total_tasks;
    std::thread* workers;
    const int num_threads_;
    /// @brief  do we really need atomics here
    bool done;
    std::atomic<int> reach;
    std::atomic<bool>* launchpad;
    std::condition_variable cv;
    bool first;
    std::mutex mtx;
    bool work_available;
};
/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();

        void run(IRunnable* runnable, int num_total_tasks);
        void waiting();
        void execute(int threadID);
        void atomic_flow(state &s);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
        
        unordered_map<int,unordered_set<int>> store;
        vector<bool> ready; /// will always increase with tasks -like each task would do it false;
        vector<state> inwait;/// in inwait & ready and store , the location would automatically be taskID
        std::deque<state> toprocess;
        std::thread taskpusher;
        int num_threads_;
        atomic<bool> done;
        vector<std::thread>workers;
        int curr;
        std::mutex mtx_ready,mtx_store,mtx_inwait,mtx_process ;
        atomic<int> global_counter;
        atomic<int> sizeP_;

        
        atomic<int> completed;
        atomic<int> demanded;
        
        /// after first problem we find it never reached there


};

#endif
