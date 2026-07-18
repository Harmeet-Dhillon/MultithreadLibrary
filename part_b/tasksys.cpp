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


TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads),num_threads_(num_threads),done(false),workers(num_threads-1),curr(0),demanded(0),completed(0),global_counter(0),sizeP_(0){
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    taskpusher=std::thread(&TaskSystemParallelThreadPoolSleeping::waiting,this);
    for(int i=0;i<num_threads_-1;i++){
        workers[i]=std::thread(&TaskSystemParallelThreadPoolSleeping::execute,this,i);
    }


}
/// @brief  we will design two phases - waiting & processing 
/// First face is slow taskpusher which handles waiting and fast processing unit
/// second face will deploy fast pusher
/// two seperate tests will be done on saving state as object and saving state as pointer
TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    done=true;
    taskpusher.join();
    for(int i=0;i<num_threads_-1;i++){
        workers[i].join();
    }
    //
}


void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}
/// @brief  it will be really intresting to make this one parallel in two or more
// but overaall it should follow same topology -- like one counter & processor
/// here each lock but outer one will already block all of them -this one has scope of improvement
void TaskSystemParallelThreadPoolSleeping::waiting(){
    while(!done){
            
            vector<int>temp;
           
            // cout<<store.size()<<" size in watiing"<<endl;
            mtx_store.lock();
            for(auto&[k,v]:store){
                ///have to check mtx can be used as we know that will be different memory but now keep it
                 
                if(v.empty()){
                    mtx_process.lock();
                    mtx_inwait.lock();
                    
                    toprocess.emplace_back(inwait[k]);
                    // cout<< "at lease here "<<k<<" i have successfully added"<<endl;
                    temp.push_back(k);
                    
                    mtx_inwait.unlock();
                    mtx_process.unlock();
                    
                }
                else{
                    vector<int> temp2;
                    for(auto e:v){
                        
                        mtx_ready.lock();
                        
                            if(ready[e]){temp2.push_back(e);}//cout<< "at lease here 345 "<<e<<endl;}
                        
                        mtx_ready.unlock();

                    }
                    for(auto a:temp2){
                        v.erase(a);
                    }
                }

            }
            mtx_store.unlock();
            
            
            for(auto a:temp){
                mtx_store.lock();
                store.erase(a);
                mtx_store.unlock();
            }
           
        }
    
    return ;
}
/// @brief  this is the most interesting and important part
/// we have to figure out that how we will work such that the global counter doesnot increase twice
/// also have to make ready true here for that state
////  guarding the last element would help us -as after that it will break 

void TaskSystemParallelThreadPoolSleeping::atomic_flow(state& s){
        while(true){
            int curr=s.index.fetch_add(1,memory_order_relaxed);
            // cout<<s.taskID<<endl;
            if(curr>=s.total_tasks)break;
            s.runner->runTask(curr,s.total_tasks);
            int currscore=s.score.fetch_add(1,memory_order_relaxed);
            if(currscore==s.total_tasks-1){
                mtx_ready.lock();
                ready[s.taskID]=true;
                global_counter.fetch_add(1,memory_order_relaxed);
                int rec=completed.fetch_add(1,memory_order_relaxed);
                int dem=demanded.load(memory_order_acquire);
                if(rec==dem)done=true;
                // cout<<s.taskID<<" task num is complelte"<<endl;
                mtx_ready.unlock();
            }
            ////this score tells if done equal
        }
        return ;
}

void TaskSystemParallelThreadPoolSleeping::execute(int threadID){
        while(!done){
            mtx_process.lock();
            int sizeP=toprocess.size();
            mtx_process.unlock();
            int gidx=global_counter.load(memory_order_acquire);
             
            if(gidx<sizeP){
                atomic_flow(toprocess[gidx]);

            }
        }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //
    unordered_set<int> dset{};
    for(auto a:deps){
        dset.insert(a);
    }
    
    
    mtx_store.lock();
    mtx_ready.lock();
    mtx_inwait.lock();

    ready.push_back(false);
    store[curr]=dset;
    demanded.fetch_add(1,memory_order_relaxed);
    // cout<<curr<<"my id is here"<<endl;
    // cout<<store.size()<< "store size "<<endl;
    inwait.emplace_back(state(curr,num_total_tasks,runnable));
    mtx_ready.unlock();
    
    mtx_inwait.unlock();
    mtx_store.unlock();
    curr++;
   
    return curr-1;




    // for (int i = 0; i < num_total_tasks; i++) {
    //     runnable->runTask(i, num_total_tasks);
    // }
    
    //  return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    // cout<<demanded<<" "<<completed<<" start story"<<endl;

    while(completed.load(std::memory_order_acquire) < demanded.load(std::memory_order_acquire)){
        //cout<<demanded<<" "<<completed<<" end story"<<endl;
        //loop around
    }
    // 
     
    return;
}
