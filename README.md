
# Building A Task Execution Library from the Ground Up #


## Overview ##
This is an implementation of assignment 2 from cs149 to build understanding of parallel execution.As this is written as general framework,the design would change depending upon application.There are many strengths and flaws in this design which are listed below. While Part A is simple - threads work independently and we just count when do all the threads reach the other end of task . Then we put threads to rest. Spawn,Spin are implemented for PART A and asynchronous for PART B. Code is written in part_b folder only. 

## PART B ## 

In part B, first is a system design which work with two parallel high level implementations. In which one takes care of when to put the ready task to processing list and other thread pool solve the problem. It has many drawbacks which are shown after the visual representation of the system design. The diagram shown below is for design 1 -->> tasksys_d1.cpp in code.

<img width="1536" height="1024" alt="sys1" src="https://github.com/user-attachments/assets/3c781d89-af84-4de0-b047-422fe23948b9" />


## Strengths ##
1. This system can be easily scaled to do parallel implementation  of waiting thread - Taskpusher.
2. "toprocess" is iterated atomically overall except  adding elements.

## Drawbacks ## 
1. Even when there are many threads in "toprocess" storage- threads iterate task one by one however inside each bulk task implementation is parallel.This approach can be bad choice when we have some large in front of small task and lot other upcomings tasks depend upon small task.
2. Choice of Hashmap is good when there are large number of tasks but may be costly when tasks are fewer due to non-cache locality.
3. When a task become ready,in taskpusher, for each dependent task we check same task again and again.Use of dependency graph can perform better.
4. Too many moving variables at the same time ->> have to manage all of them making the system more complex.
5. No sleep is controlled using Control Variables.
6. Cache performance-> a deque has fixed memory locations for each state, when different cores hit the cache -they upload it casuing cache bouncing.It can be improved using padding of objects

## Desing d2 and d3 ##
These designs are different from design 1. Show 15% performance improvement on an average. It store dependency in parent state and do not use hash maps. It does not have waiting thread and any thread which completes a task would check dependency of other tasks.If any of the child task has dependencies equal to zero - add that into ready queue where it is processed by executors(threads). d2 is completely lock style  and d3 is a trial to make if lockfree. However due to atomic contention and cache bouncing d3 does not show much improvement of d2. 

This is a very general work to build understanding and test how each factor effect performance.However there are few clear winners :
1. Use of Dependency graph inside state of parent saving its children and when parent task removed it will check delete dependency count on children by one.
2. Fewer mtx & atomic variables takes far less effort to execute than a complex design which provide hardly any improvement.
