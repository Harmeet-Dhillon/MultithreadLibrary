
# Building A Task Execution Library from the Ground Up #


## Overview ##
This is an implementation of assignment 2 from cs149 to build understanding and compare different designs. While Part A is simple - threads work independently and we just count when do all the threads reach the other end of task . Then we put threads to rest. Spawn & Spin are written in Part B . Sleeping is in Part A. 

## PART B ## 

In part B, first is a system design which work with two parallel high level implementations. In which one takes care of when to put the ready task to processing list and other thread pool solve the problem. It has many drawbacks which are shown after the visual representation of the system design. 

<img width="1536" height="1024" alt="sys1" src="https://github.com/user-attachments/assets/3c781d89-af84-4de0-b047-422fe23948b9" />


## Strengths ##
