---
author:
- |
    Yuanxu Wu
title: |
    Optimization of Basic Raft Consensus Algorithm
    Project Proposal
---

Motivation 
==========

Consensus is a fundamental problem in any fault-tolerant distributed
systems and it plays a key role in building reliable large-scale
software systems. One very important Consensus algorithm is Raft. In the
basic raft algorithm, there are actually a lot of aspects which can be
improved to let raft to be more efficient.

Goal and Objectives 
===================

This paper’s goal is to understand and analyze ideas of optimizing the
efficiency of Raft Algorithm, which include using batching, pipelining,
appending logs in parallelism and asynchronous apply in order to get
higher throughput, lower latency, and higher performing speed.

Approach 
========

First of all the basic Raft consensus algorithm is implemented based on
the idea from the paper [@b1]. In this project, I’m going to analyze
four improvements of Raft, based on section 10.2 in the paper [@b2]:

-   Sending the request to nodes by batch instead of one by one.\
    \
    Batch processing is the execution of a series of jobs in a program
    on a computer without manual intervention. In this project, the
    batch is a set of inputs, rather than a single input.

-   Applying pipeline to forward requests.\
    \
    The pipeline means a chain of data-processing stages. In this
    project, it means leader sending appendEnties RPCs in a pipeline.

-   Parallelly executing.\
    \
    Parallelly executing means executing parallelly between appending
    logs and forwarding requests.

-   Asynchronous apply to the state machine.\
    \
    Asynchronous apply to state machine means after a log got committed,
    we can use another thread to apply this log asynchronously.

The ideal improvement of implementing batching is to get higher
throughput. The ideal improvement of implementing pipeline, parallelly
executing and asynchronous applying to the state machine is to get lower
latency.

Validation 
==========

After implementing the optimized Raft, I’m going to test the runtime
over several numbers of requests from clients between basic Raft and the
optimized Raft. In addition, because the runtime of this optimized Raft
could be influenced by the batch size of requests, I will also try
different sizes of batches to find the optimum one.\
\
There are some risks on implementing parallel executing and asynchronous
apply to the state machine that could jeopardize the consensus of the
Raft algorithm. So after implementing the optimized Raft I will test
some corner cases and prove the optimized Raft is still consensus on
asynchronous distributed systems.

[00]{} Ongaro, Diego, and John K. Ousterhout. “In search of an
understandable consensus algorithm.” USENIX Annual Technical Conference.
2014. Ongaro, Diego. Consensus: Bridging theory and practice. Diss.
Stanford University, 2014.

