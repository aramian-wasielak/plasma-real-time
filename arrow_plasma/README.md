# Real-Time Messaging with In-Memory Plasma Store

A High-Performance Shared-Memory Object Store
Motivating Plasma

This code demonstrates how to efficiently distribute real-time messages using [the Apache Arrow Plasma][1] in-memory object store.

Please note that **the Plasma API is under active development and it is currently (as of 2020/05) not stable**.

The idea is for a producer and consumer processes to utilize the same logic to generate the same series of message ids (Plasma object ids).

The script starts up an instance of Plasma store, one producer and multiple consumer processes. The producer process generates a random dataset that is broken up into batches and saved onto the Plasma store. At the same time (concurrently), consumer processes are waiting (blocking call) for new messages and/or process incoming messages.

The advantage of this approach is that the producer process is not affected by the count of consumer processes and their speed.

## Getting Started
```
$ python src/arrow_plasma.py
```

### Prerequisites

## Additional ideas for improvement
* Create an example where the producer process is implemented in C++ (Cython)
* Create an example where consumer processes to something more interesting than just to read messages:
  - One idea is to incrementally train a model (using fit_partial in sklearn)

## Acknowledgments

* https://arrow.apache.org/docs/python/plasma.html
* 
