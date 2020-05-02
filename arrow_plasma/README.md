Distribution of real-time messages using Apache Arrow Plasma store (shared memory).

This module demonstrates how to efficiently distribute real-time messages via shared memory using
the Apache Arrow Plasma store.

The messages are read and processed (concurrently) by a number of consumer processes. The producer
process is not affected by consumer(s) performance. Both the producer and the consumer processes
utilize the same logic for generating object ids based on batch numbers.