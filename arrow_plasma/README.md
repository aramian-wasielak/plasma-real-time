Using the Plasma In-Memory Object Store for Real-Time Communication
-------------------------------------------------------------------

This code demonstrates how to efficiently distribute real-time messages using [the Apache Arrow Plasma][1] in-memory object store.

Please note that **the Plasma API is under active development and it is not stable**.

This code creates a random dataset that is broken into a large number of batches and distributed by the producer process to a number of consumer processes. The big advantage of using the in-memory object store is the fact that messages are read and processed (concurrently) by a number of consumer processes. The producer
process is not affected by consumer(s) performance. If 

Both the producer and the consumer processes
utilize the same logic for generating consecutive batch numbers into Plasma's object ids.

Running code:
```
$ python src/arrow_plasma.py
```

[1]: https://arrow.apache.org/docs/python/plasma.html
