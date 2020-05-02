Using the Plasma In-Memory Object Store for Real-Time Communication
-------------------------------------------------------------------

This module demonstrates how to efficiently distribute real-time messages using [the Apache Arrow Plasma][1] in-memory object store.

Please note that **the Plasma is still under active development and the API is not stable**.

The messages are read and processed (concurrently) by a number of consumer processes. The producer
process is not affected by consumer(s) performance. Both the producer and the consumer processes
utilize the same logic for generating object ids based on batch numbers.

Running code:
```
$ python src/arrow_plasma.py
```

[1]: https://arrow.apache.org/docs/python/plasma.html
