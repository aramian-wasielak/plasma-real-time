## Real-Time Streaming Example Using Shared Memory (Plasma Store) and Scikit-learn

This code demonstrates how to efficiently distribute and process real-time messages using [the Apache Arrow Plasma][1] in-memory object store. We also show how we can incrementally train an online linear model using [the Scikit-learn's SGDRegressor][2].

The script creates an instance of Plasma store, starts up one producer and multiple consumer processes. 

* Producer process:
  - Breaks up a given dataframe into a set of batches and saves them into the plasma store.
  - The dataframe contains values generated using the following linear model: ```y = a_0 + a_1 * x + error```

* Consumer processes:
  - Poll for new messages (this is a blocking call with a defined timeout)
  - Every new message is processed by incrementally training a linear model with the goal of figuring out coefficients (```a_0, a_1```)

The advantage of this approach is that the producer process is not affected by the count of consumer processes and their speed. It is important that the producer and consumer processes utilize the same logic to generate a series of consecutive message ids.

Please note that **the Plasma API is under active development and it is currently (as of 2020/05) not stable**.

### Getting Started
Install prerequisites:
```
conda env create -f environment.yml 
```
Run the example script:
```
$ python src/plasma_rt_example.py
```

### Potential Future Improvements
* Use a C++ client (https://github.com/apache/arrow/blob/master/cpp/apidoc/tutorials/plasma.md) to produce messages

### References

* https://arrow.apache.org/docs/python/plasma.html
* https://arrow.apache.org/blog/2017/08/08/plasma-in-memory-object-store/
* https://github.com/apache/arrow/blob/master/python/examples/plasma/sorting/sort_df.py
* https://blog.tensorflow.org/2019/08/tensorflow-with-apache-arrow-datasets.html

[1]: https://arrow.apache.org/docs/python/plasma.html
[2]: https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.SGDRegressor.html
