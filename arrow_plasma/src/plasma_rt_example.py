"""Distribution of real-time messages using Apache Arrow Plasma store (shared memory).

This module demonstrates how to efficiently distribute real-time messages via shared memory using
the Apache Arrow Plasma store.

The messages are read and processed (concurrently) by a number of consumer processes. The producer
process is not affected by consumer(s) performance. Both the producer and the consumer processes
utilize the same logic for generating object ids based on batch numbers.
"""

import logging
from multiprocessing import Pool
import random
import subprocess
import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.plasma as plasma
from sklearn.linear_model import SGDRegressor

PLASMA_STORE_LOCATION = "/tmp/store"
PLASMA_STORE_SIZE_BYTES = 1 * 10 ** 9  # 1 GB
PLASMA_OBJID_SIZE_BYTES = 20

# Producer related values
NUM_ROWS = 10000
RANDOM_SEED = 1234
BATCH_MAX_ROWS = 40

# Consumer related values
NUM_CONSUMERS = 3
CONSUMER_TIMEOUT_MS = 1000


def get_object_id(batch_num):
    """Generates an object id used by the plasma store for a given batch number.

    Args:
        batch_num (int): The batch number.

    Returns:
        The plasma object id.
    """
    return plasma.ObjectID(batch_num.to_bytes(PLASMA_OBJID_SIZE_BYTES, byteorder="big"))


def put_df(client, object_num, df):
    """Saves a data frame into the plasma store.

    Code adjusted from:
    https://github.com/apache/arrow/blob/master/python/examples/plasma/sorting/sort_df.py

    Args:
        client: Plasma store connection.
        object_num (int): Object number to be used to generate a corresponding object id.
        df (pandas): Data frame to be saved.
    """
    record_batch = pa.RecordBatch.from_pandas(df)

    # Get size of record batch and schema
    mock_sink = pa.MockOutputStream()
    stream_writer = pa.RecordBatchStreamWriter(mock_sink, record_batch.schema)
    stream_writer.write_batch(record_batch)
    data_size = mock_sink.size()

    # Generate an ID and allocate a buffer in the object store for the
    # serialized DataFrame
    object_id = get_object_id(object_num)
    buf = client.create(object_id, data_size)

    # Write the serialized DataFrame to the object store
    sink = pa.FixedSizeBufferWriter(buf)
    stream_writer = pa.RecordBatchStreamWriter(sink, record_batch.schema)
    stream_writer.write_batch(record_batch)

    # Seal the object
    client.seal(object_id)


def get_data_checksum(checksum, data):
    """Calculates the data checksum and adds it to the running checksum.

    Args:
        checksum (int): The running checksum.
        data (numpy): The data for which checksum is calculated.

    Returns:
        int: The checksum value.
    """
    return checksum + np.sum(np.sum(data))


def get_random_data(num_points, seed):
    """Generate linear-looking data.

    Args:
        num_points (int): Number of points to be generated.
        seed (int): Seed for the random generator.

    Returns:
        numpy: Generated random data.
    """
    np.random.seed(seed)

    a_0 = 4
    a_1 = 3
    logging.info(
        "Simulating linear-looking data these coefficients (plus some error): a = [%f, %f]",
        a_0, a_1)

    x = 10 * np.random.rand(num_points)
    y = a_0 + a_1 * x + np.random.randn(num_points)
    df = pd.DataFrame(data={'x': x, 'y': y})
    return df


def producer(data, batch_max_rows=BATCH_MAX_ROWS):
    """Saves the input data as a set of batches (of random size) into the plasma store.

    Args:
        data (numpy): The input data saved into the plasma store.
        batch_max_rows (int): The maximum size allowed for each batch.

    Returns:
        float: The checksum value for all the input data.
    """
    logging.info("Producer: connecting to the plasma store")
    client = plasma.connect(PLASMA_STORE_LOCATION)

    logging.info("Producer: starting to load data (%i rows) onto the plasma store", len(data))
    row_num = 0
    batch_num = 0
    checksum = 0.0
    while row_num < len(data):
        k = random.randint(1, batch_max_rows)
        rows = data[row_num: row_num + k]
        checksum = get_data_checksum(checksum, rows)

        logging.debug("Producer: storing batch number: %i", batch_num)
        put_df(client, batch_num, rows)

        row_num += k
        batch_num += 1

    client.disconnect()
    logging.info("Producer: total %i rows distributed (checksum: %f)", len(data), checksum)
    return checksum


def consumer(cid, timeout=CONSUMER_TIMEOUT_MS):
    """Reads and processes data batches from the plasma store (shared memory). This function blocks
     (with a specified timeout) if a batch number has not yet been saved into the plasma store.

    Args:
        cid (int): Unique consumer id.
        timeout (int): How long we should wait for new messages to be saved into the plasma store.

    Returns:
        float: The checksum value calculated for all the read and processed data.
    """
    logging.info("Consumer %i: connecting to the plasma store", cid)
    client = plasma.connect(PLASMA_STORE_LOCATION)

    model = SGDRegressor()

    batch_num = 0
    row_cnt = 0
    checksum = 0
    while True:
        object_id = get_object_id(batch_num)
        logging.debug("Consumer %i: retrieving object id '%s'", cid, object_id)

        [data] = client.get_buffers([object_id], timeout)
        if not data:
            # This means we hit the timeout condition. Normally we would check if we should
            # continue to poll the plasma store for more data, but in our case we will just return.
            client.disconnect()
            logging.info("Consumer %i: total %i rows processed (checksum: %f)",
                         cid, row_cnt, checksum)
            logging.info("Consumer %i: calculated linear coefficients: a = [%f, %f]",
                         cid, model.intercept_, model.coef_)
            break

        buffer = pa.BufferReader(data)
        reader = pa.RecordBatchStreamReader(buffer)
        record_batch = reader.read_next_batch()
        df = record_batch.to_pandas()

        logging.debug("Consumer %i: processing data:\n%s", cid, df)

        # Incremental model training
        x = df['x'].to_numpy().reshape(-1, 1)
        y = df['y'].to_numpy()
        model.partial_fit(x, y)

        checksum = get_data_checksum(checksum, df)
        row_cnt += len(df)

        batch_num += 1

    return checksum


def main():
    """Starts up a plasma store instance and runs one producer and multiple consumer processes."""
    logging.basicConfig(level=logging.INFO)

    # Start the plasma store.
    plasma_store = subprocess.Popen(["plasma_store",
                                     "-s", PLASMA_STORE_LOCATION,
                                     "-m", str(PLASMA_STORE_SIZE_BYTES)])

    # Wait for the plasma store to come up and make sure it is running.
    time.sleep(1)
    assert plasma_store.poll() is None

    num_processes = NUM_CONSUMERS + 1
    pool = Pool(processes=num_processes)

    data = get_random_data(NUM_ROWS, RANDOM_SEED)
    producer_result = pool.map_async(producer, [data])
    consumer_result = pool.map_async(consumer, range(NUM_CONSUMERS))

    producer_check = producer_result.get()
    consumer_check = consumer_result.get()

    # Make sure data is identical across all consumers and the producer process.
    assert len(np.unique(consumer_check)) == 1 and consumer_check[0] == producer_check
    plasma_store.kill()


if __name__ == "__main__":
    main()
