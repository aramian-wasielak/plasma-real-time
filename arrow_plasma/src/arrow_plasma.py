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
import pyarrow as pa
import pyarrow.plasma as plasma

PLASMA_STORE_LOCATION = '/tmp/store'
PLASMA_STORE_SIZE_BYTES = 1 * 10 ** 9  # 1 GB
PLASMA_OBJID_SIZE_BYTES = 20

# Producer related values
NUM_COLS = 4
NUM_ROWS = 1000
RANDOM_SEED = 1234
BATCH_MAX_ROWS = 40

# Consumer related values
NUM_CONSUMERS = 10
CONSUMER_TIMEOUT_MS = 1000


def gen_object_id(batch_num):
    """Generates an object id used by the plasma store for a given batch number.

    Args:
        batch_num (int): The batch number.

    Returns:
        The plasma object id.
    """
    return plasma.ObjectID(batch_num.to_bytes(PLASMA_OBJID_SIZE_BYTES, byteorder="big"))


def calc_data_checksum(checksum, data):
    """Calculates the data checksum and adds it to the running checksum.

    Args:
        checksum (int): The running checksum.
        data (numpy): The data for which checksum is calculated.

    Returns:
        int: The checksum value.
    """
    return checksum + sum(sum(data))


def gen_random_data(num_rows, num_cols, seed):
    """Generate random data.

    Args:
        num_rows (int): Number of rows.
        num_cols (int): Number of columns.
        seed (int): Seed for the random generator.

    Returns:
        numpy: Generated random data.
    """
    np.random.seed(seed)
    return np.random.rand(num_rows, num_cols)


def producer(data, batch_max_rows=BATCH_MAX_ROWS):
    """Saves the input data as a set of batches (of random size) into the plasma store.

    Args:
        data (numpy): The input data saved into the plasma store.
        batch_max_rows (int): The maximum size allowed for each batch.

    Returns:
        float: The checksum value for all the input data.
    """
    client = plasma.connect(PLASMA_STORE_LOCATION)

    row_num = 0
    batch_num = 0
    while row_num < len(data):
        k = random.randint(1, batch_max_rows)
        rows = data[row_num: row_num + k]
        object_id = gen_object_id(batch_num)

        logging.info("Producer: storing batch number '%i' as object id '%s'", batch_num, object_id)

        tensor = pa.Tensor.from_numpy(rows)
        data_size = pa.get_tensor_size(tensor)
        buf = client.create(object_id, data_size)

        stream = pa.FixedSizeBufferWriter(buf)
        pa.write_tensor(tensor, stream)
        client.seal(object_id)

        row_num += k
        batch_num += 1

    return calc_data_checksum(0, data)


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

    batch_num = 0
    checksum = 0
    while True:
        object_id = gen_object_id(batch_num)
        logging.debug("Consumer %i: retrieving object id '%s'", cid, object_id)
        [buf2] = client.get_buffers([object_id], timeout)
        if not buf2:
            # This means that we hit the timeout condition. Normally we would check if we should
            # continue to poll the plasma store for more data, but in our case we will just return.
            logging.info("Consumer %i: data checksum: %i", cid, checksum)
            break

        reader = pa.BufferReader(buf2)
        tensor = pa.read_tensor(reader).to_numpy()
        logging.debug(tensor)
        checksum = calc_data_checksum(checksum, tensor)
        batch_num += 1

    return checksum


def main():
    """Sets up the plasma store and simulates writing (by a producer process) and readying
    (by consumer processes) of real-time messages.
    """
    logging.basicConfig(level=logging.DEBUG)

    # Start the plasma store.
    plasma_store = subprocess.Popen(['plasma_store',
                                     '-s', PLASMA_STORE_LOCATION,
                                     '-m', str(PLASMA_STORE_SIZE_BYTES)])

    # Wait for the plasma store to come up and make sure it is running.
    time.sleep(1)
    assert plasma_store.poll() is None

    num_processes = NUM_CONSUMERS + 1
    pool = Pool(processes=num_processes)

    data = gen_random_data(NUM_COLS, NUM_ROWS, RANDOM_SEED)
    result_producer = pool.map_async(producer, [data])
    result_consumer = pool.map_async(consumer, range(NUM_CONSUMERS))

    producer_check = result_producer.get()
    consumer_check = result_consumer.get()

    assert len(np.unique(consumer_check)) == 1 and consumer_check[0] == producer_check
    plasma_store.kill()


if __name__ == '__main__':
    main()
