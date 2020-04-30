"""Example of distributing real-time messages via Apache Arrow Plasma store.

This module demonstrates usage of Apache Arrow Plasma store to distribute
real-time messages to a given number of consumers. This approach TODO:
"""

from multiprocessing import Pool
import random
import subprocess
import time

import numpy as np
import pyarrow as pa
import pyarrow.plasma as plasma


NUM_CONSUMERS = 10
PLASMA_STORE_LOCATION = '/tmp/store'
PLASMA_STORE_SIZE = 1 * 10 ** 9  # 1 GB
PLASMA_OBJID_SIZE = 20
NUM_COLS = 4
NUM_ROWS = 10000
BATCH_MAX_ROWS = 40
CONSUMER_TIMEOUT_MS = 1000


def producer(data):
    """Produces messages and ...

    TODO:

    Args:
        data: An open Bigtable Table instance.

    Returns:
        A list
    """

    client = plasma.connect(PLASMA_STORE_LOCATION)

    row_num = 0
    batch_num = 0
    while row_num < len(data):
        k = random.randint(1, BATCH_MAX_ROWS)
        rows = data[row_num: row_num+k]

        object_id = plasma.ObjectID((batch_num).to_bytes(
            PLASMA_OBJID_SIZE, byteorder="big"))

        print("Producer storing object id {}".format(object_id))

        tensor = pa.Tensor.from_numpy(rows)
        data_size = pa.get_tensor_size(tensor)
        buf = client.create(object_id, data_size)

        stream = pa.FixedSizeBufferWriter(buf)
        pa.write_tensor(tensor, stream)
        client.seal(object_id)

        row_num += k
        batch_num += 1

    print("Producer final sum: {}".format(sum(data)))
    return sum(data)


def consumer(cid):
    """Consums messages.

    TODO:

    Args:
        cid: Consumer id

    Returns:
        A list
    """

    client = plasma.connect(PLASMA_STORE_LOCATION)

    batch_num = 0
    res = np.zeros(4)
    while True:
        object_id = plasma.ObjectID((batch_num).to_bytes(
            PLASMA_OBJID_SIZE, byteorder="big"))
        print("Consumer {} retrieving object id: {}".format(cid, object_id))
        [buf2] = client.get_buffers([object_id], CONSUMER_TIMEOUT_MS)
        if not buf2:
            print('Consumer {} final sum: {}'.format(cid, res))
            break

        reader = pa.BufferReader(buf2)
        tensor2 = pa.read_tensor(reader)
        array = tensor2.to_numpy()

        print(array)
        res += sum(array)

        batch_num += 1

    return res


def main():
    """TODO:

    """

    # Start the plasma store.
    plasma_store = subprocess.Popen(['plasma_store',
                                     '-s', PLASMA_STORE_LOCATION,
                                     '-m', str(PLASMA_STORE_SIZE)])

    time.sleep(1)
    assert plasma_store.poll() is None  # Make sure the plasma store is running

    num_processes = NUM_CONSUMERS + 1
    pool = Pool(processes=num_processes)

    data = np.random.randn(NUM_ROWS, NUM_COLS)

    result_producer = pool.map_async(producer, [data])
    result_consumer = pool.map_async(consumer, range(NUM_CONSUMERS))

    producer_ret = result_producer.get()
    consumer_ret = result_consumer.get()

    print(producer_ret)
    print(consumer_ret)

    plasma_store.kill()


if __name__ == '__main__':
    main()
