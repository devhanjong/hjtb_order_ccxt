from kafka import KafkaProducer
from datetime import datetime

def my_partitioner(key, all_partitions, available_partitions):
    return all_partitions[int(key)]

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: str(x).encode('utf-8'),
    partitioner=my_partitioner
)

def pub_msg(count, keys):
    now = datetime.now()
    msg = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "_BUY_" + str(count)
    print('msg :::' + str(msg))
    producer.send('ccxt_order_buy', key=str(keys), value=str(msg))
    producer.flush()

try:
    start_time = datetime.now()
    repeat_count = 0
    while True:
        repeat_count += 1
        key = repeat_count % 20
        pub_msg(repeat_count, key)
        print("repeat count:", repeat_count)
        print("key:", key)
        if repeat_count == 10000:
            print("::: Exit script")
            break
except KeyboardInterrupt:
    print("::: Exit script")
finally:
    end_time = datetime.now()
    execution_time = end_time - start_time
    execution_seconds = execution_time.total_seconds()
    print("Execution Time:", execution_seconds, "seconds")

producer.close()