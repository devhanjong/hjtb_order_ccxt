# -*- coding: utf-8 -*-
from kafka import KafkaConsumer
from kafka import KafkaProducer
from datetime import datetime

consumer = KafkaConsumer(
    'ccxt_order_buy',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='ccxt_order_buy_group',
    value_deserializer=lambda x: x.decode('utf-8')  # value를 디코딩하는 설정 추가
)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    max_in_flight_requests_per_connection=1,
    value_serializer=lambda x: str(x).encode('utf-8')
)

def process_message(message, key):
    try:
        message = str(message)  # str으로 변환하여 b'' 없이 출력

        # message_time, order_type, order_uuid, vasp_simple_name, vasp_symbol, my_symbol, amount, mb_idx, apikey = message.split('_')
        message_time, order_type, count = message.split('_')
        now = datetime.now()
        message_time = datetime.strptime(message_time, '%Y-%m-%d %H:%M:%S.%f')
        time_diff = (now - message_time).total_seconds()

        print('now ::: ' + str(now))
        print('message_time ::: ' + str(message_time))
        print('time_diff ::: ' + str(time_diff))
        print('key ::: ' + str(key))

        if time_diff <= 0.5:
            return_value = 'success'
        else:
            return_value = 'failure'

        producer.send('ccxt_order_buy_return', key=key, value=str(return_value))
        producer.flush()
        print(str(return_value))
    except:
        return_value = 'failure'
        producer.send('ccxt_order_buy_return', key=key, value=str(return_value))
        producer.flush()
        print(str(return_value))

for message in consumer:
    value = str(message.value)
    if value.startswith("b'"):
        value = value[2:-1]
    # print(value)
    process_message(value, message.key)
    print('--------------------------------------------')

producer.close()