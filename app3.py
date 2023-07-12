import asyncio
from concurrent import futures
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from kafka.errors import KafkaError
from datetime import datetime

async def process_message(producer, message, key):
    try:
        message = str(message)

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

        await producer.send('ccxt_order_buy_return', key=key, value=str(return_value))
        await producer.flush()
        print(str(return_value))
    except:
        return_value = 'failure'
        await producer.send('ccxt_order_buy_return', key=key, value=str(return_value))
        await producer.flush()
        print(str(return_value))

async def consume_messages(producer):
    consumer = AIOKafkaConsumer(
        'ccxt_order_buy',
        bootstrap_servers='localhost:9092',
        group_id='ccxt_order_buy_group',
        value_deserializer=lambda x: x.decode('utf-8'),
        # max_poll_records=500
    )
    await consumer.start()
    try:
        while True:
            # async for message in consumer:
            #     value = str(message.value)
            #     if value.startswith("b'"):
            #         value = value[2:-1]
            #     await asyncio.sleep(1)  # 1초 지연
            #     await process_message(producer, value, message.key)
            #     print('--------------------------------------------')
            messages = await consumer.getmany(max_records=10, timeout_ms=1000000)
            if not messages:
                break
            tasks = []
            for tp, msgs in messages.items():
                for message in msgs:
                    value = str(message.value)
                    if value.startswith("b'"):
                        value = value[2:-1]
                    task = asyncio.ensure_future(process_message(producer, value, message.key))
                    tasks.append(task)
                    print('--------------------------------------------')
            await asyncio.gather(*tasks)  # 병렬로 메시지 처리
            # await asyncio.sleep(1) # 병렬처리 테스트를 위해 루프당 1초 지연
    except KafkaError as e:
        print(f'Kafka Error: {e}')
    finally:
        await consumer.stop()

async def main():
    producer = AIOKafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda x: str(x).encode('utf-8')
    )
    await producer.start()

    await asyncio.gather(consume_messages(producer))

    await producer.stop()

asyncio.run(main())