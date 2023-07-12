from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: str(x).encode('utf-8')
)

def pub_msg(count):
    now = datetime.now()
    msg = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "_BUY_" + str(count)

    print('msg :::' + str(msg))

    producer.send('ccxt_order_buy', value=str(msg))
    producer.flush()


if __name__ == '__main__':
    while True:
        count = 0
        user_input = input("send? (y/n): ")
        if user_input.lower() == 'y':
            count += 1
            pub_msg(count)
        else:
            print("::: Exit script")
            break

producer.close()