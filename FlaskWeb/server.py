from flask import Flask, request, redirect
from kafka import KafkaProducer
from datetime import datetime
import random
 
app = Flask(__name__)

# def my_partitioner(key, all_partitions, available_partitions):
#     return all_partitions[int(key)]

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: str(x).encode('utf-8'),
)

def repeat_pub_msg(count, key):
    now = datetime.now()
    msg = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "_BUY_" + str(count)

    msg = msg.encode('utf-8')
    key = str(key).encode('utf-8')

    producer.send('ccxt_order_buy', key=key, value=msg)
    producer.flush()

 
def template():
    return f'''<!doctype html>
    <html>
        <body>
            <h1><a href="/">WEB</a></h1>
            <h3> Hello! Flask! <h3>
            <ol>
                <li><a href="/pub/">pub</a></li>
                <li><a href="/repeatPub/">repeat pub</a></li>
            </ol>
        </body>
    </html>
    '''
 
@app.route('/')
def index():
    return template()
 
 
# @app.route('/pup/', methods=['POST'])
# def pub_msg():
#     now = datetime.now()
#     msg = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "_BUY_1"
#     print('msg :::' + str(msg))
#     producer2.send('ccxt_order_buy', value=str(msg))
#     producer2.flush() 

    
@app.route('/repeatPub/', methods=['GET', 'POST'])
def repeat_pub():
    try:
        start_time = datetime.now()
        repeat_count = 0
        while True:
            repeat_count += 1
            key = random.randint(1, 100000)
            repeat_pub_msg(repeat_count, key)
            if repeat_count == 10000:
                break
    except KeyboardInterrupt:
        return index()
    finally:
        end_time = datetime.now()
        execution_time = end_time - start_time
        execution_seconds = execution_time.total_seconds()
        print("Execution Time:", execution_seconds, "seconds")

    return '', 204   # 204 No Content 상태 코드로 응답하여 페이지 이동 없이 요청만 처리
    
app.run(debug=True, host='0.0.0.0', port=9900) 