from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'ccxt_order_buy_return',                    
    bootstrap_servers=['localhost:9092'], 
    auto_offset_reset='earliest',         
    enable_auto_commit=True,              
    group_id='ccxt_order_buy_return'     
)

for message in consumer:
    print(message.value)