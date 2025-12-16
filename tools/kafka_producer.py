import json
import time
import random
from datetime import datetime,timedelta
from kafka import KafkaProducer
# 模拟kafka 生产者发数据
def start_producer():
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x:json.dumps(x).encode('utf-8')
    )

    cities = ["Beijing","Shanghai","Guangzhou","Shenzhen","Hangzhou"]

    print(f"》》》开始向kafka推送电商订单数据...")

    while True:
        now = datetime.now()

        # 模拟生成数据
        # 10% 概率生成一条迟到的数据（迟到10分钟，超过预设的5分钟的水位线）
        is_late = random.random() < 0.1

        if is_late:
            event_time = now - timedelta(minutes=random.randint(5,10))
        else:
            event_time = now

        data = {
            "order_id": f"ORD-{random.randint(1000,9999)}",
            "city": random.choice(cities),
            "amount": round(random.uniform(10.0,500.0),2),
            "timestamp": event_time.strftime("%Y-%m-%d %H:%M:%S") 
        }

        producer.send('order_events',value=data)

        # 打印正常数据日志，减少刷屏
        if not is_late:
            print(f"发送正常数据：city = {data['city'], {data['amount']}}")

        time.sleep(20) #每1s发送一条数据

if __name__ == "__main__":
    start_producer()



