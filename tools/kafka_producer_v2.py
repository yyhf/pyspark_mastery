import json
import time 
import uuid
import random
from datetime import datetime
from kafka import KafkaProducer

def start_producer():
    producer = KafkaProducer(bootstrap_servers=["localhost:9092"],
                            value_serializer = lambda x: json.dumps(x).encode('utf-8'))

    print("》》》启动生产者，发送订单和广告数据")

    while True:
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 1 发送订单数据（带userid）
        order_data = {
            "type":"order",
            "order_id":str(uuid.uuid4())[:8],
            "user_id": str(random.randint(1,5)),
            "amount":round(random.uniform(100,1000),2),
            "timestamp":now_str
        }

        producer.send('order_events',value=order_data)


        # 2 广告曝光
        ad_id = str(uuid.uuid4())[:8]
        view_data = {
            "type":"ad_view",
            "ad_id":ad_id,
            "ad_type":"banner",
            "timestamp":now_str
        }

        producer.send('ad_views',value=view_data)

        # 广告点击
        # 30%概率产生点击，且时间滞后一点
        if random.random() < 0.3:
            click_data = {
                "type":"ad_click",
                "ad_id":ad_id,  # 必须和之前的ad_id一致才能做关联
                "timestamp":now_str
            }
            # 模拟一点点击延迟
            producer.send('ad_clicks',value=click_data)
        
            # 假设 now_str = "2025-12-12 18:48:48"
            # 假设 order_data, view_data, click_data 是一些数据
            print(f"""{now_str}发送一批数据: 
                1.订单数据: {order_data}
                2.曝光数据: {view_data}
                3.点击数据: {click_data}""")
        
        time.sleep(5)

if __name__ == "__main__":
    start_producer()