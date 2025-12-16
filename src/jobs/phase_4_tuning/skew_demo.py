"""
**现象**：
任务进度条跑到 99% 也就是一瞬间的事，但最后那 1% 的任务（Task）可能要跑几个小时，甚至最后报 OOM (Out of Memory) 失败。

**原因**：
假设你要 `groupBy("city")`。
*   Task 1 处理 "Beijing"：有 1000 万条数据。
*   Task 2 处理 "Shanghai"：有 100 条数据。
*   Task 2 秒做完，Task 1 处理到死。这就是**数据倾斜**。

"""

from pyspark.sql import functions as F
from pyspark.sql.types import *
import random
import time
import os
import sys
sys.path.append(os.getcwd())
from utils.spark_utils import get_spark_session

def run_job():
    spark = get_spark_session("Skew_Demo","local[2]")
    # 为了演示效果，调低分区数，让冲突更明显
    spark.conf.set("spark.sql.shuffle.partitions", 3) 

    print(">>> 1. 制造倾斜数据...")
    # 构造一个大表: 
    # 99% 的数据 key 都是 "A" (倾斜源)
    # 1% 的数据 key 是 "B" 到 "Z"
    # 构建空数组存人造数据
    skew_data = []

    for _ in range(1000000):
        skew_data.append(("A", random.randint(0, 100)))
    
    for i in range(10000):
        key = chr(ord('B') + (i % 25))
        skew_data.append((key, random.randint(1,100)))

    large_df = spark.createDataFrame(skew_data,['key','value'])

    # 构造一个小的维表
    small_data = [('A','Label A'), ('B','Label B'), ('C','Label C')]
    small_df = spark.createDataFrame(small_data,['key','value'])
    print("数据准备完毕")

    # 场景1  普通join (SortMergeJoin) 
    # 会触发shuffle 所有的A 会拉取到同一个Partition(同一个Task)中
    print("方案1 普通join(存在倾斜)")
    start = time.time() 
    # 强制禁用广播，模拟生产环境大表与大表的join
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)

    result_1 = large_df.join(small_df,"key","inner")
    # action触发运算
    count_1 = result_1.count()
    # end = time.time()
    # 可以去localhost:4040看stage
    print(f"普通 Join 耗时: {time.time() - start:.2f}s, Count: {count_1}")   # 普通 Join 耗时: 7.00s, Count: 1000800 


    # 场景2 显示使用broadcast优化

    start = time.time()
    from pyspark.sql.functions import broadcast
    result_2 = large_df.join(broadcast(small_df),"key","inner")
    count_2 = result_2.count()
    print(f"Broadcast Join 耗时: {time.time() - start:.2f}s, Count: {count_2}")   # Broadcast Join 耗时: 0.37s, Count: 1000800

    # 防止程序退出太快，留时间给你看 UI
    print("\n>>> 任务结束，请在 60秒内 查看 Spark UI (http://localhost:4040)")
    time.sleep(180)
    spark.stop()
