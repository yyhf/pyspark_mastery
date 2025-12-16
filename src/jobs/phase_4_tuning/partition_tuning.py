# 分区数调整是Spark SQL 中**最重要**的参数，没有之一。

# *   **默认值**：200。
# *   **含义**：在进行 Join 或 GroupBy (Shuffle) 时，结果会被分成多少份。

# **场景模拟**：
# 1.  **太小 (比如 1)**：所有数据挤在一个文件里，处理极慢，容易 OOM。
# 2.  **太大 (比如 2000 处理小数据)**：产生大量小文件（KB级别），Spark 花在调度 Task 上的时间比真正干活的时间还长。
# 💡 调优法则**：
# *   官方建议：每个 Partition 处理的数据量最好在 **128MB - 200MB** 左右。
# *   如果你的 shuffle 阶段总数据量是 10GB。
#     *   10GB = 10240MB。
#     *   合适的分区数 = 10240 / 128 ≈ 80。
#     *   所以设 `spark.sql.shuffle.partitions = 80` 比默认的 200 好。

import time
import os,sys
from utils.spark_utils import get_spark_session

def run_job():
    spark = get_spark_session("Partition_Turning","local[2]")

    # 造500万条数据、
    df = spark.range(0,5000000).toDF("id")
    # 制造一个需要shuffle的操作 
    df_grouped = df.groupBy(df.id % 10 ).count()

    # 1 分区数取200(太多)
    spark.conf.set("spark.sql.shuffle,partitions",200)
    start = time.time()
    df_grouped.collect()
    print(f"分区数200耗时: {time.time() - start:.2f}s")

    # 2 分区数取10 （合适）
    spark.conf.set("spark.sql.shuffle.partitions",10)
    start = time.time()
    df_grouped.collect()
    print(f"分区数10耗时: {time.time() - start:.2f}s")

    # 3 分区数取1（太少）
    spark.conf.set("spark.sql.shuffle.partitions",1)
    start = time.time()
    df_grouped.collect()
    print(f"分区数1耗时: {time.time() - start:.2f}s")

# if __name__ == "__main__":
#     run_job()