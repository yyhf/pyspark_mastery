# 累加器的使用
# **核心挑战**：流任务运行在后台，我们怎么知道处理了多少条数据？有多少条解析失败了？Print 是没用的，因为 Print 在 Executor 上，Driver 看不到。
# **技术关键**：**LongAccumulator**。它是一个分布式共享变量，Executor 只能“加”，Driver 只能“读”。

from pyspark.sql.types import *
from pyspark.sql import functions as F
import os
import sys
import time
sys.path.append(os.getcwd())
from src.utils.spark_utils import get_spark_session

def run_job():
    spark = get_spark_session("Accumulator_Demo","local[2]")
    sc = spark.sparkContext  # Accumulator只能从SparkContext创建 

    # 1.定义累加器（Driver端）
    # •	Accumulator 是：
	# •	Executor → Driver 单向汇报
	# •	Executor 只能 add
	# •	Driver 只能 value（或打印对象）
    # total_count = sc.longAccumulator("TotalRecords")
    # error_count = sc.longAccumulator("ErrorRecords")

    # 【已修改】使用 sc.accumulator(0) 代替 sc.longAccumulator()
    total_count = sc.accumulator(0)  # 使用初始值0，隐式创建 LongAccumulator
    error_count = sc.accumulator(0)

    # 2.注册UDF来统计“副作用”数据（side effect）:在执行计算的同时，修改或依赖“Spark 计算结果之外”的外部状态的函数（改变了外部变量的状态），Spark 中唯一“官方允许”的 Side Effect：Accumulator
    # 因为spark的分布式，懒执行，可能重试，可能并行执行，Driver与Executor是不同进程，所以肯呢个执行多次，并发执行，在不同机器上执行
    def count_metric(val,is_error):
        total_count.add(1)
        if is_error:
            error_count.add(1)
        return val
    
    # StringType()是占位用的，实际逻辑在Side Effect，不必必填，但建议填，spark是强schema引擎，不填有可能推断schema失败
    # 把 side effect 包进UDF 
    count_udf = F.udf(lambda v,e: count_metric(v,e),StringType())

    # 3.模拟读取（用Rate Source 模拟每秒生成数据）
    stream_df = spark.readStream \
                    .format("rate") \
                    .option("rowsPerSecond",5) \
                    .load()
    
    # 4.模拟业务，如果value是3的倍数，则算作错误数据
    processed_df = stream_df.withColumn(
        "processed_val",
        count_udf(F.col("value").cast("string"),(F.col("value") % 3 == 0))
    )

    # 生产级别推荐方式：使用聚合算指标
    # stream_df \
    #     .withColumn("is_error", F.col("value") % 3 == 0) \
    #     .groupBy() \
    #     .agg(
    #         F.count("*").alias("total"),
    #         F.sum(F.col("is_error").cast("int")).alias("error")
    #     )

    # 5.启动查询
    query = processed_df.writeStream \
                        .outputMode("append") \
                        .format("console") \
                        .option("truncate","false") \
                        .start()
    # 6.在Driver开启一个监控县城，定期打印累加器的值（大屏显示的实时处理条数的来源）
    try:
        while True:
            time.sleep(3)
            if query.isActive:
                # Driver 端获取累加器的值
                print(f"[Monitor] 总处理：{total_count},异常数：{error_count}")
            else:
                break
    except KeyboardInterrupt:
        query.stop()
