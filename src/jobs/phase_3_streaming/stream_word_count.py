from pyspark.sql import functions as F
import os,sys
sys.path.append(os.getcwd())
from src.utils.spark_utils import get_spark_session

# 消费使用socket模拟的流式数据

def run_job():
    # 注意：流处理任务通常至少需要 2 个核 (1个负责收数据，1个负责处理)
    spark = get_spark_session("Stream_WordCount", deploy_mode="local[2]")
    
    # 1. 设置监听源 (ReadStream)
    # 这是一个 "流式" DataFrame
    lines_df = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load()

    # 2. 处理逻辑 (和之前写批处理一模一样！)
    # lines_df 只有一列，默认叫 "value"
    words_df = lines_df.select(
        F.explode(F.split(lines_df.value, " ")).alias("word")
    )
    
    # 实时统计词频
    counts_df = words_df.groupBy("word").count()

    # 3. 启动流任务 (WriteStream)
    print(">>> 正在启动流计算，请在 socket 终端输入文字...")
    
    query = counts_df.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    # outputMode 解释:
    # "complete": 每次输出完整的统计结果 (适合聚合操作)
    # "append": 只输出新增的数据 (适合简单的 ETL，不支持聚合后输出)
    # "update": 只输出有变化的数据
    
    # 4. 阻塞主线程
    # 流任务是后台运行的，必须这就话卡住主程序，否则程序一启动就退出了
    query.awaitTermination()
