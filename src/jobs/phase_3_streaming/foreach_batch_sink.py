from pyspark.sql.types import *
from pyspark.sql import functions as F
import os,sys
sys.append.join(os.getcwd())
from src.utils.spark_utils import get_spark_session
# **场景**：Structured Streaming 原生不支持直接写入 MySQL。但这是最常见的需求。
# **解决方案**：使用 `foreachBatch`。它把流拆成了一个个**小的微批次 (DataFrame)**，然后让你用普通的批处理代码去处理这个微批次。
# *   `foreachBatch` 是 Structured Streaming 的“逃生舱”。只要 Spark 原生没有 Sink，就用它。
# *   **注意**：`foreachBatch` 里的函数是在 Driver 端调用的，但 `batch_df.write...` 还是分布式的。不要在函数里尝试遍历 `batch_df` 的每一行（collect），否则 Driver 会爆

def run_job():
    spark = get_spark_session("Foreach_Batch_Sink","local[2]")
    stream_df = spark.readStream.format("rate").load()

    processed_df = stream_df.withColumn("doubled_value",F.col("value")*2)

    # 定义微批次处理逻辑
    # batch_df :当前这个微批次的数据
    # batch_id : 批次id 

    def save_to_mysql_simulator(batch_df,batch_id):
    #     batch_df.write \
    #         .format("jdbc") \
    #         .option("url","jdbc:mysql://localhost:3306/test")
    #         .option("driver","com.mysql.cj.jdbc.Driver") \
    #         .option("dbtable","test_table") \
    #         .option("user","root") \
    #         .option("password","123456") \
    #         .mode("append") \
    #         .save()

        # 模拟写入  只做count和show
        count = batch_df.count()
        print(f"批次{batch_id}一共有{count}条数据")
        if count > 0:
            batch_df.show(5)

        # 也可以吧处理过的数据再发给kafka或者其他操作

        print(f"批次{batch_id} 处理完毕")

    # 启动流,使用foreachBatch
    query = processed_df.writeStream.foreachBatch(save_to_mysql_simulator).start()
    
    query.awaitTermination()