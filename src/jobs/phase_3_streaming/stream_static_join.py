from pyspark.sql.types import StructType,StructField,StringType,IntegerType,LongType,TimestampType,DoubleType
from pyspark.sql import functions as F
import os,sys
sys.path.append(os.getcwd())
from src.utils.spark_utils import get_spark_session 

def run_job():
    spark = get_spark_session("Stream_Static_Join","local[2]")

    # 模拟静态维度数据
    user_data= [
        ("1","Alice","VIP"),
        ("2","Bob","Common"),
        ("3","Charlie","VIP"),
        ("4","David","Common"),
        ("5","Eve","SuperVIP")
    ]

    static_user_df = spark.createDataframe(user_data,["user_id","user_name","vip_level"])
    # 广播小表数据，发送给所有节点(仅限小表能放入Driver/executor 内存的情况下，默认10M，生产环境可以视情况调制几百M)
    # SortMergeJoin 会首先进行shuffle和sort 
    static_user_df = F.broadcast(static_user_df)

    # 流式数据
    stream_schema = StructType().add("type",StringType()) \
                                .add("order_id",StringType()) \
                                .add("user_id",StringType()) \
                                .add("amount",DoubleType()) \
                                .add("timestamp",TimestampType()) 

    stream_df = spark.readStream() \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers","localhost:9092") \
                    .option("subscribe","order_events") \
                    .option("startingOffsets","latest") \
                    .load() \
                    .select(F.from_json(F.col("value").cast("string"),stream_schema).alias("data")) \
                    .select("data.*") \
                    .filter("type = 'order'") # 只处理订单数据

    # 执行流与静态数据的join

    enriched_df = stream_df.join(
        static_user_df,
        on = "user_id",
        how = "left"
    )


    # 只要VIP用户的订单
    vip_orders = enriched_df.filter(F.col("vip_level").isin("VIP","SuperVIP"))

    print("启动流式与静态维度join")

    query = vip_orders.writeStream() \
                    .outputMode("append") \
                    .format("console")  \
                    .option("truncate","false") \
                    .start()

    query.awaitTermination()