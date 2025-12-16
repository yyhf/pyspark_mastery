from pyspark.sql.types import StructType,StructField,StringType,IntegerType,LongType,TimestampType,DoubleType
from pyspark.sql import functions as F 
import os,sys
sys.path.append(os.getcwd())
from src.utils.spark_utils import get_spark_session

def run_job():
    spark = get_spark_session("Stream_Stream)Join","local[2]")

    # 通用kafka读取函数
    def read_kafka_topic(topic,schema):
        return spark.readStream \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers","localhost:9092") \
                    .option("subscribe",topic) \
                    .option("startingOffsets","latest") \
                    .load() \
                    .select(F.from_json(F.col("value").cast("string"),schema).alias("data")) \
                    .select("data.*") 
                    #.withWatermark("timestamp","1 minute") # 两个流都需要设置Watermark  生产级别的Watermark需要设置在最终使用的字段列上，下面有重命名

    # 1.读取View流
    view_schema = StructType().add("ad_id",StringType()).add("timestamp",TimestampType())
    view_stream = read_kafka_topic("ad_views",view_schema).withColumnRenamed("timestamp","view_time").withWatermark("view_time","1 minute")


    # 读取Click流
    click_schema = StructType().add("ad_id",StringType()).add("timestamp",TimestampType())
    click_stream = read_kafka_topic("ad_clicks",click_schema).withColumnRenamed("timestamp","click_time").withWatermark("click_time","1 minute")


    # 2.双流join
    # A Join key :ad_id
    # B 时间约束:Click 时间必须在View时间滞后，且不能晚于View + 1min
    # 如果没有时间约束，Spark为了保证能join上，会永久保存State，导致OOM
    join_condition = (
        (view_stream.ad_id == click_stream.ad_id) &
        (click_stream.click_time >= view_stream.view_time) &
        (click_stream.click_time <=  F.expr("view_time + INTERVAL 1 MINUTE"))
    )


    result_df = view_stream.join(click_stream,on = join_condition,how = "inner") \
                        .select(view_stream.ad_id,view_stream.view_time,click_stream.click_time)

    print("==== View Schema ====")
    view_stream.printSchema()

    print("==== Click Schema ====")
    click_stream.printSchema()

    print("==== Explain Plan ====")
    result_df.explain("extended")


    print("启动stream-stream广告归因")

    query = result_df.writeStream \
                    .outputMode("append") \
                    .format("console") \
                    .option("truncate","false") \
                    .start()

    query.awaitTermination()

