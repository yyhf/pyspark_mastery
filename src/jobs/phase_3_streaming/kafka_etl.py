import logging
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,LongType,TimestampType,DoubleType
import os,sys
base_dir = os.getcwd()
sys.path.append(base_dir)
from src.utils.spark_utils import get_spark_session

def run_job():
    checkpoint_dir = os.path.join(base_dir,"data/checkpoint/kafka_etl")
    spark = get_spark_session("Kafka_Window_Watermark","local[2]")
    raw_df = spark.readStream \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers","localhost:9092") \
                    .option("subscribe","order_events") \
                    .option("startingOffsets","latest") \
                    .load()
    
    data_schema = StructType() \
                .add("order_id",StringType()) \
                .add("city",StringType()) \
                .add("amount",DoubleType()) \
                .add("timestamp",TimestampType()) #必须是timestamp类型才能做window 
    
    # ETL逻辑
    #a. 将Binary转为String再解析JSON
    parsed_df = raw_df.select(
        F.from_json(F.col("value").cast("string"),data_schema).alias("data")
    ).select("data.*") #展开结构体
    
    #b. 添加watermark
    # 允许迟到数据晚5min
    watermarked_df = parsed_df \
                        .withWatermark("timestamp","5 minutes")
    
    #c. 开窗聚合
    windowed_count = watermarked_df \
                            .groupBy(
                                F.window("timestamp","1 minute"), # 窗口列
                                "city"
                            ) \
                            .agg(F.sum("amount").alias("total_gmv"))
    # 输出
    print("启动kafka流处理(Update模式)")
    query = windowed_count.writeStream \
                        .outputMode("update") \
                        .format("console") \
                        .option("truncate","false") \
                        .option("checkpointLocation",checkpoint_dir) \
                        .trigger(processingTime = "10 seconds") \
                        .start()
    query.awaitTermination()
                        