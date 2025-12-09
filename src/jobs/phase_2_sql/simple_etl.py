from pyspark.sql import functions as F 
from pyspark.sql.types import StructType,StructField,StringType,LongType,IntegerType
from src.utils.spark_utils import get_spark_session 
import os 


def run_job():
    spark = get_spark_session("Simple_ETL_Job","local")
    
    # 获取当前工作区目录，确保能找到数据
    base_dir = os.getcwd()
    input_path = os.path.join(base_dir,"data/input/users.csv")
    output_path = os.path.join(base_dir,"data/output/city_stats")

    # 定义文件schema
    users_schema = StructType(
        [
            StructField("id",LongType(),True),
            StructField("name",StringType(),True),
            StructField("age",IntegerType(),True),
            StructField("city",StringType(),True),
            StructField("salary",IntegerType(),True)
        ]
    )

    # 读取数据
    df = spark.read.csv(input_path,schema=users_schema,header=True,sep=",")
    # 也可以使用自动类型推断 但是会扫描全部数据，当数据集比较大的时候不太好
    # df = spark.read.csv(input_path,header=True,inferSchema=True)

    # 预览原始数据
    df.printSchema()
    df.show(20)

    # 数据清洗与转换

    result_df = df.dropna(subset=["name"]) \
                .filter(F.col("age") >= 18) \
                .groupBy("city") \
                .agg(
                    F.avg(F.col("salary")).alias("avg_salary"),
                    F.count("*").alias("user_count"))\
                .orderBy(F.col("avg_salary").desc())

    result_df.show()

    # 保存结果数据
    result_df.write.mode("overwrite").parquet(output_path)

    spark.stop()


