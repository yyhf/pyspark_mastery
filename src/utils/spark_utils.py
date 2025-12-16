from pyspark.sql import SparkSession
import os 

# 获取sparkSession 函数
def get_spark_session(app_name,deploy_mode):
    """
    get_spark_session 的 Docstring
    
    :param app_name: 应用名称
    :param deploy_mode: 运行模式（local,yarn,etc）
    """
    
    spark = SparkSession.builder \
                          .appName(app_name) \
                          .master(deploy_mode) \
                          .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
                          .config("spark.sql.shuffle,partitions","2") \
                          .getOrCreate()
                          
    # 设置日志级别，避免控制台被INFO刷屏
    spark.sparkContext.setLogLevel("WARN")

    return spark

def run_job():
    # 1. 初始化 Spark
    spark = get_spark_session("ETL_Job_Demo","local[*]")
    
    # 2. 读取数据 (E - Extract)
    # 假设 data/input 下有个 users.csv
    input_path = "data/input/users.csv" 
    # 如果文件不存在，这里只是演示代码结构
    try:
        df = spark.read.csv(input_path, header=True, inferSchema=True)
    except Exception:
        print("数据文件未找到，请先准备数据")
        return

    # 3. 数据处理 (T - Transform)
    # 示例：过滤年龄大于 18 且 名字不为空
    processed_df = df.filter(F.col("age") > 18) \
                     .withColumn("processed_time", F.current_timestamp()) \
                     .groupBy("city").count()

    # 4. 写入数据 (L - Load)
    output_path = "data/output/user_stats"
    processed_df.write.mode("overwrite").parquet(output_path)
    
    print(f"ETL 完成，数据已保存至 {output_path}")
    
    # 5. 关闭资源
    spark.stop()

if __name__ == "__main__":
    run_job()


#### 3. `main.py` (统一入口)
# 在实际工作中，我们通常通过 `spark-submit` 提交这个文件，通过参数决定运行哪个 Job。

# ```python
import sys
import argparse
from src.jobs.phase_1_rdd import word_count

def main():
    parser = argparse.ArgumentParser(description="PySpark 学习项目入口")
    parser.add_argument('--job', type=str, required=True, help="要运行的任务名称 (如: word_count, etl)")
    
    args = parser.parse_args()
    
    job_map = {
        "word_count": word_count.run_job,
        "etl": etl_job.run_job
    }
    
    job_func = job_map.get(args.job)
    
    if job_func:
        print(f"开始运行任务: {args.job}")
        job_func()
    else:
        print(f"未知的任务: {args.job}. 可用任务: {list(job_map.keys())}")

if __name__ == "__main__":
    main()