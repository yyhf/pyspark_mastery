from src.utils.spark_utils import get_spark_session
import time

def run_job():
    # 初始化spark
    spark = get_spark_session("HelloWorld_Job","local")
    sc = spark.sparkContext # 获取RDD的入口 SparkContext 

    # 创建数据
    data = [1,2,3,45,6,7,8,9,10]
    rdd = sc.parallelize(data)

    # 数据转换
    # 找出所有的偶数，并乘以10
    result_rdd = rdd.filter(lambda x: x % 2 == 0) \
                    .map(lambda x: x*10)
    
    # 触发执行
    # 在调用collect之前，上面步骤都没有执行，因为是惰性执行
    print("开始计算")
    result = result_rdd.collect()

    # 打印结果
    print("-"*30)
    print(f"原始数据：{data}")
    print(f"计算结果：{result}")
    print("-"*30)

    # 停止运行
    spark.stop()