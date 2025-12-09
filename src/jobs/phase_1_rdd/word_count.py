from src.utils.spark_utils import get_spark_session
import time

def run_job():
    spark = get_spark_session("WordCount_job","local")
    sc = spark.sparkContext

    print("1.数据准备")

    data = [
        "hello world",
        "hello spark",
        "spark java"
    ]

    rdd = sc.parallelize(data)
    result_rdd = rdd.flatMap(lambda x: x.split(" ")) \
                    .map(lambda x: (x,1)) \
                    .reduceByKey(lambda x,y: x+y)

    sorted_rdd = result_rdd.sortBy(lambda x: x[1],ascending=False)
    result = sorted_rdd.collect()

    for word,count in result:
        print(f"单词: {word:<10} 出现次数: {count}")

    spark.stop()

if __name__ == "__main__":
    run_job()