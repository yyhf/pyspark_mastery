
import os 
import sys 
# 将项目根目录加入到python path 防止报错找不到模块
sys.path.append(os.getcwd())
from src.utils.spark_utils import get_spark_session 

spark = get_spark_session("read_parquet_test","local")
base_dir = os.getcwd()

print(base_dir)

input_path = os.path.join(base_dir,"data/output/city_stats/part-00000-77793d4b-f856-44e9-af30-ab570652c2a5-c000.snappy.parquet")

spark.read.parquet(input_path).show()

spark.stop()


