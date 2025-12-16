# 假设一行数据代表一次“会话”，里面有一个 `actions` 数组，包含用户在这个会话里做的所有事情。
# 数据长这样：
# ```json
# {
#   "user_id": "u1",
#   "session_id": "s100",
#   "actions": [
#     {"time": "10:00", "type": "login"},
#     {"time": "10:05", "type": "view"},
#     {"time": "10:10", "type": "buy"}
#   ]
# }
# ```
# **需求**：把这一行变成三行（扁平化），统计每种 `type` 的出现次数。

# **思路**：
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField,StringType,ArrayType
import os,sys
sys.path.append(os.getcwd())
from src.utils.spark_utils import get_spark_session

def run_job():
    spark = get_spark_session("complex_data","local[*]")

    # 构建数据
    data = [
            ("u1", "s100", [("10:00", "login"), ("10:05", "view"), ("10:10", "buy")]),
            ("u2", "s101", [("11:00", "login"), ("11:02", "logout")]),
            ("u3", "s102", []) # 空数组
        ]

    data_schema = StructType(
        [
            StructField("user_id",StringType(),True),
            StructField("session_id",StringType(),True),
            StructField("actions",ArrayType(
                StructType([
                    StructField("time",StringType(),True),
                    StructField("type",StringType(),True)
                ])
            ),True)
        ]
    )


    df = spark.createDataFrame(data,data_schema)

    df.printSchema()
    df.show()

    df_processed = df.select("user_id","session_id",F.explode("actions").alias("action"))
    df_processed.show(truncate=False)


    df_explode = df_processed.select("user_id","session_id",F.col("action.time").alias("action_time"),F.col("action.type").alias("action_type"))
    df_explode.show(truncate=False)


    df_explode.groupBy("action_type").count().show()

    spark.stop()