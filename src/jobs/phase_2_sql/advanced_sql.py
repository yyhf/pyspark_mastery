from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType
from pyspark.sql import Window
import os ,sys
# 必须先将项目根目录添加，否则找不到src模块
sys.path.append(os.getcwd())
from src.utils.spark_utils import get_spark_session

def run_job():
    spark = get_spark_session("Advanced_SQL_Job","local[*]")

    base_dir = os.getcwd()
    input_path = os.path.join(base_dir,"data/input/users.csv")

    #定义读取文件的Schema 
    data_schema = StructType(
        [
            StructField("id",IntegerType(),True),
            StructField("name",StringType(),True),
            StructField("age",IntegerType(),True),
            StructField("city",StringType(),True),
            StructField("salary",DoubleType(),True)
        ]
    )

    df = spark.read.csv(input_path,header=True,schema=data_schema)
    df.printSchema()
    df.show()

    # Important Considerations:
    # 1. Performance: UDFs can be less performant than built-in Spark functions because Spark's Catalyst optimizer cannot fully optimize the custom logic within a UDF, treating it as a "black box." Prioritize built-in functions whenever possible.
    # 2. Data Types: Explicitly defining the return type of the UDF is essential for type safety and performance.
    # 3. Serialization: The Python function and any objects it references need to be serializable for distribution across Spark executors.
    # 4. Error Handling: UDFs can be a source of runtime errors if not carefully implemented, as errors within the UDF are not always easily caught by Spark's optimizer.

    # 需求1：定义一个python UDF 将姓名只显示首字母，其余以*展示
    # 注意：Python UDF性能较低(每行数据需要序列化发送给JVM再反序列化发给spark)，可以使用Pandas udf(Vectorized UDF)，利用Arrow格式批量传数据
    def mask_name_logic(name):
        if not name:
            return 'Unknown'
        else:
            return name[0]+"****"

    # 注册udf (必须告知返回值类型)
    mask_name_udf = F.udf(mask_name_logic,StringType())
    #使用
    df_mask_name = df.withColumn("masked_name",mask_name_udf(F.col("name")))
    df_mask_name.show()



    # 需求2: 找出每个城市薪资最高的 2 个人
        # 关键步骤: 
        #   不要用 groupBy (因为我们要保留人名)
        #   要用 Window "画圈圈": 
        #       1. partitionBy("city"): 在每个城市内部画圈
        #       2. orderBy(desc("salary")): 在圈内按薪资排队

    #### 2. Window 函数的数据倾斜 ⚖️
    # *   `Window.partitionBy("city")` 会把同一个 City 的所有数据拉到**同一个 Executor** 上处理。
    # *   **风险**：如果你的数据里，“NewYork” 的人有 1 亿个，而其他城市只有 10 个。那么处理 NewYork 的那台机器会卡死（OOM），其他机器在围观。这就是**数据倾斜**
    
    window_spec = Window.partitionBy(F.col("city")).orderBy(F.col("salary").desc())
    # partitionBy 可以接收字符串列别 ("name","city"),也可以接收Column对象 F.col("name")
    # orderBy 虽然可以接收字符串，但是在指定排序方向 asc()或desc()时，只能使用F.col() 或者F.desc() 。orderBy("salary") 无法指定排序方向
    # window_spec = Window.partitionBy("city").orderBy(F.desc("salary"))   #等效于上面的写法

    # 使用 rank() 或 row_number() 打标签
        # row_number: 1, 2, 3, 4... (即使薪资一样，名次也不重复)
        # rank: 1, 2, 2, 4... (薪资一样名次并列)
    df_ranked = df.withColumn("rank",F.row_number().over(window_spec))
    df_filter = df_ranked.filter(F.col("rank") <= 2)
    df_top2 = df_filter.select("city","name","salary","rank").show()

    spark.stop()