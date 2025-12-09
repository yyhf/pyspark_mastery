# from pyspark.sql import SparkSession

# spark = SparkSession.builder() \
#                     .appName("pysparkTest") \
#                     .getOrCreate()

# df = spark.createDataFrame([(1,"Alice"),(2,"Bob")],["id","name"])
# df.printSchema()
# df.show()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("LocalTest") \
    .master("local[*]") \
    .getOrCreate()

df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
df.show()

spark.stop()