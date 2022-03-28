# import sys
# import findspark        # imported incase pyspark is not found in PATH
# findspark.init()
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("proj1-python").config("spark.master", "local").enableHiveSupport().getOrCreate()

# df_pyspark = spark.read.text("../input/Bev_BranchA.txt")
df_pyspark = spark.read.text("input/Bev_BranchA.txt")

df_pyspark.show()