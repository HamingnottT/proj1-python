# This part of the application handles the PySpark portion
import pyspark
from pyspark.sql import SparkSession

print("="*48 + "\nspark_queries.py running\n" + "="*48)

class spark_queries:
    def __init__(self):
        self.spark = SparkSession.builder.appName("proj1-python").config("spark.master", "local").enableHiveSupport().getOrCreate()

    def df_demo(self, spark):
        # df_pyspark = spark.read.text("../input/Bev_BranchA.txt")
        df_pyspark = spark.read.text("input/Bev_BranchA.txt")

        df_pyspark.show()

        

    def start(self):
        self.df_demo(self.spark)

        self.spark.stop()