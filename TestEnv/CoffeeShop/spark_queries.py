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

    # Handles the back end Spark function calling in sub_portal
    def list_branches(self):
        self.df_demo(self.spark)
        self.spark.stop()
        

    def scenario_mode(self):
        def sub_options():
            print("Website option selected. Please input one of the numbers into the field below:\n\n"
                "1 = Scenario 1 \n"
                "2 = Scenario 2 \n"
                "3 = Scenario 3 \n"
                "4 = Scenario 4 \n"
                "5 = Scenario 5 \n"
                "6 = Scenario 6 \n"
                "0 = Exit To Main Menu \n")
        
        sub_options()
        option = int(input("Input here: "))

        while option != 0:
            if option == 1:
                # sql_queries.create_website()
                pass
            elif option == 2:
                # sql_queries.get_Certain_Website()
                pass
            elif option == 3:
                # print("\n")
                # sql_queries.get_all_websites()
                # print(f"\nReturned all websites from database under user {name}. Returning to menu. . .")
                pass
            elif option == 4:
                # sql_queries.update_website()
                pass
            elif option == 5:
                # sql_queries.del_website() 
                pass
            elif option == 6:
                pass
            else:
                print("\nInvalid response, please try again.\n")

            print("\n")
            sub_options()
            option = int(input("Input here: "))

        # spark may still continue running until user exits the application
        self.spark.stop()