# This part of the application handles the PySpark portion
import pyspark
from pyspark.sql import SparkSession

print("="*48 + "\nspark_queries.py running\n" + "="*48)

class spark_queries:
    def __init__(self):
        self.spark = SparkSession.builder.appName("proj1-python").config("spark.master", "local").enableHiveSupport().getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")

    def df_demo(self, spark):
        # df_pyspark = spark.read.text("../input/Bev_BranchA.txt")
        df_pyspark = spark.read.text("input/Bev_BranchA.txt")

        df_pyspark.show()

    def table_creator(self):
        # ~ Create Spark SQL tables - loading Branch & conscount respectively ~
        # self.spark.sql("create table if not exists bev_branches(beverage String,branch String) row format delimited fields terminated by ','")
        # self.spark.sql("create table if not exists bev_conscount(beverage String,conscount Int) row format delimited fields terminated by ','")
        # /* ~ Load Bev Branch data into bev_branches ~ */
        # self.spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE bev_branches")
        # self.spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE bev_branches")
        # self.spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE bev_branches")
        # /* ~ Load Bev Conscount data into bev_conscount ~ */
        # self.spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountA.txt' INTO TABLE bev_conscount")
        # self.spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountB.txt' INTO TABLE bev_conscount")
        # self.spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountC.txt' INTO TABLE bev_conscount")

        # if data is already loaded into spark warehouse
        pass

    def scenario_1(self, spark):
        print("\nTotal consumers for Branch 1:\n")
        spark.sql("SELECT sum(c.conscount) FROM bev_branches b join bev_conscount c on b.beverage=c.beverage where b.branch = 'Branch1'").show()

        print("\nTotal consumers for Branch 2:\n")
        spark.sql("SELECT sum(c.conscount) FROM bev_branches b join bev_conscount c on b.beverage=c.beverage where b.branch = 'Branch2'").show()

        print("\nCombined table of Branches and Consumer Count by Beverage Name:\n")
        spark.sql("select DISTINCT b.branch, c.beverage, c.conscount from bev_branches b join bev_conscount c on b.beverage = c.beverage ORDER BY b.branch desc").show()

    def scenario_2(self, spark):
        print("\nTop most consumed beverages in Branch 1:\n")
        spark.sql("SELECT DISTINCT c.beverage, c.conscount FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage WHERE b.branch = 'Branch1' ORDER BY c.conscount desc").show(1)

        print("\nTop least consumed beverages in Branch 2:\n")
        spark.sql("SELECT DISTINCT c.beverage, c.conscount FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage WHERE b.branch = 'Branch2' ORDER BY c.conscount asc").show(1)

        print("\nAverage consumed beverage in Branch 2:\n")
        spark.sql("SELECT c.beverage, ROUND(AVG(c.conscount)) avg_conscount FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage WHERE b.branch = 'Branch2' GROUP BY c.beverage").show(1)

    def scenario_3(self, spark):
        # There is no branch 10 but querying this was technically a part of the requirements
        print("\nBeverages available on Branch10:\n")
        spark.sql("SELECT DISTINCT c.beverage FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage WHERE b.branch = 'Branch10'").show(40)
        
        print("\nBeverages available on Branch9:\n")
        spark.sql("SELECT DISTINCT c.beverage FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage WHERE b.branch = 'Branch9'").show(40)
        
        print("\nBeverages available on Branch8:\n")
        spark.sql("SELECT DISTINCT c.beverage FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage WHERE b.branch = 'Branch8'").show(40)
        
        print("\nBeverages available on Branch1:\n")
        spark.sql("SELECT DISTINCT c.beverage FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage WHERE b.branch = 'Branch1'").show(40)
        
        print("\nBeverages available on Branch 4 and 7:\n")
        spark.sql("SELECT DISTINCT b.branch, c.beverage FROM bev_branches b INNER JOIN bev_conscount c ON b.beverage=c.beverage WHERE b.branch = 'Branch4' OR b.branch = 'Branch7' ORDER BY b.branch asc").show(150)

    def scenario_4(self, spark):
        # ~ Create new table partitioned by branch from existing other tables ~
        # spark.sql("CREATE TABLE IF NOT EXISTS bev_branches_partitioned(beverage String) PARTITIONED BY (branch String)")
        # spark.sql("set hive.exec.dynamic.partition.mode=nonrestrict")

        # ~ Loads data from other tables into partitioned table ~
        # spark.sql("INSERT OVERWRITE TABLE bev_branches_partitioned PARTITION(branch) SELECT beverage,branch FROM bev_branches")

        # ~ Showcase of partitioned table calling Branch 4 & 7 ~
        print("\nNOTE: Partitioned table is a table created based on the joined main tables.\nThis showcase calls branch 4 and 7 like scenario 3.\n")

        spark.sql("SELECT * FROM bev_branches_partitioned WHERE branch = 'Branch4' OR branch = 'Branch7'").show(150)
        spark.sql("Describe formatted bev_branches_partitioned").show(50)
    
    def scenario_5(self, spark):
        # ~ Adds test notes and comments ~
        # spark.sql("ALTER TABLE bev_branches_partitioned SET TBLPROPERTIES('comment' = 'Partitioned by branch.')")
        # spark.sql("ALTER TABLE bev_branches_partitioned SET TBLPROPERTIES('note' = 'TEST note.')")

        # ~ Displays information on partitioned table ~
        print("\nInformation on partitioned table:\n")
        spark.sql("SHOW TBLPROPERTIES bev_branches_partitioned").show
        spark.sql("Describe formatted bev_branches_partitioned").show(50)

        # ~ Displays information on the two main tables used in this project for comparison ~
        print("\nOther tables used [source]:\n")
        spark.sql("Describe formatted bev_branches").show
        spark.sql("Describe formatted bev_conscount").show

        # /!\ Delete functionality bugged - pending fix
        # Deletes a row from a table - for comparison there is a before and after query on row count
        # print("\nCount of total beverages in branch 4:\n")
        # print("~ Before row deletion ~\n")
        # spark.sql("SELECT count(beverage) FROM bev_branches_partitioned WHERE branch = 'Branch4'").show()
        # spark.sql("DELETE FROM bev_branches_partitioned WHERE branch = 'Branch4' AND beverage = 'SMALL_cappuccino'")
        # print("~ After row deletion ~\n")
        # spark.sql("SELECT count(beverage) FROM bev_branches_partitioned WHERE branch = 'Branch4'").show()

    # /!\ issues using df method in PySpark - pending fix
    def scenario_6(self, spark):
        pivot1src = spark.sql("SELECT b.branch, c.beverage, c.conscount FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage")

        print("\nAbsolute maximum consumer sales per beverage:\n")
        spark.sql("SELECT max(c.conscount) FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage").show()

        print("\nTotal number of beverages available over entire region:\n")
        spark.sql("SELECT count(b.beverage) FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage").show

        print("\nTotal consumer sales in all branches:\n")
        pivot1src.groupBy("b.branch").agg(sum("c.conscount")).sort("b.branch").show(50)

        print("\nIndividual beverage sales per branch:\n")
        # pivot1src.groupBy("c.beverage").pivot("b.branch").agg(count("c.conscount")).sort("c.beverage").show(50)

    # Handles the back end Spark function calling in sub_portal
    def list_branches(self):
        self.df_demo(self.spark)
        self.spark.stop()
        

    def scenario_mode(self):
        self.table_creator()
        print()
        print("-"*48)
        def sub_options():
            print("\nWebsite option selected. Please input one of the numbers into the field below:\n\n"
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
                self.scenario_1(self.spark)
            elif option == 2:
                self.scenario_2(self.spark)
            elif option == 3:
                self.scenario_3(self.spark)
            elif option == 4:
                self.scenario_4(self.spark)
            elif option == 5:
                self.scenario_5(self.spark)
            elif option == 6:
                self.scenario_6(self.spark)
            else:
                print("\nInvalid response, please try again.\n")

            print("\n")
            sub_options()
            option = int(input("Input here: "))

        # spark may still continue running until user exits the application
        self.spark.stop()