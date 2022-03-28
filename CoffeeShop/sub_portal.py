# Sub-menu of main_portal: Scenario Mode option
from CoffeeShop.spark_queries import spark_queries

class sub_portal:
    # Option 
    def list_branches():
        
        print()
        print("-"*48)
        print("Returning list of all our active branches:")
        print("-"*48)

        spark_queries().list_branches()

        print("-"*48 + "\nWelcome back to the main menu:\n" + "-"*48)

    def scenario_mode():
        print()
        print("-"*48)
        print("Scenario Mode chosen. This lists out Project 1's\nproblems by scenario. Initializing Spark. . .")
        print("-"*48)

        print()

        spark_queries().scenario_mode()   


