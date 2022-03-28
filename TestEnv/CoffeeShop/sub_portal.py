# Sub-menu of main_portal: Scenario Mode option
from CoffeeShop.spark_queries import spark_queries

class sub_portal:
    # Option 
    def list_branches():
        print("-"*48)
        print("Returning list of all our active branches:")
        print("-"*48)

        print("-"*48 + "\nWelcome back to the main menu:\n" + "-"*48)

    def scenario_mode():
        print("-"*48)
        print("Scenario Mode chosen. This lists out Project 1's\nproblems by scenario.")
        print("-"*48)

        def sub_options():
            print("Website option selected. Please input one of the numbers into the field below:\n\n"
                "1 = Add Website to Database \n"
                "2 = Find a Specific Website \n"
                "3 = Get All Websites \n"
                "4 = Edit Website Name \n"
                "5 = Delete Website Info \n"
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
            else:
                print("\nInvalid response, please try again.\n")

            print("\n")
            sub_options()
            option = int(input("Input here: "))   


