# Main menu of program
import sys
from CoffeeShop.spark_queries import spark_queries

print("="*48 + "\nmain_portal.py running\n" + "="*48)
class menu:
    def main_menu():
        # initalizes the start function in spark_queries
        # spark_queries().start()

        print("-"*48)
        print("Welcome user to Coffee Shop Inc. database!\nBelow you will find our program's main directory:")
        print("-"*48)

        def main_options():
            print("What information are you looking for today?\n\n"
                "1 = List all branches in region \n"
                "2 = Scenario Mode \n"
                "0 = Cancel & Exit \n")
        
        main_options()
        option = int(input("input here: "))

        while option != 0:
            if option == 1:
                # sub_menus.websites() 
                pass 
            elif option == 2:
                # sub_menus.user() 
                pass    
            else:
                print("\nInvalid response, please try again.\n")
                main_options()
                option = int(input("Input here: "))
        
        print("\nEnding program. . .\n")

        # ensures spark and the program has properly ended
        sys.exit()