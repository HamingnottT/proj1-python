# Main menu of program
import sys
from CoffeeShop.sub_portal import sub_portal

print("="*48 + "\nmain_portal.py running\n" + "="*48)
class menu:
    def main_menu():
        # initalizes the start function in spark_queries
        # spark_queries().start()
        print()
        print("-"*48)
        print("Welcome user to Coffee Shop Inc. database!\nBelow you will find our program's main directory:")
        print("-"*48)
        print()

        def main_options():
            print("What information are you looking for today?\n\n"
                "1 = List all branches in region \n"
                "2 = Scenario Mode \n"
                "0 = Cancel & Exit \n")
        
        main_options()
        option = int(input("input here: "))

        while option != 0:
            if option == 1:
                sub_portal.list_branches()
                # option = 99
            elif option == 2:
                sub_portal.scenario_mode()
                # option = 99
                pass
            # elif option == 99:
            #     # choosing an option caused an infinite while loop, an unexpected issue despite proj0 having none related to this
            #     # for now this is a way to circumvent this bug
            #     main_options()
            #     option = int(input("Input here: "))
            else:
                print("\nInvalid response, please try again.\n")

            print("\n")    
            main_options()
            option = int(input("Input here: "))
        
        print("\nEnding program. . .\n")

        # ensures spark and the program has properly ended
        sys.exit()