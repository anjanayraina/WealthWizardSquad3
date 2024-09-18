import subprocess

def run_main_module():
    try:
        # Run the command using subprocess
        result = subprocess.run(['python', '-m', 'src.main'], check=True, capture_output=True, text=True)
        
        # Output the result
        print("Output:\n", result.stdout)
        print("Error (if any):\n", result.stderr)
        
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running the command: {e}")



def main():
    while(True):
        try:
            print("Welcome to the main menu!\nWhat would you like to do?")
            print("1. User registeration and login \
                \n2. Expense Tracking \
                \n3. Budget Setting \
                \n4. Visualizing Spending Patterns\
                \n5. Setting Financial Goals \
                \n6. Categorizing Transactions \
                \n7. Setting Notifications and Alerts \
                \n8. Receiving Budget Alerts  \
                \n9. Exit")
            choice = int(input("Enter your choice: "))

            if choice<1 or choice>9:
                raise ValueError("Invalid choice. Please enter a valid choice.")
            if choice==1:
                #call group 1's main
                pass
            elif choice==2:
                #call group 2's main
                pass
            elif choice==3:
                run_main_module()
            elif choice==4:
                #call group 4's main
                pass
            elif choice==5:
                #call group 5's main
                pass
            elif choice==6:
                #call group 6's main
                pass
            elif choice==7:
                #call group 7's main
                pass
            elif choice==8:
                #call group 8's main
                pass
            elif choice==9:
                break
            
        except ValueError as e:
            print(f"{e}")

if __name__ == "__main__":
    main()


