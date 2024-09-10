from .BudgetManager import BudgetManager
from .BudgetManagerspark import BudgetManagerSpark
from dotenv import load_dotenv
load_dotenv()
import os

if __name__ == "__main__":

    manager = BudgetManager()
    manager.test_connection()
    try:
        while True:
            print("\nBudget Manager")
            print("1. Create Budget")
            print("2. Update Budget")
            print("3. Delete Budget")
            print("4. Retrieve Budget")
            print("5. List Budgets")
            print("6. Raise Alert")
            print("7. Delete Budget of user_id (spark)")
            print("8. Exit")
            choice = input("Enter your choice: ")

            if choice == '1':
                print("Do you want to:")
                print("1. Add a single budget entry")
                print("2. Add multiple budget entries from a CSV file")
                sub_choice = input("Enter your choice (1 or 2): ")

                if sub_choice == '1':
                    budget_id = input("Enter budget ID: ")
                    user_id = input("Enter user ID: ")
                    category = input("Enter budget category: ")
                    amount = input("Enter budget amount: ")
                    start_date = input("Enter start date (DD-MM-YYYY): ")
                    end_date = input("Enter end date (DD-MM-YYYY): ")
                    manager.create_budget(budget_id, user_id, category, amount, start_date, end_date)
                    print("Budget Created!!")
                elif sub_choice == '2':
                    csv_file_path = input("Enter the path to the CSV file: ")
                    manager.process_csv_file(csv_file_path)
                    print("Budget Created!!")
                else:
                    print("Invalid choice. Please try again.")

            elif choice == '2':
                budget_id = input("Enter budget ID: ")
                user_id = input("Enter user ID: ")
                category = input("Edit budget category: ")
                amount = input("Edit budget amount: ")
                start_date = input("Edit start date (DD-MM-YYYY): ")
                end_date = input("Edit end date (DD-MM-YYYY): ")
                manager.edit_budget(budget_id, user_id, category, amount, start_date, end_date)

            elif choice == '3':
                budget_id = input("Enter budget ID: ")
                manager.delete_budget(budget_id)

            elif choice == '4':
                manager.get_budget()

            elif choice == '5':
                user_id = input("Enter user ID: ")
                manager.view_all_budgets(user_id)
            elif choice == '6':
                manager.raise_alert()
            elif choice == '7':
                jdbc_url = "jdbc:oracle:thin:@localhost:1521:orcl"
                driver_path = r"C:\Users\HP\Downloads\ojdbc8.jar"
                oracle_user = os.getenv("USER_SYSTEM")  # default value provided
                oracle_password = os.getenv("PASSWORD")  # default value provided
                
                connection_properties = {
                          "user": oracle_user,
                          "password": oracle_password,
                           "driver": "oracle.jdbc.driver.OracleDriver"
                        }

            # Initialize the BudgetManager
                budget_manager = BudgetManagerSpark("OracleConnection", driver_path, jdbc_url, connection_properties)
    
            # Connect to the database
                budget_manager.connect_to_db()

            # Get user_id from input
                user_id_to_check = input("Enter your user_id to delete associated budgets: ")

            # Fetch and show budgets
                df = budget_manager.fetch_budgets(user_id_to_check)
                if df.count() > 0:
                    print(f"All budgets for user_id '{user_id_to_check}':")
                    df.show()

            # Get unique categories
                    categories = [row['CATEGORY'] for row in df.select("CATEGORY").distinct().collect()]
                    print(f"Categories available for deletion: {categories}")
        
        # Ask for category to delete
                    category_to_delete = input("Enter the category you want to delete or type 'all' to delete all budgets: ")

        # Perform deletion
                    if category_to_delete == 'all' or category_to_delete in categories:
                        budget_manager.delete_budgets(user_id_to_check, category_to_delete)
                    else:
                        print(f"Category '{category_to_delete}' not found.")
                else:
                    print(f"No budgets found for user_id '{user_id_to_check}'.")

       # Show remaining budgets
                budget_manager.show_remaining_budgets()

    # Close resources
                budget_manager.close()

                  

            elif choice == '8':

                break

            else:
                print("Invalid choice. Please try again.")
    except Exception as e:
        print(e)
    finally:
        manager.db_helper.close()
