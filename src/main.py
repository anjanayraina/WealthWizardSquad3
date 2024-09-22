from src.BudgetManager import BudgetManager
from src.BudgetManagerspark import BudgetManagerSpark
from src.ViewBudgetFilters import ViewBudgetFilters
from src.BudgetEditor import BudgetEditor
from dotenv import load_dotenv
load_dotenv()
import os
from pyspark.sql import SparkSession
import random

if __name__ == "__main__":

    manager = BudgetManager()
    manager.test_connection()
    try:
        while True:
            print("\nBudget Manager")
            print("1. Create Budget")
            print("2. Update Budget")
            print("3. Convert currency")
            print("4. Delete Budget(Quickly deletes if u remember ur budget_id)")
            print("5. Delete Budget of user_id (if u don't remember ur budget_id)")
            print("6. View all Budgets")
            print("7. Visualization")
            print("8. Exit")
            choice = input("Enter your choice: ")

            if choice == '1':
                print("Do you want to:")
                print("1. Add a single budget entry")
                print("2. Add multiple budget entries from a CSV file")
                sub_choice = input("Enter your choice (1 or 2): ")

                if sub_choice == '1':
                    budget_id = random.randint(1,int(1e5))
                    
                    while manager.check_for_duplicate_id(budget_id):
                        budget_id = random.randint(1,int(1e5))
                    budget_id = str(budget_id)
                    
                    print("Budget with ID : ",budget_id , " created.")

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
                try:
                    user_id = input("Enter user ID: ")
                    if not manager.check_user_exists(user_id):
                        print("User doesn't exists!")
                        continue

                    manager.view_all_budgets(user_id)
                    
                    budget_id = input("Enter budget ID: ")
                    if not manager.check_for_duplicate_id(budget_id):
                        print("No budget found with this ID.")
                        continue
                    
                    query = "SELECT * FROM budgets WHERE user_id = :1 AND budget_id=:2"
                    result = manager.db_helper.execute_query(query,params=(user_id,budget_id))
                    if len(result)==0:
                        print("Budget not associated with User!")
                        continue
                    
                    print("\nYou can just press enter if you don't wish to edit the fields\n")
                    print(f"Existing category name:{result[0][2]}")

                    category = input("Edit budget category: ")
                    if not category:
                        category = result[0][2]
                    
                    print(f"Existing budget amount:{result[0][3]}")
                    amount = input("Edit budget amount: ")
                    if not amount:
                        amount = result[0][3]
                    
                    print(f"Existing start date: {result[0][4].strftime("%d-%m-%Y")}")
                    start_date = input("Edit start date (DD-MM-YYYY): ")
                    if not start_date:
                        start_date = result[0][4].strftime("%d-%m-%Y")

                    print(f"Existing end date: {result[0][5].strftime("%d-%m-%Y")}")
                    end_date = input("Edit end date (DD-MM-YYYY): ")
                    if not end_date:
                        end_date = result[0][5].strftime("%d-%m-%Y")

                    manager.edit_budget(budget_id, user_id, category, amount, start_date, end_date)
                except Exception as e:
                    print(e)



            elif choice == '4':
                budget_id = input("Enter budget ID: ")
                manager.delete_budget(budget_id)

            elif choice == '3':
                try:
                    budget_editor = BudgetEditor()
                    budget_editor.read_budget_df()
                    user_id = input("Enter user ID: ")
                    if not manager.check_user_exists(user_id):
                        print("User doesn't exists!")
                        continue
                    
                    manager.view_all_budgets(user_id)

                    budget_id = input("Enter budget ID: ")
                    if not manager.check_for_duplicate_id(budget_id):
                        print("No budget found with this ID.")
                        continue
                    
                    query = "SELECT * FROM budgets WHERE user_id = :1 AND budget_id=:2"
                    result = manager.db_helper.execute_query(query,params=(user_id,budget_id))
                    if len(result)==0:
                        print("Budget not associated with User!")
                        continue

                    print("Supported currency list: USD, INR, AUD, GBP, CHF")
                    curr_type = input("Enter the currency type for conversion: ")
                    budget_editor.currency_exchange(target_curr=curr_type.lower())
                    budget_editor.show_curr_df_for_user(int(budget_id))
                    budget_editor.close_session()
                    
                except Exception as e:
                    print(e)

            elif choice == '6':
                user_id = input("Enter user ID: ")
                manager.view_all_budgets(user_id)
                view_filters = ViewBudgetFilters(user_id)
            elif choice == '7':
                # spark = SparkSession.builder \
                # .appName("Budget Management System") \
                # .config("spark.local.dir", r"C:\Users\HP\Desktop\testingvaibhav") \
                # .getOrCreate()
                spark = SparkSession.builder \
                .appName("Budget Management System") \
                .config("spark.driver.extraClassPath", r"C:\Users\HP\Downloads\ojdbc8.jar") \
                .config("spark.cleaner.referenceTracking.cleanCheckpoints", "false") \
                .getOrCreate()

                #.config("spark.driver.extraClassPath", "C:\Users\HP\Downloads\ojdbc8.jar") \

                # Process the budget data and retrieve the processed DataFrame
                processed_data = manager.process_budget_data(spark)

                # Create a Dash web application using the processed budget data
                dash_app = manager.create_dash_app(processed_data)

                # Run the Dash web server for the application in debug mode
                dash_app.run_server(debug=True,port=5001)

                # Stop the SparkSession after the application has finished running
                spark.stop()
                #break
                #To be executed only once, and that is first time when you are executing the code so that the
                #scheduler is formed in the database and after that there is no need of it as the scheduler would be already there.
                # Manage Scheduler Jobs
                #scheduler = SchedulerManager(spark)
                #scheduler.create_scheduler_job()  # Create the job to schedule the budget check
                #scheduler.drop_scheduler_job()  # Uncomment to drop the job if needed
                #scheduler.close()
                #spark.stop()
            elif choice == '5':
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
