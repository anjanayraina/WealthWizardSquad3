from src.BudgetManager import BudgetManager
from src.BudgetManagerspark import BudgetManagerSpark
from dotenv import load_dotenv
load_dotenv()
import os
from pyspark.sql import SparkSession


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
                user_id = input("Enter user ID: ")
                if not manager.check_user_exists(user_id):
                    print("User doesn't exists!")
                    continue
                
                budget_id = input("Enter budget ID: ")
                if not manager.check_for_duplicate_id(budget_id):
                    print("No budget found with this ID.")
                    continue
                
                query = "SELECT * FROM budgets WHERE user_id = :1 AND budget_id=:2"
                result = manager.db_helper.execute_query(query,params=(user_id,budget_id))
                if len(result)==0:
                    print("Budget not associated with User!")
                    continue
                
                print(f"Existing category name:{result[0][2]}")
                category = input("Edit budget category: ")
                if not category:
                    category = result[0][2]
                
                print(f"Existing budget amount:{result[0][3]}")
                amount = input("Edit budget amount: ")
                if not amount:
                    amount = result[0][3]
                
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
                #manager.raise_alert()
                # Set Python paths to ensure compatibility
                os.environ['PYSPARK_PYTHON'] = 'C:/Users/91991/AppData/Local/Programs/Python/Python38/python.exe'
                os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:/Users/91991/AppData/Local/Programs/Python/Python38/python.exe'
                spark = SparkSession.builder \
                .appName("Budget Management System") \
                .config("spark.local.dir", "C:/Users/91991/Desktop/testing") \
                .config("spark.driver.extraClassPath", "C:/Users/91991/Downloads/WINDOWS.X64_213000_db_home/jdbc/lib/ojdbc8.jar") \
                .getOrCreate()

                # Database connection details
                user = "sys as sysdba"  # Specify the user, in this case, SYSDBA for administrative tasks
                password = "vaibhav1234"  # Password for the database user
                dsn = "localhost/orcl"  # Data Source Name (DSN) specifying the Oracle database instance

                # Initialize the BudgetManager with SparkSession and database connection details
                budget_manager = BudgetManager()

                # Process the budget data and retrieve the processed DataFrame
                processed_data = budget_manager.process_budget_data(spark, user, password,dsn)

                # Create a Dash web application using the processed budget data
                dash_app = budget_manager.create_dash_app(processed_data)

                # Run the Dash web server for the application in debug mode
                dash_app.run_server(debug=True)

                # Stop the SparkSession after the application has finished running
                spark.stop()
                break
                #To be executed only once, and that is first time when you are executing the code so that the
                #scheduler is formed in the database and after that there is no need of it as the scheduler would be already there.
                # Manage Scheduler Jobs
                #scheduler = SchedulerManager(spark,dsn, user, password)
                #scheduler.create_scheduler_job()  # Create the job to schedule the budget check
                #scheduler.drop_scheduler_job()  # Uncomment to drop the job if needed
                #scheduler.close()
                #spark.stop()
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
