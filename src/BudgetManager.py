from datetime import datetime, timedelta
from .exceptions import UserNotLoggedInError , BudgetAlreadyExistsError
from .Budget import Budget
from .utils import is_user_logged_in , budget_already_exists
from .DBHelper import DBHelper
from prettytable import PrettyTable
from src.BudgetDataProcessor import BudgetDataProcessor
import os
import oracledb
from dotenv import load_dotenv
from src.Budget import Budget
from pyspark.sql.functions import col
from pyspark.sql.functions import col, when, sum as _sum
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
import cx_Oracle
load_dotenv()



print(os.getenv("USER_SYSTEM"))

class BudgetManager:
    def __init__(self):
        self.budgets = {}
        oracledb.init_oracle_client()
        self.db_helper = DBHelper(
            user=os.getenv("USER_SYSTEM"),
            password=os.getenv("PASSWORD"),
            host=os.getenv("HOST"),
            port = os.getenv("PORT"),
            sid=os.getenv("SID")
        )
        self.db_helper.connect()

    def test_connection(self):
        query = "select * from budgets"
        result = self.db_helper.execute_query(query)
        print(f"DB content: {result}")

    def check_for_duplicate_id(self, id):
        query = "SELECT * FROM budgets WHERE budget_id = :1"
        result = self.db_helper.execute_query(query, params=(id,))
        return len(result) > 0

    def process_csv_file(self, csv_file_path):
        processor = BudgetDataProcessor()
        processor.process_and_save(csv_file_path)

    def budget_associated_with_user(self,budget_id,user_id):
        query = "SELECT user_id FROM budgets WHERE budget_id = :1"
        result = self.db_helper.execute_query(query,params=(budget_id,))
        try:
            associated_user_id = result[0][0]
        except IndexError:
            return False
        if associated_user_id == user_id:
            return True

    def check_user_exists(self,user_id):
        query = "SELECT * FROM budgets WHERE user_id = :1"
        result = self.db_helper.execute_query(query,params=(user_id,))
        return len(result) > 0

    def create_budget(self, budget_id, user_id, category, amount, start_date, end_date):
        if not budget_id:
            raise ValueError("Budget ID cannot be empty.")
        if not user_id:
            raise ValueError("User ID cannot be empty.")
        if not category:
            raise ValueError("Budget category cannot be empty.")
        if not amount:
            raise ValueError("Budget amount cannot be empty.")
        if not is_user_logged_in(user_id):
            raise UserNotLoggedInError("User must be logged in to create a budget")

        if self.check_for_duplicate_id(budget_id):
            raise BudgetAlreadyExistsError("Budget already exists, please enter a new Budget")

        try:
            amount = float(amount)
            if amount <= 0:
                raise ValueError("Amount must be greater than zero.")
        except ValueError:
            raise ValueError("Please enter a valid number for the amount.")
        if not start_date:
            raise ValueError("Start date cannot be empty.")
        self._validate_date(start_date)
        if not end_date:
            raise ValueError("End date cannot be empty.")
        self._validate_date(end_date)
        if datetime.strptime(end_date, '%d-%m-%Y') <= datetime.strptime(start_date, '%d-%m-%Y'):
            raise ValueError("End date must be after the start date.")

        query = """
            BEGIN
                create_budget_proc(:1, :2, :3, :4, :5, :6);
            END;
        """
        params = (budget_id, user_id, category, amount, start_date, end_date)
        self.db_helper.execute_query(query, params, commit=True)
        self.budgets[budget_id] = Budget(budget_id, user_id, category, amount, start_date, end_date)

    def _validate_date(self, date_str):
        if isinstance(date_str, datetime):
            date_str = date_str.strftime('%d-%m-%Y')
        elif isinstance(date_str, str):
            try:
                datetime.strptime(date_str, '%d-%m-%Y')
            except ValueError:
                raise ValueError("Date format must be DD-MM-YYYY.")
        else:
            raise ValueError("Invalid date format. Expected string in DD-MM-YYYY format or datetime object.")

    def edit_budget(self, budget_id, user_id, category, amount, start_date, end_date):
        """"
            ### Editing a budget
            Arguments
                - Budget Manager Object
                - `budget_id` - The budget ID whose contents should be edited
                - `user_id` - The User ID associated with the Budget
                - `category` - The Budget category
                - `amount` - Edited amount
                - `start_date` - New start date of the budget (expects in the 'DD/MM/YYYY' form)
                - `end_date` - New end date of the budget
        """
        if not is_user_logged_in(user_id): # to check if the user has logged in or not
            raise UserNotLoggedInError("User must be logged in to create a budget")

        ### budget_id data validations
        if not budget_id: # To check if the budget_id is empty or not
            raise ValueError("Budget ID cannot be empty.")

        try: # Check if the `budget_id` is a valid integer or not
            budget_id = int(budget_id)
        except ValueError:
            print("Budget ID must be an Integer!")
            raise

        if not self.check_for_duplicate_id(budget_id): # if a budget doesn't exists
            raise InvalidDataError("No budget found with this ID.")

        ### user_id Data validation
        if not user_id: # if `user_id` is empty or not
            raise ValueError("User ID cannot be empty.")
        
        # Check if the user_id exists in DB
        if not self.check_user_exists(user_id):
            raise InvalidDataError("User ID provided doesn't exists!")
        
        # Check if the budget_id is associated with user_id
        if not self.budget_associated_with_user(budget_id=budget_id,user_id=user_id):
            raise InvalidDataError("User ID not associated with given budget ID")

        ### category Data validation
        # if `category` is empty or not
        if not category:
            raise ValueError("Budget category cannot be empty.")

        ### amount Data validation
        if not amount: # if `amount` is empty or not
            raise ValueError("Budget amount cannot be empty.")

        try: # if `amount` isn't convertible to float
            if amount:
                amount = float(amount)
                if amount <= 0:
                    raise ValueError("Amount must be greater than zero.")
        except ValueError as e:
            print(f"Error: {e}")

        ### start_date and end_date Data validation
        if not start_date: # if `start_date` is valid or not
            raise ValueError("Start date cannot be empty.")

        if not end_date: # if `end_date` is valid or not
            raise ValueError("End date cannot be empty.")

        try: # If `start_date` and `end_date` are of proper date formats
            self._validate_date(start_date)
            self._validate_date(end_date)
        except Exception as e:
            print(f"Exception occured {e}")

        # if the `end_date` is greater than `start_date`
        if datetime.strptime(end_date, '%d-%m-%Y') <= datetime.strptime(start_date, '%d-%m-%Y'):
            raise ValueError("End date must be after the start date.")

        # Converting `start_date` and `end_date` to datetime objects
        start_date = datetime.strptime(start_date, '%d-%m-%Y')
        end_date = datetime.strptime(end_date,'%d-%m-%Y')

        # Query to call the `edit_budget_proc` procedure
        query = """
            BEGIN
                edit_budget_proc(:1, :2, :3, :4, :5, :6);
            END;
        """
        params = (budget_id, user_id, category, amount, start_date, end_date)

        self.db_helper.execute_query(query, params, commit=True)
        print("Updation successful!")
        # self.budgets[budget_id] = Budget(budget_id, user_id, category, amount, start_date, end_date)


    def delete_budget(self,budget_id):
        #budget_id = input("Enter the budget ID to delete: ")
        #print(self.budgets)
        # if budget_id not in self.budgets:
        #     print("No budget found with this ID.")
        #     return
        # del self.budgets[budget_id]
        # print(f"Budget with ID {budget_id} deleted successfully.")
        query1 = """
            BEGIN
                delete_budget_proc(:1);
            END;
        """


        """Deleting a budget
            Arguments
            - Budget Manager Object
            - `budget_id` - The budget ID whose contents should be edited"""
        if not is_user_logged_in(budget_id): # to check if the user has logged in or not
            raise UserNotLoggedInError("User must be logged in to create a budget")
        
        
        
        try:
        # Check if budget_id is an integer
            budget_id = int(budget_id)
                #raise ValueError("Budget ID must be an integer.")
            if not self.check_for_duplicate_id(budget_id): # if a budget doesn't exists
                print("No budget found with this ID.")
                return    
        
        # SQL query to delete budget by budget_id
            query1 = """
                BEGIN
                    delete_budget_proc(:1);
                END;
                  """
            params = (budget_id,)
        
        # Execute the query and commit the changes
            self.db_helper.execute_query(query1, params, commit=True)
            print(f"Budget with ID {budget_id} deleted successfully.")
        
        # Check if the budget_id exists in self.budgets
            if budget_id in self.budgets:
                del self.budgets[budget_id]
                print(f"Budget with ID {budget_id} deleted successfully.")
            
    
        except ValueError as ve:
            print(f"Input Error: {ve}")
    
        
        except Exception as e:
                # Generic exception handling
            print(f"An error occurred: {e}")

    def get_budget(self):
        budget_id = input("Enter the budget ID to retrieve: ")
        if budget_id not in self.budgets:
            print("No budget found with this ID.")
            return
        print(self.budgets[budget_id])

    def view_all_budgets(self, user_id):
        query = """
            SELECT budget_id, user_id, category, amount, start_date, end_date
            FROM budgets
            WHERE user_id = :user_id
        """
        try:
            result = self.db_helper.execute_query(query, params={'user_id': user_id})
            if not result:
                print(f"No budgets found for user ID {user_id}.")
                print("Let's create a new budget.")
                try:
                    budget_id = input("Enter budget ID: ")
                    category = input("Enter category: ")
                    amount = input("Enter amount: ")
                    start_date = input("Enter start date (DD-MM-YYYY): ")
                    end_date = input("Enter end date (DD-MM-YYYY): ")
                    self.create_budget(budget_id, user_id, category, amount, start_date, end_date)
                    self.view_all_budgets(user_id)
                except Exception as e:
                    print(f"Error: {e}")
            else:
                table = PrettyTable()
                table.field_names = ["Budget Id", "User Id", "Category", "Amount", "Start_date", "End_date"]
                for budget in result:
                    sdate = budget[4].strftime('%d-%m-%Y')
                    edate = budget[5].strftime('%d-%m-%Y')
                    table.add_row([budget[0], budget[1], budget[2], budget[3], sdate, edate])
                print(table)
        except Exception as e:
            print(f"Error: {e}")
        return result



    '''def raise_alert(self):
        budget_id = input("Enter the budget ID to check for alerts: ")
        if budget_id not in self.budgets:
            print("No budget found with this ID.")
            return
        budget = self.budgets[budget_id]
        today = datetime.now()
        if budget.end_date - today <= timedelta(days=7):
            print(f"Alert: Budget ID {budget_id} for category '{budget.category}' is nearing its end date.")
        else:
            print(f"Budget ID {budget_id} is not nearing its end date.")'''
    

    #from here on my part is starting   
    def intialising_variables(self,spark,user,password,budget_dsn):
        self.spark=spark
        self.user=user
        self.password=password
        self.budget_dsn=budget_dsn


    def retrieve_budget(self, budget_id):
        try:
            # Establish a connection to the Oracle database using cx_Oracle
            connection = cx_Oracle.connect(self.user, self.password, self.budget_dsn, mode=cx_Oracle.SYSDBA)
            cursor = connection.cursor()
            print("Connected to budget database.")
        
            # Retrieve budget details for the given budget_id
            print(f"Retrieving budget with ID: {budget_id}")
            cursor.execute(
                "SELECT budget_id, user_id, category, amount, start_date, end_date, comments "
                "FROM budgets WHERE budget_id = :1", [budget_id]
            )
            budget_row = cursor.fetchone()  # Fetch the first row of the result
            cursor.close()
            connection.close()  # Close the database connection

            if budget_row:
                # If a budget is found, create and return a Budget object with the retrieved data
                print(f"Budget retrieved: {budget_row}")
                return Budget(
                    budget_row[0],  # budget_id
                    budget_row[1],  # user_id
                    budget_row[2],  # category
                    budget_row[3],  # amount
                    budget_row[4].strftime('%Y-%m-%d'),  # start_date
                    budget_row[5].strftime('%Y-%m-%d'),  # end_date
                    budget_row[6]  # comments
             )
            else:
                # If no budget is found with the provided ID, return None
                print("No budget found with this ID.")
                return None
        except cx_Oracle.DatabaseError as e:
            # Handle any database-related errors
            print(f"Error connecting to the database: {e}")
            return None

    def retrieve_total_expenses(self, user_id, category_name, start_date, end_date):
        print(f"Retrieving total expenses for user ID: {user_id} and category Name: {category_name} between {start_date} and {end_date}")
        try:
            # Load expenses from the database into a Spark DataFrame using JDBC
            expenses_df = self.spark.read \
            .format("jdbc") \
            .option("url", "jdbc:oracle:thin:@localhost:1521/orcl") \
            .option("dbtable", "expenses") \
            .option("user", self.user) \
            .option("password", self.password) \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .option("fetchsize", "500") \
            .option("connectTimeout", "60000") \
            .option("socketTimeout", "60000") \
            .load()

            # Filter the expenses DataFrame based on user_id, category_name, and the date range
            filtered_expenses_df = expenses_df.filter(
                (col("user_id") == user_id) &
                (col("category_name") == category_name) &
                (col("expense_date").between(start_date, end_date))
            )
            # Calculate the total expenses for the filtered data
            total_expenses = filtered_expenses_df.agg(_sum("amount").alias("total_expenses")).collect()[0]["total_expenses"]
            print(f"Total expenses retrieved: {total_expenses}")
            return total_expenses
        except Exception as e:
            # Handle any errors during the expense retrieval process
            print(f"Error retrieving expenses: {e}")
            return None

    def compare_budget_and_expenses(self, budget_id):
        print(f"Comparing budget and expenses for Budget ID: {budget_id}")
        budget = self.retrieve_budget(budget_id)  # Retrieve the budget details
        if not budget:
            # If the budget details are not available, print a message and return
            print("No budget details available.")
            return

        # Retrieve the total expenses for the budget's user, category, and date range
        total_expenses = self.retrieve_total_expenses(budget.user_id, budget.category, budget.start_date, budget.end_date)
        if total_expenses is None:
            # If there is an error retrieving expenses, print a message and return
            print("Error retrieving expenses.")
            return

        print(f"Current budget comment: {budget.comment}")
        alert_message = budget.check_budget(total_expenses)  # Generate an alert message based on the budget status
        print(f"Alert message: {alert_message}")

        # Depending on the user's notification method, send the alert message
        if budget.notification_method == 'console':
            print(alert_message)  # Print the alert message to the console
        elif budget.notification_method == 'email':
            print(f"Sending email alert: {alert_message}")  # Simulate sending an email alert

    def process_budget_data(self,spark,user,password,budget_dsn):
        #initalizing all the variables first by doing a function call and bringing them from the main.py file
        self.intialising_variables(spark,user,password,budget_dsn)

        print("Starting Spark job for budget data processing.")

        # Define the schema for the budgets DataFrame
        budget_schema = ["budget_id", "user_id", "category", "amount", "start_date", "end_date", "comments"]

        # Load the budgets from the database into a Spark DataFrame using JDBC
        budget_df = self.spark.read \
        .format("jdbc") \
        .option("url", "jdbc:oracle:thin:@localhost:1521/orcl") \
        .option("dbtable", "budgets") \
        .option("user", self.user) \
        .option("password", self.password) \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .option("fetchsize", "500") \
        .option("connectTimeout", "60000") \
        .option("socketTimeout", "60000") \
        .load()
        print("Budgets DataFrame loaded from the database.")
        budget_df.show()

        # Load expenses from the database into a Spark DataFrame using JDBC
        expenses_df = self.spark.read \
            .format("jdbc") \
            .option("url", "jdbc:oracle:thin:@localhost:1521/orcl") \
            .option("dbtable", "expenses") \
            .option("user", self.user) \
            .option("password", self.password) \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .option("fetchsize", "500") \
            .option("connectTimeout", "60000") \
            .option("socketTimeout", "60000") \
            .load()
        print("Expenses DataFrame loaded from the database.")
        expenses_df.show()

        # Aggregate expenses by user_id and category_name
        aggregated_expenses = expenses_df.groupBy("user_id", "category_name") \
            .agg(_sum("amount").alias("spent"))
        print("Aggregated expenses DataFrame created.")
        aggregated_expenses.show()

        # Join the budgets and aggregated expenses DataFrames based on user_id and category_name
        joined_df = budget_df.alias("budget").join(
            aggregated_expenses.alias("expenses"),
            (col("budget.USER_ID") == col("expenses.user_id")) &
            (col("budget.CATEGORY") == col("expenses.category_name")),
            "left"
            )

        # Select relevant columns from the joined DataFrame
        joined_df = joined_df.select(
        col("budget.BUDGET_ID").alias("BUDGET_ID"),
        col("budget.USER_ID").alias("USER_ID"),
        col("budget.CATEGORY").alias("CATEGORY"),
        col("budget.AMOUNT").alias("BUDGET_AMOUNT"),
        col("expenses.spent").alias("SPENT"),
        col("budget.START_DATE").alias("START_DATE"),
        col("budget.END_DATE").alias("END_DATE"),
        col("budget.COMMENTS").alias("COMMENTS")
        )
        print("Joined DataFrame created by combining budget and expenses data.")
        joined_df.show()

        # Apply business logic to determine the budget status based on spent amount
        result_df = joined_df.withColumn(
        "status",
        when(col("SPENT").isNull(), "Under Control")
        .when(col("SPENT") >= col("BUDGET_AMOUNT"), "Exceeded")
        .when(col("SPENT") >= col("BUDGET_AMOUNT") * 0.9, "Approaching")
        .otherwise("Under Control")
        )
        print("Final DataFrame with budget status calculated.")
        result_df.show()

        # Further processing or saving the result_df as required
        print("Completed Spark job for budget data processing.")

        # Ensure consistent data types for columns used in Plotly Express visualizations
        result_df = result_df.withColumn("BUDGET_AMOUNT", col("BUDGET_AMOUNT").cast("double"))
        result_df = result_df.withColumn("SPENT", col("SPENT").cast("double"))
        result_df = result_df.withColumn("CATEGORY", col("CATEGORY").cast("string"))

        # Convert the result DataFrame to a Pandas DataFrame for use in Dash
        result_pd_df = result_df.toPandas()

        # Convert None/NaN values to 0 for plotting purposes
        result_pd_df['SPENT'] = result_pd_df['SPENT'].fillna(0)

        return result_pd_df

    def create_dash_app(self, processed_data):
        # Initialize a Dash app instance
        app = dash.Dash(__name__)

        # Ensure column names are in uppercase for consistency
        processed_data.columns = [col.upper() for col in processed_data.columns]

        # Create the layout for the Dash app
        app.layout = html.Div([
            html.H1("Budget vs Expenses Dashboard"),
    
            # Create a pie chart for category-wise spending breakdown
            dcc.Graph(
                id='category-breakdown',
                figure=px.pie(processed_data, names='CATEGORY', values='SPENT', title='Spending Breakdown by Category')
            ),
    
            # Create a bar chart for budget vs. spent comparison
            dcc.Graph(
                id='budget-vs-spent',
                figure=px.bar(processed_data, x='CATEGORY', y=['BUDGET_AMOUNT', 'SPENT'], barmode='group',
                            title='Budget vs. Spent Amount by Category')
            ),
    
            # Create a scatter plot for the spending trend over time
            dcc.Graph(
                id='spending-trend',
                figure=px.scatter(processed_data, x='START_DATE', y='SPENT', color='CATEGORY',
                            title='Spending Trend Over Time')
            )
        ])

        print("Dash app layout created.")

        # Run the Dash app on the specified host and port
        return app





