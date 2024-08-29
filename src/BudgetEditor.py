import os
import requests as re
from datetime import datetime
from dotenv import load_dotenv
from .SparkManager import SparkManager
from pyspark.sql.functions import when, col, lit, to_date
from .utils import is_user_logged_in, budget_already_exists
from .exceptions import UserNoAccessError, UserNotLoggedInError, BudgetAlreadyExistsError

CURRENCY_EXCHANGE_URL = "https://latest.currency-api.pages.dev/v1/currencies/"

load_dotenv()

class BudgetEditor:
    """
        ### Class to manage edit operation in Budget using Spark
        - Loads the SparkManager instance with credentials from `.env` file
        - Contains transformation example on Budget Data Frame
    """
    def __init__(self):
        """
        Initializes a `SparkManager` instance with credentials from `.env` file  
        Also loads the `budget_df` Spark DataFrame object.
        """
        self.sparkManager = SparkManager(
            user=os.getenv("USER_SYSTEM"),
            password=os.getenv("PASSWORD"),
            host=os.getenv("HOST"),
            port=os.getenv("PORT"),
            sid=os.getenv("SID")
        )
        self.read_budget_df()

    def read_budget_df(self):
        """
        Loads the `budgets` Oracle SQL Table using JDBC driver as a Pyspark Dataframe  
        """
        self.budget_df = self.sparkManager.read_data("budgets")
        self.budget_df.show()

    def check_for_budget_id(self,budget_id):
        """
        Checks if the budget record for the given `budget_id` exists
        """
        new_df = self.budget_df.filter(self.budget_df["budget_id"] == budget_id)
        if new_df.count() > 0:
            return True
        else: return False

    def _validate_date(self, date_str):
        """
        Validates if date is in the proper format
        """
        try:
            datetime.strptime(date_str, '%d-%m-%Y')
        except ValueError:
            raise ValueError("Date format must be DD-MM-YYYY.")

    def edit_budget_df(self, budget_id, user_id, category, amount, start_date, end_date):
        """
            ### Method to edit the budget dataframe (doesn't write back to Oracle DB!)
            #### Arguments
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
        
        if not budget_id: # To check if the budget_id is empty or not
            raise ValueError("Budget ID cannot be empty.")

        try: # Check if the `budget_id` is a valid integer or not
            budget_id = int(budget_id)
        except ValueError:
            print("Budget ID must be an Integer!")
            raise
            
        if not self.check_for_budget_id(budget_id): # if a budget exists
            print("No budget found with this ID.")
            return

        if not user_id: # if `user_id` is empty or not
            raise ValueError("User ID cannot be empty.")

        if not category: # if `category` is empty or not
            raise ValueError("Budget category cannot be empty.")

        if not amount: # if `amount` is empty or not
            raise ValueError("Budget amount cannot be empty.")

        try: # if `amount` isn't convertible to float
            if amount:
                amount = float(amount)
                if amount <= 0:
                    raise ValueError("Amount must be greater than zero.")
        except ValueError as e:
            print(f"Error: {e}")
        
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

        # Filter the DataFrame for the specific budget_id and update the values
        self.budget_df = self.budget_df.withColumn(
            "user_id", when(col("budget_id") == budget_id, lit(user_id)).otherwise(col("user_id"))
        ).withColumn(
            "category", when(col("budget_id") == budget_id, lit(category)).otherwise(col("category"))
        ).withColumn(
            "amount", when(col("budget_id") == budget_id, lit(amount)).otherwise(col("amount"))
        ).withColumn(
            "start_date", when(col("budget_id") == budget_id, to_date(lit(start_date), 'dd-MM-yyyy')).otherwise(col("start_date"))
        ).withColumn(
            "end_date", when(col("budget_id") == budget_id, to_date(lit(end_date), 'dd-MM-yyyy')).otherwise(col("end_date"))
        )

        # Display the edited Budget DataFrame
        self.budget_df.show()

    def write_back_db(self):
        """
            Writes back to Oracle SQL DB
            [ ] Yet to be implemented
        """
        pass

    def currency_exchange(self,source_curr="inr",target_curr = "usd"):
        """
            An example currency converter for each budget in the DataFrame
            - `source_curr` represents the currency format in which `amount` is present in the Dataframe currently
                - Defaults to `INR`
            - `target_curr` represents the exchange currency
                - Defaults to `USD`
        """
        response = re.get(f"{CURRENCY_EXCHANGE_URL}{source_curr}.json").json()
        currency_df = self.budget_df.withColumn(f"{target_curr.upper()} Amount",self.budget_df["amount"]*response[source_curr][target_curr])
        currency_df.show()

    def close_session(self):
        """
            Close Spark session
        """
        self.sparkManager.stop_session()


if __name__ == "__main__":
    try:
        budget_editor = BudgetEditor()
        budget_editor.read_budget_df()
        budget_editor.edit_budget_df(1,"101",
                                    "Groceries",3030,"20-08-2024","20-09-2024")
        budget_editor.currency_exchange()
    except Exception as e:
        raise
    finally:
        budget_editor.close_session()

# [x] Develop logic to handle updates in budget data.
    # - edit_budget_df function
# [x] Implement data validation and transformation for edits.
    # [x] - data validation happens in edit_budget_df
    # [x] - look for currency conversion in transformation (used for printing alone)
# [ ] Write scripts to update the data warehouse with modified budget entries.
    # [ ] - Write back to Oracle DB (if required)
# [ ] Create unit tests for data processing logic.

# TO DO:
# ShutdownHookManager ERROR is produced while closing Spark session