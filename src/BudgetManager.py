from datetime import datetime, timedelta
from .exceptions import UserNotLoggedInError , BudgetAlreadyExistsError
from .Budget import Budget
from .utils import is_user_logged_in , budget_already_exists
from .DBHelper import DBHelper
from prettytable import PrettyTable
import os
from dotenv import load_dotenv
load_dotenv()

print(os.getenv("USER_SYSTEM"))

class BudgetManager:
    def __init__(self):
        self.budgets = {}
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

    def check_for_duplicate_id(self , id):
        query = "SELECT * FROM budgets WHERE budget_id = :1"
        result = self.db_helper.execute_query(query, params=(id,))
        if len(result) > 0 :
            return True
        return False

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

        if budget_already_exists(budget_id):
            raise BudgetAlreadyExistsError("Budget already exists, please enter a new Budget ")
        
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
        if budget_id in self.budgets:
            raise ValueError("A budget with this ID already exists.")
        query = """
            BEGIN
                create_budget_proc(:1, :2, :3, :4, :5, :6);
            END;
        """
        params = (budget_id, user_id, category, amount, start_date, end_date)
        self.db_helper.execute_query(query, params, commit=True)
        self.budgets[budget_id] = Budget(budget_id, user_id, category, amount, start_date, end_date)
        
    def _validate_date(self, date_str):
        try:
            datetime.strptime(date_str, '%d-%m-%Y')
        except ValueError:
            raise ValueError("Date format must be DD-MM-YYYY.")
        
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
        
        if not budget_id: # To check if the budget_id is empty or not
            raise ValueError("Budget ID cannot be empty.")

        try: # Check if the `budget_id` is a valid integer or not
            budget_id = int(budget_id)
        except ValueError:
            print("Budget ID must be an Integer!")
            raise
            
        if not self.check_for_duplicate_id(budget_id): # if a budget doesn't exists
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
        
        # SQL query to delete budget by budget_id
            query1 = """
                BEGIN
                    delete_budget_proc(:1);
                END;
                  """
            params = (budget_id,)
        
        # Execute the query and commit the changes
            self.db_helper.execute_query(query1, params, commit=True)
        
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
        # to check if the user has logged in or not
        if not is_user_logged_in(user_id): 
            raise UserNotLoggedInError("User must be logged in to create a budget")
        
        #Query
        query = """
            SELECT budget_id, user_id, category, amount, start_date, end_date
            FROM budgets
            WHERE user_id = :user_id
        """
        try:
            result = self.db_helper.execute_query(query, params={'user_id': user_id})
            if not result:
                print(f"No budgets found for user ID {user_id}.")
                #Creating a new budget
                print("Let's create a new budget.")
                try:
                    budget_id = input("Enter budget ID: ")
                    category = input("Enter category: ")
                    amount = input("Enter amount: ")
                    start_date = input("Enter start date (DD-MM-YYYY): ")
                    end_date = input("Enter end date (DD-MM-YYYY): ")
                    #Function call
                    self.create_budget(budget_id, user_id, category, amount, start_date, end_date)
                    self.view_all_budgets(user_id)  
                except Exception as e:
                    print(f"Error: {e}")
            else:
                #Displaying the data using pretty table
                table = PrettyTable()
                table.field_names = ["Budget Id", "User Id", "Category", "Amount", "Start_date", "End_date"]
                for budget in result:
                    table.add_row([budget[0], budget[1], budget[2], budget[3], budget[4], budget[5]])
                print(table)
        except Exception as e:
            print(f"Error: {e}")
        return result


    
    def raise_alert(self):
        budget_id = input("Enter the budget ID to check for alerts: ")
        if budget_id not in self.budgets:
            print("No budget found with this ID.")
            return
        budget = self.budgets[budget_id]
        today = datetime.now()
        if budget.end_date - today <= timedelta(days=7):
            print(f"Alert: Budget ID {budget_id} for category '{budget.category}' is nearing its end date.")
        else:
            print(f"Budget ID {budget_id} is not nearing its end date.")
    def _validate_budget(self, budget_id, amount, start_date, end_date):
        if budget_id in self.budgets:
            raise ValueError("A budget with this ID already exists.")
        if amount <= 0:
            raise ValueError("Amount must be greater than zero.")
        self._validate_date(start_date)
        self._validate_date(end_date)
        if datetime.strptime(end_date, '%Y-%m-%d') <= datetime.strptime(start_date, '%Y-%m-%d'):
            raise ValueError("End date must be after the start date.")




