from datetime import datetime, timedelta
from src import UserNotLoggedInError , BudgetAlreadyExistsError , Budget , is_user_logged_in , budget_already_exists , DBHelper
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
        print(result)

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
        
    def update_budget(self):
        budget_id = input("Enter the budget ID to update: ")
        if budget_id not in self.budgets:
            print("No budget found with this ID.")
            return
        amount = input("Enter new amount (or leave blank to keep current): ")
        start_date = input("Enter new start date (YYYY-MM-DD) (or leave blank to keep current): ")
        end_date = input("Enter new end date (YYYY-MM-DD) (or leave blank to keep current): ")
        try:
            if amount:
                amount = float(amount)
                if amount <= 0:
                    raise ValueError("Amount must be greater than zero.")
                self.budgets[budget_id].amount = amount
            if start_date:
                self._validate_date(start_date)
                self.budgets[budget_id].start_date = datetime.strptime(start_date, '%Y-%m-%d')
            if end_date:
                self._validate_date(end_date)
                if datetime.strptime(end_date, '%Y-%m-%d') <= self.budgets[budget_id].start_date:
                    raise ValueError("End date must be after the start date.")
                self.budgets[budget_id].end_date = datetime.strptime(end_date, '%Y-%m-%d')
            print(f"Budget with ID {budget_id} updated successfully.")
        except ValueError as e:
            print(f"Error: {e}")

    def delete_budget(self):
        budget_id = input("Enter the budget ID to delete: ")
        if budget_id not in self.budgets:
            print("No budget found with this ID.")
            return
        del self.budgets[budget_id]
        print(f"Budget with ID {budget_id} deleted successfully.")
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




