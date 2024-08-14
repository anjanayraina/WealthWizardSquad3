from Budget import Budget
from datetime import datetime, timedelta
from prettytable import PrettyTable

class BudgetManager:
    def __init__(self):
        self.budgets = {}

    def create_budget(self, budget_id, user_id, category, amount, start_date, end_date):
        try:
            self._validate_budget(budget_id, amount, start_date, end_date)
            self.budgets[budget_id] = Budget(budget_id, user_id, category, amount, start_date, end_date)
            print(f"Budget with ID {budget_id} created successfully.")
        except ValueError as e:
            print(f"Error: {e}")

    def update_budget(self, budget_id, amount=None, start_date=None, end_date=None):
        if budget_id not in self.budgets:
            print("No budget found with this ID.")
            return
        
        try:
            if amount is not None:
                if amount <= 0:
                    raise ValueError("Amount must be greater than zero.")
                self.budgets[budget_id].amount = amount
            if start_date is not None:
                self._validate_date(start_date)
                self.budgets[budget_id].start_date = datetime.strptime(start_date, '%d-%m-%Y')
            if end_date is not None:
                self._validate_date(end_date)
                if datetime.strptime(end_date, '%d-%m-%Y') <= self.budgets[budget_id].start_date:
                    raise ValueError("End date must be after the start date.")
                self.budgets[budget_id].end_date = datetime.strptime(end_date, '%d-%m-%Y')
            print(f"Budget with ID {budget_id} updated successfully.")
        except ValueError as e:
            print(f"Error: {e}")

    def delete_budget(self, budget_id):
        if budget_id not in self.budgets:
            print("No budget found with this ID.")
            return
        
        del self.budgets[budget_id]
        print(f"Budget with ID {budget_id} deleted successfully.")

    def get_budget(self, budget_id):
        if budget_id not in self.budgets:
            print("No budget found with this ID.")
            return
        
        print(self.budgets[budget_id])

    def View_All_Budgets(self):
        if not self.budgets:
            print("No budgets set.")
            #API TO CALL CREATE BUDGET.
        else:
            table = PrettyTable()
            table.field_names = ["Budget Id", "Category", "Amount", "Start_data", "End_date"]
            for budget in self.budgets.values():
                table.add_row([budget.budget_id, budget.category, budget.amount, budget.start_date, budget.end_date])
            print(table)

    def raise_alert(self, budget_id):
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
        if datetime.strptime(end_date, '%d-%m-%Y') <= datetime.strptime(start_date, '%d-%m-%Y'):
            raise ValueError("End date must be after the start date.")

    def _validate_date(self, date_str):
        try:
            datetime.strptime(date_str, '%d-%m-%Y')
        except ValueError:
            raise ValueError("Date format must be DD-MM-YYYY.")