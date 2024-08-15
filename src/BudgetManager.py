from .Budget import Budget
from datetime import datetime, timedelta
class BudgetManager:
    def __init__(self):
        self.budgets = {}

    def create_budget(self, budget_id, user_id, category, amount, start_date, end_date):
        try:
            if not budget_id:
                raise ValueError("Budget ID cannot be empty.")

            if not user_id:
                raise ValueError("User ID cannot be empty.")

            if not category:
                raise ValueError("Budget category cannot be empty.")

            if not amount:
                raise ValueError("Budget amount cannot be empty.")
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

            self.budgets[budget_id] = Budget(budget_id, user_id, category, amount, start_date, end_date)
            print(f"Budget with ID {budget_id} created successfully.")

        except ValueError as e:
            print(f"Error: {e}")

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

    def list_budgets(self):
        if not self.budgets:
            print("No budgets set.")
        else:
            for budget in self.budgets.values():
                print(budget)

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



