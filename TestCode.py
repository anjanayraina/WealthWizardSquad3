from datetime import datetime, timedelta


class Budget:


    def __init__(self, budget_id, user_id, category, amount, start_date, end_date):
        self.budget_id = budget_id
        self.user_id = user_id
        self.category = category
        self.amount = amount
        self.start_date = datetime.strptime(start_date, '%Y-%m-%d')
        self.end_date = datetime.strptime(end_date, '%Y-%m-%d')
    def __repr__(self):
        return (f"Budget(budget_id={self.budget_id}, user_id={self.user_id}, category={self.category}, "
                f"amount={self.amount}, start_date={self.start_date.strftime('%Y-%m-%d')}, "
                f"end_date={self.end_date.strftime('%Y-%m-%d')})")


class BudgetManager:
    def __init__(self):
        self.budgets = {}

    def create_budget(self):
        budget_id = input("Enter budget ID: ")
        user_id = input("Enter user ID: ")
        category = input("Enter budget category: ")
        amount = float(input("Enter budget amount: "))
        start_date = input("Enter start date (YYYY-MM-DD): ")
        end_date = input("Enter end date (YYYY-MM-DD): ")

        try:
            self._validate_budget(budget_id, amount, start_date, end_date)
            self.budgets[budget_id] = Budget(budget_id, user_id, category, amount, start_date, end_date)
            print(f"Budget with ID {budget_id} created successfully.")
        except ValueError as e:
            print(f"Error: {e}")

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

    def _validate_date(self, date_str):
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            raise ValueError("Date format must be YYYY-MM-DD.")


# Example usage
if __name__ == "__main__":
    manager = BudgetManager()

    while True:
        print("\nBudget Manager")
        print("1. Create Budget")
        print("2. Update Budget")
        print("3. Delete Budget")
        print("4. Retrieve Budget")
        print("5. List Budgets")
        print("6. Raise Alert")
        print("7. Exit")
        choice = input("Enter your choice: ")

        if choice == '1':
            manager.create_budget()
        elif choice == '2':
            manager.update_budget()
        elif choice == '3':
            manager.delete_budget()
        elif choice == '4':
            manager.get_budget()
        elif choice == '5':
            manager.list_budgets()
        elif choice == '6':
            manager.raise_alert()
        elif choice == '7':
            break
        else:
            print("Invalid choice. Please try again.")