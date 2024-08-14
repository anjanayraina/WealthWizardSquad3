from Budget import Budget

class BudgetManager:
    def __init__(self):
        self.budgets = {}

    def create_budget(self):
        category = input("Enter budget category: ")
        amount = float(input("Enter budget amount: "))
        time_period = input("Enter time period (e.g., monthly, yearly): ")
        budget = Budget(category, amount, time_period)
        self.budgets[budget.id] = budget
        print(f"Budget with ID {budget.get_id()} for {budget.get_cateogery()} created successfully!\n")

    def edit_budget(self):
        try:
            budget_id = int(input("Enter the budget ID to edit: "))
            if budget_id in self.budgets:
                amount = float(input("Enter new budget amount: "))
                time_period = input("Enter new time period (e.g., monthly, yearly): ")
                self.budgets[budget_id].amount = amount
                self.budgets[budget_id].time_period = time_period
                print(f"Budget with ID {budget_id} updated successfully!\n")
            else:
                print(f"Budget with ID {budget_id} does not exist.\n")
        except ValueError:
            print("Invalid input. Please enter a valid budget ID.\n")

    def delete_budget(self):
        try:
            budget_id = int(input("Enter the budget ID to delete: "))
            if budget_id in self.budgets:
                del self.budgets[budget_id]
                print(f"Budget with ID {budget_id} deleted successfully!\n")
            else:
                print(f"Budget with ID {budget_id} does not exist.\n")
        except ValueError:
            print("Invalid input. Please enter a valid budget ID.\n")

    def view_budgets(self):
        if not self.budgets:
            print("No budgets available.\n")
            return
        print("Summary of all active budgets:")
        for budget in self.budgets.values():
            print(budget)
        print()