# main.py

import sys
from BudgetManager import BudgetManager


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