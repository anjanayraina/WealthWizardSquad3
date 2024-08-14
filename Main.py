# main.py

import sys
from BudgetManager import BudgetManager


def main():
    manager = BudgetManager()

    while True:
        print("Budget Management CLI")
        print("1. Create a new budget")
        print("2. Edit an existing budget")
        print("3. Delete a budget")
        print("4. View all active budgets")
        print("5. Exit")

        choice = input("Select an option (1-5): ")

        if choice == '1':
            manager.create_budget()
        elif choice == '2':
            manager.edit_budget()
        elif choice == '3':
            manager.delete_budget()
        elif choice == '4':
            manager.view_budgets()
        elif choice == '5':
            print("Exiting the Budget Management CLI.")
            sys.exit(0)
        else:
            print("Invalid option. Please choose a number between 1 and 5.\n")


if __name__ == "__main__":
    main()