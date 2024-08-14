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
        print("5. View all Budgets")
        print("6. Raise Alert")
        print("7. Exit")
        choice = input("Enter your choice: ")
        
        if choice == '1':
            budget_id = input("Enter budget ID: ")
            user_id = input("Enter user ID: ")
            category = input("Enter budget category: ")
            amount = float(input("Enter budget amount: "))
            start_date = input("Enter start date (DD-MM-YYYY): ")
            end_date = input("Enter end date (DD-MM-YYYY): ")
            manager.create_budget(budget_id, user_id, category, amount, start_date, end_date)
        
        elif choice == '2':
            budget_id = input("Enter the budget ID to update: ")
            amount = input("Enter new amount (or leave blank to keep current): ")
            start_date = input("Enter new start date (DD-MM-YYYY) (or leave blank to keep current): ")
            end_date = input("Enter new end date (DD-MM-YYYY) (or leave blank to keep current): ")
            amount = float(amount) if amount else None
            manager.update_budget(budget_id, amount, start_date if start_date else None, end_date if end_date else None)
        
        elif choice == '3':
            budget_id = input("Enter the budget ID to delete: ")
            manager.delete_budget(budget_id)
        
        elif choice == '4':
            budget_id = input("Enter the budget ID to retrieve: ")
            manager.get_budget(budget_id)
        
        elif choice == '5':
            manager.View_All_Budgets()
        
        elif choice == '6':
            budget_id = input("Enter the budget ID to check for alerts: ")
            manager.raise_alert(budget_id)
        
        elif choice == '7':
            break
        
        else:
            print("Invalid choice. Please try again.")
