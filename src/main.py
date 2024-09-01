from BudgetManager import BudgetManager


if __name__ == "__main__":

    manager = BudgetManager()
    manager.test_connection()
    try:
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
                print("Do you want to:")
                print("1. Add a single budget entry")
                print("2. Add multiple budget entries from a CSV file")
                sub_choice = input("Enter your choice (1 or 2): ")

                if sub_choice == '1':
                    budget_id = input("Enter budget ID: ")
                    user_id = input("Enter user ID: ")
                    category = input("Enter budget category: ")
                    amount = input("Enter budget amount: ")
                    start_date = input("Enter start date (DD-MM-YYYY): ")
                    end_date = input("Enter end date (DD-MM-YYYY): ")
                    manager.create_budget(budget_id, user_id, category, amount, start_date, end_date)
                elif sub_choice == '2':
                    csv_file_path = input("Enter the path to the CSV file: ")
                    manager.process_csv_file(csv_file_path)
                else:
                    print("Invalid choice. Please try again.")

            elif choice == '2':
                budget_id = input("Enter budget ID: ")
                user_id = input("Enter user ID: ")
                category = input("Edit budget category: ")
                amount = input("Edit budget amount: ")
                start_date = input("Edit start date (DD-MM-YYYY): ")
                end_date = input("Edit end date (DD-MM-YYYY): ")
                manager.edit_budget(budget_id, user_id, category, amount, start_date, end_date)

            elif choice == '3':
                manager.delete_budget()

            elif choice == '4':
                manager.get_budget()

            elif choice == '5':
                user_id = input("Enter user ID: ")
                manager.view_all_budgets(user_id)
            elif choice == '6':
                manager.raise_alert()

            elif choice == '7':

                break

            else:
                print("Invalid choice. Please try again.")
    except Exception as e:
        print(e)
    finally:
        manager.db_helper.close()
