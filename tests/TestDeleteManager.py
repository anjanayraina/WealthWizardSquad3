import unittest
from datetime import datetime
from src.BudgetManager import BudgetManager
from src.DBHelper import DBHelper
import os
import random as rand
from dotenv import load_dotenv
load_dotenv()


class TestDeleteManager(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db_helper = DBHelper(
            user=os.getenv("USER_SYSTEM"),
            password=os.getenv("PASSWORD"),
            host=os.getenv("HOST"),
            port=os.getenv("PORT"),
            sid=os.getenv("SID")
        )
        cls.db_helper.connect()

    @classmethod
    def tearDownClass(cls):
        cls.db_helper.close()

    def setUp(self):
        self.manager = BudgetManager()

    def test_delete_budget(self):
        try:
        # Step 1: Insert a budget to be deleted or provide the ID of an already present budget
            budget_id = input("What Budget ID do you want to delete?")

        # Try to convert the budget_id to an integer
            budget_id = int(budget_id)
        
        # Query to check if the budget exists
            query = "SELECT * FROM budgets WHERE budget_id = :1"
            result = self.db_helper.execute_query(query, params=(budget_id,))
        
            print('The item to be deleted =', result)
        
        # Step 2: Delete the budget
            if len(result) != 0:
                self.manager.delete_budget(budget_id)
                print(f"Budget ID {budget_id} has been deleted.")
            else:
                print(f"Budget ID {budget_id} doesn't exist")
        
        # Step 3: Verify the budget was deleted
            query = "SELECT * FROM budgets WHERE budget_id = :1"
            result = self.db_helper.execute_query(query, params=(budget_id,))
            self.assertEqual(len(result), 0)  # No records should be found with the given budget_id
    
        except ValueError:
        # Handle the case where the input is not an integer
            print("Error: Budget ID must be an integer.")
    

    
if __name__ == '__main__':
    unittest.main()