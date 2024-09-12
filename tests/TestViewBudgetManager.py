import unittest
from datetime import datetime
from src.BudgetManager import BudgetManager
from src.DBHelper import DBHelper
import os
import oracledb
import random as rand
from dotenv import load_dotenv
load_dotenv()


class TestViewBudgetManager(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        oracledb.init_oracle_client()
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

    def test_list_budgets(self):
        # Sample budget data
        budgets = [
            (rand.randint(0, 100000), 2000, 'Groceries', 500, '01-08-2024', '21-08-2024'),
            (rand.randint(0, 100000), 2001, 'Utilities', 300, '15-08-2024', '30-08-2024'),
        ]

        # Insert sample budgets
        for budget in budgets:
            self.manager.create_budget(*budget)

        # View all budgets for a specific user
        self.manager.view_all_budgets(2000)

        # Verify the results
        query = "SELECT * FROM budgets WHERE user_id = :1"
        result = self.db_helper.execute_query(query, params=(2000,))

        self.assertGreater(len(result), 0, "No budgets found for user_id 2000.")
        
        # Assert each budget in result
        for budget in result:
            self.assertEqual(budget[1], 2000)
            print(f"Verified budget ID {budget[0]} for user_id {budget[1]}.")


if __name__ == '__main__':
    unittest.main()