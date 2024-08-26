import unittest
from datetime import datetime
from src.BudgetManager import BudgetManager
from src.DBHelper import DBHelper
import os
import oracledb
import random as rand
from dotenv import load_dotenv
load_dotenv()


class TestBudgetManager(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Initialize DBHelper for direct database queries
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
        # Initialize BudgetManager
        self.manager = BudgetManager()

    def tearDown(self):
        self.db_helper.execute_query("truncate table budgets", commit=True)

    def test_create_budget_valid_input(self):
        budget_id = rand.randint(0 , 100000)
        user_id = 2000
        category = 'Groceries'
        amount = 500
        start_date = '01-08-2024'
        end_date = '21-08-2024'
        self.manager.create_budget(budget_id, user_id, category, amount, start_date, end_date)
        query = "SELECT * FROM budgets WHERE budget_id = :1"
        result = self.db_helper.execute_query(query, params=(budget_id,))
        self.assertEqual(len(result), 1)
        budget_record = result[0]
        self.assertEqual(budget_record[0], budget_id)
        self.assertEqual(budget_record[1], user_id)
        self.assertEqual(budget_record[2], category)
        self.assertEqual(budget_record[3], amount)
        self.assertEqual(budget_record[4].date(),
                         datetime.strptime(start_date, '%d-%m-%Y').date())
        self.assertEqual(budget_record[5].date(), datetime.strptime(end_date, '%d-%m-%Y').date())
        
        
    def test_list_budgets(self):
        budgets = [
            (rand.randint(0, 100000), 2000, 'Groceries', 500, '01-08-2024', '21-08-2024'),
            (rand.randint(0, 100000), 2001, 'Utilities', 300, '15-08-2024', '30-08-2024'),
        ]

        for budget in budgets:
            self.manager.create_budget(*budget)
        res = self.manager.view_all_budgets(2000)
        query = "SELECT * FROM budgets WHERE user_id = :1"
        result = self.db_helper.execute_query(query, params=(2000,))
        self.assertGreater(len(result), 0, "No budgets found for user_id 2000.")
        for budget in result:
            self.assertEqual(budget[1], 2000)  
            self.assertIn(budget[0], [b[0] for b in budgets if b[1] == 2000]) 


if __name__ == '__main__':
    unittest.main()
