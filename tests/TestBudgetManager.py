import unittest
from datetime import datetime
from src.BudgetManager import BudgetManager
from src.DBHelper import DBHelper
from src.exceptions import UserNotLoggedInError, BudgetAlreadyExistsError
import os
import random as rand
from unittest.mock import patch
from dotenv import load_dotenv
import oracledb
load_dotenv()


class TestBudgetManager(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        #oracledb.init_oracle_client()
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

    def tearDown(self):
        self.db_helper.execute_query("truncate table budgets", commit=True)

    def test_create_budget_valid_input(self):
        budget_id = rand.randint(0, 100000)
        user_id = 2000
        category = 'Groceries'
        amount = 500
        start_date = '01-08-2024'
        end_date = '21-08-2024'

        with patch('src.is_user_logged_in', return_value=True):
            with patch('src.budget_already_exists', return_value=False):
                self.manager.create_budget(budget_id, user_id, category, amount, start_date, end_date)

        query = "SELECT * FROM budgets WHERE budget_id = :1"
        result = self.db_helper.execute_query(query, params=(budget_id,))
        self.assertEqual(len(result), 1)
        budget_record = result[0]
        self.assertEqual(int(budget_record[0]), int(budget_id))
        self.assertEqual(int(budget_record[1]), int(user_id))
        self.assertEqual(budget_record[2], category)
        self.assertEqual(budget_record[3], amount)
        self.assertEqual(budget_record[4].date(),
                         datetime.strptime(start_date, '%d-%m-%Y').date())
        self.assertEqual(budget_record[5].date(), datetime.strptime(end_date, '%d-%m-%Y').date())

    def test_create_budget_missing_user_id(self):
        budget_id = rand.randint(0, 100000)
        category = 'Groceries'
        amount = 500
        start_date = '01-08-2024'
        end_date = '21-08-2024'

        with patch('src.is_user_logged_in', return_value=True):
            with self.assertRaises(ValueError) as context:
                self.manager.create_budget(budget_id, None, category, amount, start_date, end_date)
            self.assertEqual(str(context.exception), "User ID cannot be empty.")

    def test_create_budget_missing_amount(self):
        budget_id = rand.randint(0, 100000)
        user_id = 2000
        category = 'Groceries'
        start_date = '01-08-2024'
        end_date = '21-08-2024'

        with patch('src.is_user_logged_in', return_value=True):
            with self.assertRaises(ValueError) as context:
                self.manager.create_budget(budget_id, user_id, category, None, start_date, end_date)
            self.assertEqual(str(context.exception), "Budget amount cannot be empty.")

    def test_create_budget_invalid_amount(self):
        budget_id = rand.randint(0, 100000)
        user_id = 2000
        category = 'Groceries'
        amount = -500  # Invalid amount
        start_date = '01-08-2024'
        end_date = '21-08-2024'

        with patch('src.utils.is_user_logged_in', return_value=True):
            with self.assertRaises(ValueError) as context:
                self.manager.create_budget(budget_id, user_id, category, amount, start_date, end_date)
            self.assertEqual(str(context.exception), "Please enter a valid number for the amount.")

    def test_create_budget_missing_start_date(self):
        budget_id = rand.randint(0, 100000)
        user_id = 2000
        category = 'Groceries'
        amount = 500
        end_date = '21-08-2024'

        with patch('src.is_user_logged_in', return_value=True):
            with self.assertRaises(ValueError) as context:
                self.manager.create_budget(budget_id, user_id, category, amount, None, end_date)
            self.assertEqual(str(context.exception), "Start date cannot be empty.")

    def test_create_budget_missing_end_date(self):
        budget_id = rand.randint(0, 100000)
        user_id = 2000
        category = 'Groceries'
        amount = 500
        start_date = '01-08-2024'

        with patch('src.is_user_logged_in', return_value=True):
            with self.assertRaises(ValueError) as context:
                self.manager.create_budget(budget_id, user_id, category, amount, start_date, None)
            self.assertEqual(str(context.exception), "End date cannot be empty.")

    def test_create_budget_duplicate_category(self):
        budget_id_1 = rand.randint(0, 100000)
        budget_id_2 = budget_id_1
        user_id = 2000
        category = 'Groceries'
        amount = 500
        start_date = '01-08-2024'
        end_date = '21-08-2024'

        with patch('src.is_user_logged_in', return_value=True):
            with patch('src.budget_already_exists', side_effect=[False, True]):
                self.manager.create_budget(budget_id_1, user_id, category, amount, start_date, end_date)
                with self.assertRaises(BudgetAlreadyExistsError) as context:
                    self.manager.create_budget(budget_id_2, user_id, category, amount, start_date, end_date)
                self.assertEqual(str(context.exception), "Budget already exists, please enter a new Budget")

    def test_create_budget_invalid_dates(self):
        budget_id = rand.randint(0, 100000)
        user_id = 2000
        category = 'Groceries'
        amount = 500
        start_date = '21-08-2024'
        end_date = '01-08-2024'

        with patch('src.is_user_logged_in', return_value=True):
            with patch('src.budget_already_exists', return_value=False):
                with self.assertRaises(ValueError) as context:
                    self.manager.create_budget(budget_id, user_id, category, amount, start_date, end_date)
                self.assertEqual(str(context.exception), "End date must be after the start date.")


    def test_list_budgets(self):
        budgets = [
            (rand.randint(0, 100000), 2000, 'Groceries', 500, '01-08-2024', '21-08-2024'),
            (rand.randint(0, 100000), 2001, 'Utilities', 300, '15-08-2024', '30-08-2024'),
        ]
        for budget in budgets:
            self.manager.create_budget(*budget)
        self.manager.view_all_budgets(2000)
        query = "SELECT * FROM budgets WHERE user_id = :1"
        result = self.db_helper.execute_query(query, params=(2000,))
        self.assertGreater(len(result), 0, "No budgets found for user_id 2000.")
        for budget in result:
            self.assertEqual(int(budget[1]), 2000)
           


if __name__ == '__main__':
    unittest.main()
