import unittest
from datetime import datetime

from src.BudgetManager import  BudgetManager
from src.DBHelper import  DBHelper
class TestBudgetManager(unittest.TestCase):
    def setUp(self):
        self.manager = BudgetManager()

    def test_create_budget_valid_input(self):
        budget_id = 'B001'
        user_id = 'U001'
        category = 'Groceries'
        amount = 500.0
        start_date = '01-08-2024'
        end_date = '31-08-2024'

        self.manager.create_budget(budget_id, user_id, category, amount, start_date, end_date)

        self.assertIn(budget_id, self.manager.budgets)
        budget = self.manager.budgets[budget_id]
        self.assertEqual(budget.user_id, user_id)
        self.assertEqual(budget.category, category)
        self.assertEqual(budget.amount, 500.0)
        self.assertEqual(budget.start_date, datetime(2024, 8, 1))
        self.assertEqual(budget.end_date, datetime(2024, 8, 31))

        query = "SELECT * FROM budgets WHERE budget_id = :1"
        result = self.db_helper.execute_query(query, (budget_id,))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], budget_id)


    def test_create_budget_missing_budget_id(self):
        with self.assertRaises(ValueError) as context:
            self.manager.create_budget('', 'U001', 'Groceries', '500.0', '01-08-2024', '31-08-2024')
        self.assertEqual(str(context.exception), "Budget ID cannot be empty.")

    def test_create_budget_missing_user_id(self):
        with self.assertRaises(ValueError) as context:
            self.manager.create_budget('B001', '', 'Groceries', '500.0', '2024-08-01', '2024-08-31')
        self.assertEqual(str(context.exception), "User ID cannot be empty.")

    def test_create_budget_missing_category(self):
        with self.assertRaises(ValueError) as context:
            self.manager.create_budget('B001', 'U001', '', '500.0', '2024-08-01', '2024-08-31')
        self.assertEqual(str(context.exception), "Budget category cannot be empty.")

    def test_create_budget_missing_amount(self):
        with self.assertRaises(ValueError) as context:
            self.manager.create_budget('B001', 'U001', 'Groceries', '', '2024-08-01', '2024-08-31')
        self.assertEqual(str(context.exception), "Budget amount cannot be empty.")




if __name__ == '__main__':
    unittest.main()
