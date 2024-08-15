import unittest
from datetime import datetime

from src.BudgetManager import  BudgetManager
class TestBudgetManager(unittest.TestCase):
    def setUp(self):
        self.manager = BudgetManager()

    def test_create_budget_valid_input(self):
        self.manager.create_budget('B001', 'U001', 'Groceries', '500.0', '01-08-2024', '31-08-2024')
        self.assertIn('B001', self.manager.budgets)
        budget = self.manager.budgets['B001']
        self.assertEqual(budget.user_id, 'U001')
        self.assertEqual(budget.category, 'Groceries')
        self.assertEqual(budget.amount, 500.0)
        self.assertEqual(budget.start_date, datetime(2024, 8, 1))
        self.assertEqual(budget.end_date, datetime(2024, 8, 31))


if __name__ == '__main__':
    unittest.main()
