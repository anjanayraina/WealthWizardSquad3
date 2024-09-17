import unittest
from datetime import datetime
from src.BudgetManager import BudgetManager
from src.DBHelper import DBHelper
from src.exceptions import UserNotLoggedInError, BudgetAlreadyExistsError, InvalidDataError
import os
from random import randint
from dotenv import load_dotenv

load_dotenv()

class TestBudgetEdits(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls): # Runs before starting the test cases
        cls.db_helper = DBHelper(
            user=os.getenv("USER_SYSTEM"),
            password=os.getenv("PASSWORD"),
            host=os.getenv("HOST"),
            port = os.getenv("PORT"),
            sid=os.getenv("SID")
        )
        cls.db_helper.connect()
        cls.manager = BudgetManager()
        cls.indexes = { # Will be used to `index budgets_avlb_query` later
            "budget_id":0,
            "user_id":1,
            "category":2,
            "amount":3,
            "start_date":4,
            "end_date":5
        }

    @classmethod
    def tearDownClass(cls): # Runs when all tests are done
        cls.db_helper.close()
    
    def setUp(self): # Runs every time before starting a new test case input
        self.budgets_avlb_query = self.db_helper.execute_query("SELECT * FROM budgets")
    
    # Passing a valid input to edit budget
    def test_valid_input_edit_budget(self):
        budget_id_index = randint(0,len(self.budgets_avlb_query)-1)
        budget_id = self.budgets_avlb_query[budget_id_index][self.indexes["budget_id"]]
        user_id = self.budgets_avlb_query[budget_id_index][self.indexes["user_id"]]
        category = "Test_"+self.budgets_avlb_query[budget_id_index][self.indexes["category"]]
        amount = randint(0,100)+self.budgets_avlb_query[budget_id_index][self.indexes["amount"]]
        start_date = "01-09-2024"
        end_date = "01-10-2024"
        self.manager.edit_budget(
            budget_id=budget_id,
            user_id=user_id,
            category=category,
            amount=amount,
            start_date=start_date,
            end_date=end_date
        )
        result = self.db_helper.execute_query("SELECT * FROM budgets WHERE budget_id = :1",
                                              params=(budget_id,))
        self.assertEqual(budget_id,result[0][self.indexes["budget_id"]])
        self.assertEqual(user_id,result[0][self.indexes["user_id"]])
        self.assertEqual(category,result[0][self.indexes["category"]])
        self.assertEqual(amount,result[0][self.indexes["amount"]])
        self.assertEqual(datetime.strptime(start_date,"%d-%m-%Y"),
                         result[0][self.indexes["start_date"]])
        self.assertEqual(datetime.strptime(end_date,"%d-%m-%Y"),
                         result[0][self.indexes["end_date"]])



    # Non-existent Budget ID Test
    def test_non_existent_budget_id(self):

        budget_ids_present = list()
        for row in self.budgets_avlb_query:
            budget_ids_present.append(row[self.indexes["budget_id"]])
        
        while True:
            budget_id = randint(0,10000)
            if budget_id not in budget_ids_present:
                break

        user_id = '101'
        category = "Test_"+'Cat'
        amount = 1500.00
        start_date = "01-09-2024"
        end_date = "01-10-2024"
        with self.assertRaises(Exception) as context:
            self.manager.edit_budget(
                budget_id=budget_id,
                user_id=user_id,
                category=category,
                amount=amount,
                start_date=start_date,
                end_date=end_date
            )
        self.assertEqual(str(context.exception),"No budget found with this ID.")

    # Empty Budget ID Test
    def test_empty_budget_id(self):
        budget_id = None
        user_id = '101'
        category = "Test_"+'Cat'
        amount = 1500.00
        start_date = "01-09-2024"
        end_date = "01-10-2024"
        with self.assertRaises(Exception) as context:
            self.manager.edit_budget(
                budget_id=budget_id,
                user_id=user_id,
                category=category,
                amount=amount,
                start_date=start_date,
                end_date=end_date
            )
        self.assertEqual(str(context.exception),"Budget ID cannot be empty.")

    # Budget Not an Integer
    def test_non_integer_budget_id(self):
        budget_id = "adfk"
        user_id = '101'
        category = "Test_"+'Cat'
        amount = 1500.00
        start_date = "01-09-2024"
        end_date = "01-10-2024"
        with self.assertRaises(Exception) as context:
            self.manager.edit_budget(
                budget_id=budget_id,
                user_id=user_id,
                category=category,
                amount=amount,
                start_date=start_date,
                end_date=end_date
            )
        self.assertEqual(str(context.exception),"Budget ID must be an Integer!")

    # Empty User ID input Test
    def test_empty_user_id(self):
        budget_id = 1
        user_id = None
        category = "Test_"+'Cat'
        amount = 1500.00
        start_date = "01-09-2024"
        end_date = "01-10-2024"
        with self.assertRaises(Exception) as context:
            self.manager.edit_budget(
                budget_id=budget_id,
                user_id=user_id,
                category=category,
                amount=amount,
                start_date=start_date,
                end_date=end_date
            )
        self.assertEqual(str(context.exception),"User ID cannot be empty.")

    # User ID provided doesn't exist
    def test_non_existent_user(self):
        budget_id = 1
        user_id = 'abcd'
        category = "Test_"+'Cat'
        amount = 1500.00
        start_date = "01-09-2024"
        end_date = "01-10-2024"
        with self.assertRaises(Exception) as context:
            self.manager.edit_budget(
                budget_id=budget_id,
                user_id=user_id,
                category=category,
                amount=amount,
                start_date=start_date,
                end_date=end_date
            )
        self.assertEqual(str(context.exception),"User ID provided doesn't exists!")


    # Empty Category input Test
    def test_empty_category_input(self):
        budget_id = 1
        user_id = '101'
        category = ""
        amount = 1500.00
        start_date = "01-09-2024"
        end_date = "01-10-2024"
        with self.assertRaises(Exception) as context:
            self.manager.edit_budget(
                budget_id=budget_id,
                user_id=user_id,
                category=category,
                amount=amount,
                start_date=start_date,
                end_date=end_date
            )
        self.assertEqual(str(context.exception),"Budget category cannot be empty.")


    # Invalid amount input Test
    def test_invalid_amount_input(self):
        budget_id = 1
        user_id = '101'
        category = "Test_"+'Cat'
        amount = '1500abcd'
        start_date = "01-09-2024"
        end_date = "01-10-2024"
        with self.assertRaises(Exception) as context:
            self.manager.edit_budget(
                budget_id=budget_id,
                user_id=user_id,
                category=category,
                amount=amount,
                start_date=start_date,
                end_date=end_date
            )
        self.assertEqual(str(context.exception),"Amount not convertible to float")


    # Invalid start date input Test
    def test_invalid_start_date_input(self):
        budget_id = 1
        user_id = '101'
        category = "Test_"+'Cat'
        amount = 1500.00
        start_date = "32-09-2024"
        end_date = "01-10-2024"
        with self.assertRaises(Exception) as context:
            self.manager.edit_budget(
                budget_id=budget_id,
                user_id=user_id,
                category=category,
                amount=amount,
                start_date=start_date,
                end_date=end_date
            )
        self.assertEqual(str(context.exception),"time data '32-09-2024' does not match format '%d-%m-%Y'")



    # Invalid end date input Test
    def test_invalid_end_date_input(self):
        budget_id = 1
        user_id = '101'
        category = "Test_"+'Cat'
        amount = 1500.00
        start_date = "30-09-2024"
        end_date = "41-10-2024"
        with self.assertRaises(Exception) as context:
            self.manager.edit_budget(
                budget_id=budget_id,
                user_id=user_id,
                category=category,
                amount=amount,
                start_date=start_date,
                end_date=end_date
            )
        self.assertEqual(str(context.exception),"time data '41-10-2024' does not match format '%d-%m-%Y'")



    # Start date after End date Test
    def test_start_date_after_end_date(self):
        budget_id = 1
        user_id = '101'
        category = "Test_"+'Cat'
        amount = 1500.00
        start_date = "01-10-2024"
        end_date = "01-09-2024"
        with self.assertRaises(Exception) as context:
            self.manager.edit_budget(
                budget_id=budget_id,
                user_id=user_id,
                category=category,
                amount=amount,
                start_date=start_date,
                end_date=end_date
            )
        self.assertEqual(str(context.exception),"End date must be after the start date.")



    # Negative Amount input Test
    def test_negative_amount_input(self):
        budget_id = 1
        user_id = '101'
        category = "Test_"+'Cat'
        amount = -1500.00
        start_date = "01-09-2024"
        end_date = "01-10-2024"
        with self.assertRaises(Exception) as context:
            self.manager.edit_budget(
                budget_id=budget_id,
                user_id=user_id,
                category=category,
                amount=amount,
                start_date=start_date,
                end_date=end_date
            )
        self.assertEqual(str(context.exception),"Amount must be greater than zero.")

    
    # Mismatch User ID Test
    def test_mismatch_user_id(self):
        budget_id = 1
        user_id = '102'
        category = "Test_"+'Cat'
        amount = 1500.00
        start_date = "01-09-2024"
        end_date = "01-10-2024"
        with self.assertRaises(Exception) as context:
            self.manager.edit_budget(
                budget_id=budget_id,
                user_id=user_id,
                category=category,
                amount=amount,
                start_date=start_date,
                end_date=end_date
            )
        self.assertEqual(str(context.exception),"User ID not associated with given budget ID")


if __name__=="__main__":
    unittest.main()