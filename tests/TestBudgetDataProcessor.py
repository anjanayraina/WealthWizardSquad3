import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from src.BudgetDataProcessor import BudgetDataProcessor
from src.DBHelper import DBHelper
import os
from dotenv import load_dotenv

load_dotenv()
class TestBudgetDataProcessor(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestBudgetDataProcessor") \
            .getOrCreate()

        cls.mock_db_helper = MagicMock(spec=DBHelper)

        cls.processor = BudgetDataProcessor()
        cls.processor.db_helper = cls.mock_db_helper

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_read_csv(self):
        schema = StructType([
            StructField("budget_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("category", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("start_date", StringType(), True),
            StructField("end_date", StringType(), True)
        ])

        df = self.processor.read_csv("../budget_data.csv")
        df.show()
    def test_process_data(self):
        data = [
            ("B001", "U001", "Groceries", 500.0, "2024-08-01", "2024-08-31"),
            ("B002", "U002", "Utilities", -100.0, "2024-08-01", "2024-08-31"),
            ("B003", "U003", "Rent", 800.0, "2024-08-31", "2024-08-01"),
            ("B004", None, "Entertainment", 200.0, "2024-08-01", "2024-08-31")
        ]

        schema = StructType([
            StructField("budget_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("category", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("start_date", StringType(), True),
            StructField("end_date", StringType(), True)
        ])

        df = self.spark.createDataFrame(data, schema)

        cleaned_df = self.processor.process_data(df)

        self.assertEqual(cleaned_df.count(), 1)
        self.assertEqual(cleaned_df.collect()[0]["budget_id"], "B001")

    def test_save_to_database(self):
        data = [("B001", "U001", "Groceries", 500.0, "2024-08-01", "2024-08-31")]
        schema = StructType([
            StructField("budget_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("category", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("start_date", DateType(), True),
            StructField("end_date", DateType(), True)
        ])
        df = self.spark.createDataFrame(data, schema)
        with patch.object(self.processor.db_helper, 'execute_query') as mock_execute_query:
            self.processor.save_to_database(df)
            self.assertEqual(mock_execute_query.call_count, 1)

    def test_process_and_save(self):
        csv_file_path = "../budget_data.csv"
        self.processor.process_and_save(csv_file_path)
        self.processor.list_all_budgets()



if __name__ == '__main__':
    unittest.main()
