import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, IntegerType
from src.BudgetDataProcessor import BudgetDataProcessor
from src.DBHelper import DBHelper
import datetime
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
            StructField("budget_id", IntegerType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("category", StringType(), True),
            StructField("amount", IntegerType(), True),
            StructField("start_date", StringType(), True),
            StructField("end_date", StringType(), True)
        ])

        df = self.processor.read_csv("../budget_data.csv")

        self.assertEqual(df.schema, schema)
        self.assertEqual(df.count(), 6)


    def test_process_data(self):
        data = [
            ("1", "user_1", "Groceries", 200.0, "01-01-2024", "31-01-2024"),
            ("2", "user_2", "Entertainment", 150.0, "01-01-2024", "31-01-2024"),
            ("3", "user_3", "Rent", -500.0, "01-01-2024", "01-01-2024"),
            ("4", "user_4", "Utilities", 0.0, "01-01-2024", "31-01-2024"),
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

        processed_df = self.processor.process_data(df)

        expected_data = [
            ("1", "user_1", "Groceries", 200.0, datetime.date(2024, 1, 1), datetime.date(2024, 1, 31)),
            ("2", "user_2", "Entertainment", 150.0, datetime.date(2024, 1, 1), datetime.date(2024, 1, 31))
        ]
        expected_schema = StructType([
            StructField("budget_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("category", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("start_date", DateType(), True),
            StructField("end_date", DateType(), True)
        ])
        expected_df = self.spark.createDataFrame(expected_data, expected_schema)

        self.assertEqual(processed_df.collect(), expected_df.collect())

    def test_save_to_database(self):
        data = [
            ("1", "user_1", "Groceries", 200.0, "2024-01-01", "2024-01-31"),
            ("2", "user_2", "Entertainment", 150.0, "2024-01-01", "2024-01-31")
        ]
        schema = StructType([
            StructField("budget_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("category", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("start_date", DateType(), True),
            StructField("end_date", DateType(), True)
        ])
        df = self.spark.createDataFrame(data, schema)

        self.processor.save_to_database(df)

        for row in df.collect():
            start_date = row['start_date'].strftime('%d-%m-%Y')
            end_date = row['end_date'].strftime('%d-%m-%Y')
            params = (
                row['budget_id'],
                row['user_id'],
                row['category'],
                row['amount'],
                start_date,
                end_date
            )

            self.mock_db_helper.execute_query.assert_any_call("""
                BEGIN
                    create_budget_proc(:1, :2, :3, :4, :5, :6);
                END;
            """, params, commit=True)

    @patch('src.BudgetDataProcessor.DBHelper')
    def test_process_and_save(self, MockDBHelper):
        mock_db_helper = MockDBHelper.return_value
        mock_db_helper.save.return_value = True

        file_path = "../budget_data.csv"
        self.processor.process_and_save(file_path)

        mock_db_helper.save.assert_called()

if __name__ == '__main__':
    unittest.main()
