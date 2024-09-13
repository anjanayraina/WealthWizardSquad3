import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, IntegerType
from src.BudgetDataProcessor import BudgetDataProcessor
from src.DBHelper import DBHelper
from src.exceptions import MissingRequiredColumnsError, InvalidDataError, DatabaseExecutionError
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

    def test_read_csv_missing_file(self):
        with self.assertRaises(InvalidDataError):
            self.processor.read_csv("non_existent_file.csv")

    def test_process_data_missing_columns(self):
        data = [
            (101, 1001, 500, '01-08-2024', '31-08-2024'),  # Missing 'category' column
        ]
        schema = StructType([
            StructField("budget_id", IntegerType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("amount", IntegerType(), True),
            StructField("start_date", StringType(), True),
            StructField("end_date", StringType(), True)
        ])
        df = self.spark.createDataFrame(data, schema)

        with self.assertRaises(MissingRequiredColumnsError):
            self.processor.process_data(df)

    def test_process_data_invalid_data(self):
        data = [
            (101, 1001, 'Groceries', 500, 'invalid_date', '31-08-2024'),
        ]
        schema = StructType([
            StructField("budget_id", IntegerType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("category", StringType(), True),
            StructField("amount", IntegerType(), True),
            StructField("start_date", StringType(), True),
            StructField("end_date", StringType(), True)
        ])
        df = self.spark.createDataFrame(data, schema)

        with self.assertRaises(InvalidDataError):
            self.processor.process_data(df)

    def test_save_to_database_error(self):
        data = [
            ("1", "1001", "Groceries", 200.0, datetime.date(2024, 8, 1), datetime.date(2024, 8, 31)),
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

        # Simulate a database execution error
        self.mock_db_helper.execute_query.side_effect = Exception("Database error")

        with self.assertRaises(DatabaseExecutionError):
            self.processor.save_to_database(df)
