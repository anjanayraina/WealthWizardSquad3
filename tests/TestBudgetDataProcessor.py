import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, IntegerType
from src.BudgetDataProcessor import BudgetDataProcessor
from src.DBHelper import DBHelper
from src.exceptions import MissingRequiredColumnsError, InvalidDataError, DatabaseExecutionError
import datetime
from dotenv import load_dotenv

load_dotenv()

class TestBudgetDataProcessor(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Initialize Spark session
        cls.spark = SparkSession.builder \
            .appName("TestBudgetDataProcessor") \
            .getOrCreate()

        # Mock DBHelper class
        cls.mock_db_helper = MagicMock(spec=DBHelper)

        # Initialize BudgetDataProcessor with mocked DBHelper
        cls.processor = BudgetDataProcessor()
        cls.processor.db_helper = cls.mock_db_helper

    @classmethod
    def tearDownClass(cls):
        # Stop Spark session after tests
        cls.spark.stop()

    def test_read_csv_missing_file(self):
        # Test for missing file error in read_csv method
        with self.assertRaises(InvalidDataError):
            self.processor.read_csv("non_existent_file.csv")

    def test_process_data_missing_columns(self):
        # Test for missing required columns in DataFrame
        data = [
            (101, "UF101", 500, '01-08-2024', '31-08-2024')  # Missing 'category' column
        ]
        schema = StructType([
            StructField("budget_id", IntegerType(), True),
            StructField("user_id", StringType(), True),  # Changed user_id to StringType (new schema)
            StructField("amount", IntegerType(), True),
            StructField("start_date", StringType(), True),
            StructField("end_date", StringType(), True)
        ])
        df = self.spark.createDataFrame(data, schema)

        with self.assertRaises(MissingRequiredColumnsError):
            self.processor.process_data(df)


    def test_save_to_database_error(self):
        # Test for database save error
        data = [
            (101, "UF101", "Groceries", 500.0, datetime.date(2024, 8, 1), datetime.date(2024, 8, 31), "NA", 1, "NA"),
        ]
        schema = StructType([
            StructField("budget_id", IntegerType(), True),
            StructField("user_id", StringType(), True),  # Changed user_id to StringType (new schema)
            StructField("category", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("start_date", DateType(), True),
            StructField("end_date", DateType(), True),
            StructField("comments", StringType(), True),  # Added 'comments' column
            StructField("alert_threshold", IntegerType(), True),  # Added 'alert_threshold' column
            StructField("alert_preference", StringType(), True)  # Added 'alert_preference' column
        ])
        df = self.spark.createDataFrame(data, schema)

        # Simulate a database execution error
        self.mock_db_helper.execute_query.side_effect = Exception("Database error")

        with self.assertRaises(DatabaseExecutionError):
            self.processor.save_to_database(df)

if __name__ == '__main__':
    unittest.main()
