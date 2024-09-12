import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DoubleType
from src.DBHelper import DBHelper
from src.exceptions import MissingRequiredColumnsError, InvalidDataError, DatabaseExecutionError
from dotenv import load_dotenv
from prettytable import PrettyTable

load_dotenv()

class BudgetDataProcessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("BudgetDataProcessing") \
            .getOrCreate()
        self.db_helper = DBHelper(
            user=os.getenv("USER_SYSTEM"),
            password=os.getenv("PASSWORD"),
            host=os.getenv("HOST"),
            port=os.getenv("PORT"),
            sid=os.getenv("SID")
        )
        self.db_helper.connect()

    def read_csv(self, file_path):
        try:
            df = self.spark.read.csv(file_path, header=True, inferSchema=True)
            return df
        except Exception as e:
            raise InvalidDataError(f"Error reading CSV file at {file_path}: {str(e)}")

    def process_data(self, df):
        required_columns = ["user_id", "category", "amount", "start_date", "end_date", "budget_id"]
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            raise MissingRequiredColumnsError(missing_columns)

        try:
            df = df.na.drop(subset=required_columns)
            df = df.filter(col("amount") > 0)
            df = df.withColumn("start_date", to_date(col("start_date"), "dd-MM-yyyy"))
            df = df.withColumn("end_date", to_date(col("end_date"), "dd-MM-yyyy"))
            df = df.filter(col("end_date") > col("start_date"))

            return df
        except Exception as e:
            raise InvalidDataError(f"Error processing data: {str(e)}")

    def save_to_database(self, df):
        try:
            for row in df.collect():
                query = """
                    BEGIN
                        create_budget_proc(:1, :2, :3, :4, :5, :6);
                    END;
                """
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
                self.db_helper.execute_query(query, params, commit=True)
        except Exception as e:
            raise DatabaseExecutionError(f"Error saving data to the database: {str(e)}")

    def process_and_save(self, file_path):
        df = self.read_csv(file_path)
        cleaned_df = self.process_data(df)
        self.save_to_database(cleaned_df)

    def close(self):
        self.spark.stop()
        self.db_helper.close()

    def list_all_budgets(self):
        query = """
               SELECT budget_id, user_id, category, amount, start_date, end_date
               FROM budgets
           """
        try:
            result = self.db_helper.execute_query(query)
            if not result:
                print("No Entries in the Budget")
            else:
                table = PrettyTable()
                table.field_names = ["Budget Id", "User Id", "Category", "Amount", "Start_date", "End_date"]
                for budget in result:
                    table.add_row([budget[0], budget[1], budget[2], budget[3], budget[4], budget[5]])
                print(table)
        except Exception as e:
            raise DatabaseExecutionError(f"Error listing all budgets: {str(e)}")
        return result
