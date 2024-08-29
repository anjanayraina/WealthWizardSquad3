import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when
from pyspark.sql.types import DoubleType
from src.DBHelper import DBHelper
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
        df = self.spark.read.csv(file_path, header=True, inferSchema=True)
        return df

    def process_data(self, df):

        df = df.withColumn("start_date", to_date(col("start_date"), "yyyy-MM-dd"))
        df = df.withColumn("end_date", to_date(col("end_date"), "yyyy-MM-dd"))

        df = df.na.drop(subset=["user_id", "category", "amount", "start_date", "end_date"])
        df = df.filter(col("amount") > 0)


        df = df.filter(col("end_date") > col("start_date"))

        return df

    def save_to_database(self, df):
        for row in df.collect():
            query = """
                BEGIN
                    create_budget_proc(:1, :2, :3, :4, :5, :6);
                END;
            """
            params = (
                row['budget_id'],
                row['user_id'],
                row['category'],
                row['amount'],
                row['start_date'].strftime('%d-%m-%Y'),
                row['end_date'].strftime('%d-%m-%Y')
            )
            self.db_helper.execute_query(query, params, commit=True)

    def process_and_save(self, file_path):
        """Read, process, and save data from a CSV file to the database."""
        df = self.read_csv(file_path)
        cleaned_df = self.process_data(df)
        print(cleaned_df)
        self.save_to_database(cleaned_df)

    def close(self):
        """Close the Spark session and database connection."""
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
            print(f"Error: {e}")
        return result

