import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
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
        # Include the new columns 'comments', 'alert_threshold', and 'alert_preference'
        required_columns = ["user_id", "category", "amount", "start_date", "end_date", "budget_id", "comments",
                            "alert_threshold", "alert_preference"]
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            raise MissingRequiredColumnsError(missing_columns)

        try:
            # Drop rows with missing values in required columns
            df = df.na.drop(subset=required_columns)

            # Filter out invalid amounts (e.g., amounts <= 0)
            df = df.filter(col("amount") > 0)

            # Convert start_date and end_date from 'dd-MM-yyyy' to date type
            df = df.withColumn("start_date", to_date(col("start_date"), "dd-MM-yyyy"))
            df = df.withColumn("end_date", to_date(col("end_date"), "dd-MM-yyyy"))

            # Filter to ensure that end_date is greater than start_date
            df = df.filter(col("end_date") > col("start_date"))

            # Handle missing or default values for 'comments', 'alert_threshold', and 'alert_preference'
            # Fill missing or null values in 'comments' with 'NA'
            df = df.fillna({'comments': 'NA'})

            # Fill missing or null values in 'alert_threshold' with 1 and ensure it's cast as integer
            df = df.fillna({'alert_threshold': 1}).withColumn("alert_threshold", col("alert_threshold").cast("int"))

            # Fill missing or null values in 'alert_preference' with 'NA'
            df = df.fillna({'alert_preference': 'NA'})

            return df
        except Exception as e:
            raise InvalidDataError(f"Error processing data: {str(e)}")

    def save_to_database(self, df):
        try:
            for row in df.collect():
                query = """
                    BEGIN
                        create_budget_proc(:1, :2, :3, :4, :5, :6, :7, :8, :9);
                    END;
                """

                # Convert date formats to 'DD-MON-YYYY' format for Oracle
                start_date = row['start_date'].strftime('%d-%b-%Y').upper()
                end_date = row['end_date'].strftime('%d-%b-%Y').upper()

                params = (
                    row['budget_id'],
                    row['user_id'],
                    row['category'],
                    row['amount'],
                    start_date,
                    end_date,
                    row['comments'] if 'comments' in row else 'NA',  # Handle missing value
                    row['alert_threshold'] if 'alert_threshold' in row else 1,  # Handle missing value
                    row['alert_preference'] if 'alert_preference' in row else 'NA'  # Handle missing value
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
