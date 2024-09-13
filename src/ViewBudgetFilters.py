import oracledb
from pyspark.sql import DataFrame
from pyspark.sql.functions import year, col, substring, date_format, sum
import os
from dotenv import load_dotenv
from SparkManager import SparkManager
from datetime import datetime

load_dotenv()

class ViewBudgetFilters:
    def __init__(self,id):
        oracledb.init_oracle_client()
        self.spark_manager = SparkManager(
            user=os.getenv("USER_SYSTEM"),
            password=os.getenv("PASSWORD"),
            host=os.getenv("HOST"),
            port=os.getenv("PORT"),
            sid=os.getenv("SID")
        )
        user_id = id
        try:
            budget_df = self.spark_manager.read_data("budgets")
            self.apply_filters(budget_df,user_id)
        except Exception as e:
            print(f"An error occurred during initialization: {e}")
        finally:
            self.clear_cache()
            self.stop_spark_session()

    def apply_filters(self, budget_df: DataFrame, user_id):
        budget_df = (budget_df.withColumn("START_BUDGET_DATE", date_format("START_DATE", "yyyy-MM-dd"))
                             .withColumn("END_BUDGET_DATE", date_format("END_DATE", "yyyy-MM-dd"))
                             .withColumn("BUDGET_ID", col("BUDGET_ID").cast("int"))
                             .withColumn("USER_ID", col("USER_ID").cast("int"))
                             .drop("START_DATE", "END_DATE"))

        try:
            # Get and validate budget year
            while True:
                try:
                    budget_year = int(input("Enter the budget year (YYYY): "))
                    if budget_year > datetime.now().year:
                        print(f"Year {budget_year} is not valid. It should not be greater than the current year.")
                    else:
                        break
                except ValueError:
                    print("Invalid input. Please enter a valid year (YYYY).")

            filtered_by_year = self.filter_by_year(budget_df, budget_year,user_id)
            if filtered_by_year:
                self.display_data(filtered_by_year)

            # Get and validate budget month
            while True:
                try:
                    budget_month = int(input("Enter the budget month (MM): "))
                    if budget_month < 1 or budget_month > 12:
                        print("Month should be between 01 and 12.")
                    else:
                        break
                except ValueError:
                    print("Invalid input. Please enter a valid month (MM).")

            filtered_by_month = self.filter_by_month(budget_df, budget_month, user_id)
            if filtered_by_month:
                self.display_data(filtered_by_month)

            # Get and validate amounts
            while True:
                try:
                    min_amount = float(input("Enter the minimum amount: "))
                    max_amount = float(input("Enter the maximum amount: "))
                    if max_amount < min_amount:
                        print("Maximum amount should be greater than or equal to minimum amount.")
                    else:
                        break
                except ValueError:
                    print("Invalid input. Please enter a valid amount.")

            filtered_by_amount = self.filter_by_amount_range(budget_df, min_amount, max_amount, user_id)
            if filtered_by_amount:
                self.display_data(filtered_by_amount)

            # Get and validate budget category
            budget_category = input("Enter the budget category: ")
            filtered_by_category = self.filter_by_category(budget_df, budget_category, user_id)
            if filtered_by_category:
                self.display_data(filtered_by_category)

            # Get and display category summary
            category_summary = self.grp_sum_by_category(budget_df, user_id)
            if category_summary:
                print("Category Wise Budget Summary:")
                self.display_data(category_summary)

        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    def filter_by_year(self, df, year_value: int, user_id: int):
        filtered_df = df.filter(
            ((year(df['START_BUDGET_DATE']) == year_value) | (year(df['END_BUDGET_DATE']) == year_value)) & 
            (df['USER_ID'] == user_id)
        )
        if filtered_df.count() == 0:
            print(f"No data found for year: {year_value}")
            return None
        return filtered_df



    def filter_by_month(self, df, month: int, user_id: int):
        month_str = str(month).zfill(2)
        filtered_df = df.filter(
            ((substring(col('START_BUDGET_DATE'), 6, 2) == month_str) | (substring(col('END_BUDGET_DATE'), 6, 2) == month_str)) & 
            (df['USER_ID'] == user_id)
        )
        if filtered_df.count() == 0:
            print(f"No data found for month: {month_str}")
            return None
        return filtered_df



    def filter_by_amount_range(self, df, min_amount: float, max_amount: float, user_id: int):
        if df is None:
            raise ValueError("DataFrame is None")
        filtered_df = df.filter((col('AMOUNT') >= min_amount) & (col('AMOUNT') <= max_amount) & (df['USER_ID'] == user_id))
        if filtered_df.count() == 0:
            print(f"No data found for amount range {min_amount} - {max_amount}")
            return None
        return filtered_df


    def filter_by_category(self, df, category_value: str, user_id: int):
        if df is None:
            raise ValueError("DataFrame is None")
        filtered_df = df.filter((col('CATEGORY') == category_value) & (df['USER_ID'] == user_id))
        if filtered_df.count() == 0:
            print(f"No data found for category: {category_value}")
            return None
        return filtered_df


    def grp_sum_by_category(self, df, user_id: int):
        if df is None:
            raise ValueError("DataFrame is None")
        summary_df = df.filter(col('USER_ID') == user_id).groupBy('CATEGORY').agg(sum('AMOUNT').alias('TOTAL_AMOUNT'))
        if summary_df.count() == 0:
            print("No data found for category summary")
            return None
        return summary_df


    def display_data(self, df: DataFrame):
        df.show(truncate=False)

    def clear_cache(self):
        """ Clears the cache after Spark operations """
        if self.spark_manager.spark is not None:
            self.spark_manager.spark.catalog.clearCache()

    def stop_spark_session(self):
        """ Stops the Spark session to release resources """
        if self.spark_manager is not None:
            self.spark_manager.stop_session()

if __name__ == "__main__":
    try:
        view_budget = ViewBudgetFilters()
    except Exception as e:
        print(f"Error: {e}")
