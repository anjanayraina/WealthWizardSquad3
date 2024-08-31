import oracledb
from pyspark.sql import DataFrame
from pyspark.sql.functions import year, col, substring, date_format,sum
import os
from dotenv import load_dotenv
from SparkManager import SparkManager

load_dotenv()

class ViewBudgetFilters:
    def __init__(self):
        # Initialize Oracle client to avoid thin mode error
        oracledb.init_oracle_client()
        self.sparkManager = SparkManager(
            user=os.getenv("USER_SYSTEM"),
            password=os.getenv("PASSWORD"),
            host=os.getenv("HOST"),
            port=os.getenv("PORT"),
            sid=os.getenv("SID")
        )

        budget_df = self.sparkManager.read_data("budgets")
        self.apply_filters(budget_df)
        

    def apply_filters(self, budget_df: DataFrame):
        # Extract date part
        budget_df = budget_df.withColumn("START_BUDGET_DATE", date_format("START_DATE", "yyyy-MM-dd")) \
                             .withColumn("END_BUDGET_DATE", date_format("END_DATE", "yyyy-MM-dd")) \
                             .withColumn("BUDGET_ID", col("BUDGET_ID").cast("int")) \
                             .withColumn("USER_ID", col("USER_ID").cast("int")) \
                             .drop("START_DATE") \
                             .drop("END_DATE")
        
        #Filtering by year
        print("Enter the budget year")
        budget_year = int(input())
        #Function call
        print(f"Budget year: {budget_year}")
        filtered_by_year = self.filter_by_year(budget_df, budget_year)
        if filtered_by_year is not None:
            self.display_data(filtered_by_year)

        #Filtering by month 
        print("Enter the budget month")
        budget_month = int(input())
        #Function call
        print(f"Budget month: {budget_month}")
        filtered_by_month = self.filter_by_month(budget_df, budget_month)
        if filtered_by_month is not None:
            self.display_data(filtered_by_month)

        #Filtering by amount
        print("Enter the amount range")
        amnt1 = int(input("Amount 1: ")) 
        amnt2 = int(input("Amount 2: "))
        #Function call
        print(f"Budget amount: from {amnt1} to {amnt2}")
        filtered_by_amount = self.filter_by_amount_range(budget_df, amnt1, amnt2)
        if filtered_by_amount is not None:
            self.display_data(filtered_by_amount)

        #Filtering by category 
        print("Enter the budget category")
        budget_category = input()
        #Function call
        print(f"Budget Category: {budget_category}")
        filtered_by_category = self.filter_by_category(budget_df, budget_category)
        if filtered_by_category is not None:
            self.display_data(filtered_by_category)

        #Grouping by category 
        filtered_by_category = self.grp_sum_by_category(budget_df)
        if filtered_by_category is not None:
            print("Category Wise budget details")
            self.display_data(filtered_by_category)


    def filter_by_year(self, df, year_value):
        #Empty dataframe
        if df is None:
            raise ValueError("DataFrame is None")
        #Applying filter function on column
        filtered_df = df.filter(year(df['START_BUDGET_DATE']) == year_value)
        if filtered_df.count() == 0:
            print(f"No data found for year: {year_value}")
            return None
        return filtered_df

    def grp_sum_by_category(self, df):
        #Empty dataframe
        if df is None:
            raise ValueError("DataFrame is None")
        #Applying grouping and aggregation.
        filtered_df = df.groupBy('Category').agg(sum('Amount').alias('total_amount'))
        if filtered_df.count() == 0:
            print(f"No data found")
            return None
        return filtered_df

    def filter_by_month(self, df, month: int):
        #Empty dataframe
        if df is None:
            raise ValueError("DataFrame is None")
        
        month_str = str(month).zfill(2)
        #Applying filter function on column
        filtered_df = df.filter(substring(col('START_BUDGET_DATE'), 6, 2) == month_str)
        if filtered_df.count() == 0:
            print(f"No data found for month: {month_str}")
            return None
        return filtered_df


    def filter_by_amount_range(self, df, min_amount: float, max_amount: float):
        #Empty dataframe
        if df is None:
            raise ValueError("DataFrame is None")
        #Applying filter function on column
        filtered_df = df.filter((col('AMOUNT') >= min_amount) & (col('AMOUNT') <= max_amount))
        if filtered_df.count() == 0:
            print(f"No data found for this amount range")
            return None
        return filtered_df
    

    def filter_by_category(self, df, category_value):
        #Empty dataframe
        if df is None:
            raise ValueError("DataFrame is None")
        #Applying filter function on column
        filtered_df = df.filter(col('CATEGORY') == category_value)
        if filtered_df.count() == 0:
            print(f"No data found for category: {category_value}")
            return None
        return filtered_df


    def display_data(self, df: DataFrame):
        df.show(truncate=False)

    def close_session(self):
        self.sparkManager.stop_session()

if __name__ == "__main__":
    try:
        view_budget = ViewBudgetFilters()
    except Exception as e:
        raise
    finally:
        view_budget.close_session()