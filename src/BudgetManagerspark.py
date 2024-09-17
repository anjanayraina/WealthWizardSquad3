import findspark
import jaydebeapi
import jpype
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

class BudgetManagerSpark:
    def __init__(self, app_name, driver_path, jdbc_url, connection_properties):
        findspark.init()
        self.spark = None
        self.conn = None
        self.cursor = None
        self.driver_path = driver_path
        self.jdbc_url = jdbc_url
        self.connection_properties = connection_properties

        try:
            self.spark = SparkSession.builder \
                .appName(app_name) \
                .config("spark.driver.extraClassPath", driver_path) \
                .config("spark.cleaner.referenceTracking.cleanCheckpoints", "false") \
                .config("spark.local.dir", "C:/path/to/your/custom/temp2") \
                .getOrCreate()
            print("SparkSession created successfully.")
        except Exception as e:
            print(f"Error while creating SparkSession: {e}")
            raise

    def connect_to_db(self):
        try:
            self.conn = jaydebeapi.connect(
                self.connection_properties["driver"],
                self.jdbc_url,
                [self.connection_properties["user"], self.connection_properties["password"]],
                jars=self.driver_path
            )
            self.cursor = self.conn.cursor()
            print("Database connection established successfully.")
        except Exception as e:
            print(f"Error while connecting to the database: {e}")
            raise

    def fetch_budgets(self, user_id):
        try:
           # sql_query = f"SELECT budget_id,CAST(user_id AS VARCHAR) AS user_id, ,category,amount FROM budgets WHERE user_id = '{user_id}'"
            sql_query = f"""
                SELECT 
                    budget_id, 
                    TO_CHAR(user_id) AS user_id, 
                    category, 
                    TO_NUMBER(amount) AS amount 
                FROM 
                    budgets 
                WHERE 
                    user_id = '{user_id}'
            """
            df = self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", f"({sql_query})") \
                .option("user", self.connection_properties["user"]) \
                .option("password", self.connection_properties["password"]) \
                .option("driver", self.connection_properties["driver"]) \
                .load()

            formatted_df = df \
                .withColumn("amount", F.format_number(F.col("amount"), 2)) \
                .withColumn("budget_id", F.format_number(F.col("budget_id"), 2))

            return formatted_df
        except Exception as e:
            print(f"Error while fetching budgets: {e}")
            raise

    def delete_budgets(self, user_id, category=None):
        try:
            if category == 'all':
                delete_sql = "DELETE FROM budgets WHERE user_id = ?"
                self.cursor.execute(delete_sql, (user_id,))
                print(f"All budgets for user_id '{user_id}' deleted successfully.")
            else:
                delete_sql = "DELETE FROM budgets WHERE user_id = ? AND category = ?"
                self.cursor.execute(delete_sql, (user_id, category))
                print(f"Budgets for user_id '{user_id}' and category '{category}' deleted successfully.")
        except Exception as e:
            print(f"An error occurred during the delete operation: {e}")
            self.conn.rollback()
            raise

    def show_remaining_budgets(self):
        try:
            remaining_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", "budgets") \
                .option("user", self.connection_properties["user"]) \
                .option("password", self.connection_properties["password"]) \
                .option("driver", self.connection_properties["driver"]) \
                .load()

            # Formatting the 'amount' and 'budget_id' columns
            formatted_df = remaining_df \
                .withColumn("amount", F.format_number(F.col("amount"), 2)) \
                .withColumn("budget_id", F.format_number(F.col("budget_id"), 2))

            print("Remaining records in budgets table with formatted columns:")
            formatted_df.show()
        except Exception as e:
            print(f"Error while reading remaining data from Oracle: {e}")
            raise

    def close(self):
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            if self.spark:
                self.spark.stop()
            print("Resources closed successfully.")
        except Exception as e:
            print(f"Error while closing resources: {e}")
            raise

