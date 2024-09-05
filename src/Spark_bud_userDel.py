import findspark
import jaydebeapi
import jpype
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize SparkSession
try:
    spark = SparkSession.builder \
        .appName("OracleConnection") \
        .config("spark.driver.extraClassPath", r"C:\Users\HP\Downloads\ojdbc8.jar") \
        .config("spark.cleaner.referenceTracking.cleanCheckpoints", "false") \
        .config("spark.local.dir", "C:/path/to/your/custom/temp2") \
        .getOrCreate()
    print("SparkSession created successfully.")
except Exception as e:
    print(f"Error while creating SparkSession: {e}")
    raise

# JDBC URL and connection properties
jdbc_url = "jdbc:oracle:thin:@localhost:1521:orcl"
driver_path = r"C:\Users\HP\Downloads\ojdbc8.jar"
connection_properties = {
    "user": "C##myuser",
    "password": "Redhat",
    "driver": "oracle.jdbc.driver.OracleDriver"
}

# Handling DELETE operation with multiple categories and displaying DataFrame
try:
    # Connect to the Oracle database using JayDeBeAPI
    conn = jaydebeapi.connect(
        connection_properties["driver"],
        jdbc_url,
        [connection_properties["user"], connection_properties["password"]],
        jars=driver_path
    )
    cursor = conn.cursor()

    # Get user input
    user_id_to_check = input("Enter your user_id to delete associated budgets: ")

    # Fetch all budgets for the user_id
    sql_query = f"SELECT * FROM budgets WHERE user_id = '{user_id_to_check}'"
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", f"({sql_query})") \
        .option("user", connection_properties["user"]) \
        .option("password", connection_properties["password"]) \
        .option("driver", connection_properties["driver"]) \
        .load()

    # Display all budgets for this user_id
    if df.count() > 0:
        print(f"All budgets for user_id '{user_id_to_check}':")
        df.show()

        # Get unique categories for this user
        # categories = df.select("CATEGORY").distinct().rdd.collect()
        # Get distinct categories as an RDD
        # distinct_categories = df.select("CATEGORY").distinct().rdd

        # Convert RDD to a list using collect()
        # categories = distinct_categories.collect()
        categories = [row['CATEGORY'] for row in df.select("CATEGORY").distinct().collect()]
        print(f"Categories available for deletion: {categories}")
        # Ask which category to delete or delete all
        category_to_delete = input("Enter the category you want to delete or type 'all' to delete all budgets: ")

        if category_to_delete == 'all':
            # Delete all budgets for the user_id
            delete_sql = "DELETE FROM budgets WHERE user_id = ?"
            cursor.execute(delete_sql, (user_id_to_check,))
            print(f"All budgets for user_id '{user_id_to_check}' deleted successfully.")
        elif category_to_delete in categories:
            # Delete only the budgets with the specified category for the user_id
            delete_sql = "DELETE FROM budgets WHERE user_id = ? AND category = ?"
            cursor.execute(delete_sql, (user_id_to_check, category_to_delete))
            print(f"Budgets for user_id '{user_id_to_check}' and category '{category_to_delete}' deleted successfully.")
        else:
            print(f"Category '{category_to_delete}' not found.")
    else:
        print(f"No budgets found for user_id '{user_id_to_check}'.")

except jaydebeapi.DatabaseError as db_err:
    print(f"Database error: {db_err}")
    conn.rollback()
except Exception as e:
    print(f"An error occurred during the delete operation: {e}")
    
finally:
    # Close the cursor and connection safely
    if cursor:
        cursor.close()
    if conn:
        conn.close()

# Read the remaining data from the budgets table
try:
    remaining_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "budgets") \
        .option("user", connection_properties["user"]) \
        .option("password", connection_properties["password"]) \
        .option("driver", connection_properties["driver"]) \
        .load()

    print("Remaining records in budgets table:")
    remaining_df.show()

except Exception as e:
    print(f"Error while reading remaining data from Oracle: {e}")

finally:
    # Stop SparkSession
    spark.stop()
