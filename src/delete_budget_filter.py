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
        .config("spark.cleaner.referenceTracking.cleanCheckpoints", "false")\
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

# Try reading data from Oracle using JDBC
try:
    sql_query = "SELECT * FROM budgets"
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", f"({sql_query})") \
        .option("user", connection_properties["user"]) \
        .option("password", connection_properties["password"]) \
        .option("driver", connection_properties["driver"]) \
        .load()

    print("Data loaded successfully from Oracle.")
    #df.show()

except Exception as e:
    print(f"Error while loading data from Oracle: {e}")
    spark.stop()
    raise


# Filter DataFrame based on a condition
try:
    # Ask the user if they want to filter by category
    user_input = input("Do you want to filter by a specific category? (yes/no): ").strip().lower()

    if user_input == 'yes':
        # Prompt for the category to filter
        category_to_filter = input("Enter the category you want to filter by: ").strip()

        # Apply the filter on the DataFrame for the chosen category
        #df.printSchema()

        filtered_df = df.filter(df.CATEGORY == category_to_filter)
        
        # Check if there are results after filtering
        if filtered_df.count() > 0:
            print(f"Filtered DataFrame for category '{category_to_filter}':")
            filtered_df.show()
        else:
            print(f"No items found for category '{category_to_filter}'.")
    else:
        print("No filtering applied. Showing the complete DataFrame:")
        df.show()

except Exception as e:
    print(f"Error while filtering DataFrame: {e}")
    spark.stop()
    raise


# Handling the DELETE operation with exception handling
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
    user_id_to_check = input("Enter user_id to delete: ")

    # Check if user_id exists
    check_sql = "SELECT COUNT(*) FROM budgets WHERE user_id = ?"
    cursor.execute(check_sql, (user_id_to_check,))
    count = cursor.fetchone()[0]

    if count > 0:
        # User ID exists, proceed to delete
        delete_sql = "DELETE FROM budgets WHERE user_id = ?"
        cursor.execute(delete_sql, (user_id_to_check,))
        

        print(f"Record with user_id '{user_id_to_check}' deleted successfully.")
    else:
        print("Not a valid user_id")

except jaydebeapi.DatabaseError as db_err:
    print(f"Database error: {db_err}")
    conn.rollback()
except Exception as e:
    print(f"An error occurred during the delete operation: {e}")
    conn.rollback()
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
