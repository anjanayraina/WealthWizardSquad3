import findspark
import jaydebeapi
import jpype
findspark.init()
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("OracleConnection") \
    .config("spark.driver.extraClassPath", r"C:\Users\HP\Downloads\ojdbc8.jar") \
    .config("spark.cleaner.referenceTracking.cleanCheckpoints", "false")\
    .getOrCreate()

# JDBC URL and connection properties
jdbc_url = "jdbc:oracle:thin:@localhost:1521:orcl"
driver_path=r"C:\Users\HP\Downloads\ojdbc8.jar"
connection_properties = {
    "user": "C##myuser",  # Replace with your Oracle username
    "password": "Redhat",  # Replace with your Oracle password
    "driver": "oracle.jdbc.driver.OracleDriver"
}

# SQL Query to select data from the budgets table
sql_query = "SELECT * FROM budgets"

# Read data from Oracle using JDBC
df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", f"({sql_query})") \
    .option("user", connection_properties["user"]) \
    .option("password", connection_properties["password"]) \
    .option("driver", connection_properties["driver"]) \
    .load()

# Show DataFrame
df.show()

# defining DELETE command
delete_sql = "DELETE FROM budgets WHERE user_id = 'abc12'"
conn = jaydebeapi.connect(
    connection_properties["driver"],
    jdbc_url,
    [connection_properties["user"], connection_properties["password"]],
    jars=driver_path
)

# Create a cursor object

cursor = conn.cursor()
user_id_to_check=103
# Execute the DELETE command
# Check if the user_id exists
check_sql = "SELECT COUNT(*) FROM budgets WHERE user_id = ?"
cursor.execute(check_sql, (user_id_to_check,))
count = cursor.fetchone()[0]



if count > 0:
    # User ID exists, proceed to delete
    delete_sql = "DELETE FROM budgets WHERE user_id = ?"
    cursor.execute(delete_sql, (user_id_to_check,))
    print(f"Record with user_id '{user_id_to_check}' deleted successfully.")
else:
    # User ID does not exist
    print("Not valid user_id")

# # Commit the transaction
# conn.commit()
remaining_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "budgets") \
    .option("user", connection_properties["user"]) \
    .option("password", connection_properties["password"]) \
    .option("driver", connection_properties["driver"]) \
    .load()

# Show the remaining data
remaining_df.show()

# Write the remaining data to a CSV file
#csv_output_path = r"remaining_budgets.csv"
#remaining_df.write.csv(csv_output_path, header=True, mode="overwrite")

# Close the cursor and connection
cursor.close()
conn.close()
# Stop SparkSession
spark.stop()
