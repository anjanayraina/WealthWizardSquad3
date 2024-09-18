import oracledb
import os


class DBHelper:
    def __init__(self, user, password, host, port, sid):
        """
        Initializes the DBHelper object with database connection details.

        Args:
        - user: Username for the database connection.
        - password: Password for the database connection.
        - host: Host address of the Oracle database.
        - port: Port number for the Oracle database.
        - sid: System identifier (SID) for the Oracle database.
        """
        # Format the Data Source Name (DSN) using the host, port, and sid
        self.dsn = f"{host}:{port}/{sid}"
        self.user = user
        self.password = password
        self.connection = None  # Database connection object
        self.cursor = None  # Cursor object for executing queries

    def connect(self):
        """
        Establishes a connection to the Oracle database using the provided credentials.
        Initializes the cursor for executing SQL queries.
        """
        try:
            # Create a connection to the Oracle database using the provided credentials and DSN
            self.connection = oracledb.connect(
                user=self.user,
                password=self.password,
                dsn=self.dsn
            )
            # Initialize a cursor to execute SQL queries
            self.cursor = self.connection.cursor()
            print("Database connection established")
        except oracledb.DatabaseError as e:
            # Print and raise an exception if the database connection fails
            print(f"Error connecting to the database: {e}")
            raise

    def execute_query(self, query, params=None, commit=True):
        """
        Executes a SQL query using the initialized cursor.

        Args:
        - query: The SQL query to be executed.
        - params: Optional parameters to be passed into the query (default is None).
        - commit: Whether to commit the transaction after execution (default is True).

        Returns:
        - For SELECT queries: The result of the query (a list of rows).
        - For non-SELECT queries: None.

        Raises:
        - ValueError: If the cursor is not initialized.
        - oracledb.DatabaseError: If there is a database error during query execution.
        """
        # Raise an error if the cursor is not initialized (connection might not be established)
        if self.cursor is None:
            raise ValueError("Cursor is not initialized. Call connect() first.")
        try:
            # Execute the provided query with optional parameters (empty tuple if none provided)
            self.cursor.execute(query, params or ())

            # If commit is True (for non-SELECT queries), commit the transaction to the database
            if commit:
                self.connection.commit()

            # Check if the query is a SELECT query, fetch and return all results if true
            if query.strip().upper().startswith("SELECT"):
                return self.cursor.fetchall()
            else:
                # For non-SELECT queries, no return value is needed
                return None
        except oracledb.DatabaseError as e:
            # Handle any database errors that occur during query execution
            print(f"Exception occurred during query execution: {e}")
            raise

    def close(self):
        """
        Closes the cursor and the database connection if they are initialized.
        """
        # Close the cursor if it was initialized
        if self.cursor:
            self.cursor.close()
            print("Cursor closed.")

        # Close the database connection if it was initialized
        if self.connection:
            self.connection.close()
            print("Database connection closed.")


class SchedulerManager:
    def __init__(self, spark):
        # Initialize the SchedulerManager with Spark session and DBHelper instance
        self.spark = spark  # Spark session for processing data
        self.db_helper = self.db_helper = DBHelper(
            user=os.getenv("USER_SYSTEM"),
            password=os.getenv("PASSWORD"),
            host=os.getenv("HOST"),
            port = os.getenv("PORT"),
            sid=os.getenv("SID")
        )

    def create_scheduler_job(self):
        print("Creating Scheduler Job...")
        try:
            # Establish a connection to the Oracle database using DBHelper
            self.db_helper.connect()
            
            # Define the PL/SQL block to create the scheduler job
            plsql_block = """
                BEGIN
                    DBMS_SCHEDULER.create_job (
                        job_name        => 'CHECK_BUDGET_ALERTS_JOB',  -- Name of the scheduler job
                        job_type        => 'PLSQL_BLOCK',  -- Type of job, here it's a PL/SQL block
                        job_action      => 'BEGIN check_budget_alerts; END;',  -- The action the job will perform
                        start_date      => SYSTIMESTAMP,  -- Start the job immediately
                        repeat_interval => 'FREQ=SECONDLY; INTERVAL=1',  -- Set the job to run every second
                        enabled         => TRUE  -- Enable the job immediately after creation
                    );
                END;
            """

            # Execute the PL/SQL block using DBHelper
            self.db_helper.execute_query(plsql_block, commit=True)
            print("Scheduler job created successfully.")
        except oracledb.DatabaseError as e:
            # Handle any database errors that occur during the job creation
            print(f"Error creating scheduler job: {e}")
        finally:
            # Close the DBHelper connection
            self.db_helper.close()

    def drop_scheduler_job(self):
        print("Dropping Scheduler Job...")
        try:
            # Establish a connection to the Oracle database using DBHelper
            self.db_helper.connect()
            
            # Define the PL/SQL block to drop the scheduler job
            plsql_block = "BEGIN DBMS_SCHEDULER.drop_job(job_name => 'CHECK_BUDGET_ALERTS_JOB'); END;"

            # Execute the PL/SQL block using DBHelper
            self.db_helper.execute_query(plsql_block, commit=True)
            print("Scheduler job dropped successfully.")
        except oracledb.DatabaseError as e:
            # Handle any database errors that occur during the job drop process
            print(f"Error dropping scheduler job: {e}")
        finally:
            # Close the DBHelper connection
            self.db_helper.close()

    def close(self):
        # No persistent connection is maintained by the SchedulerManager
        print("SchedulerManager does not maintain a persistent connection. No close operation required.")
