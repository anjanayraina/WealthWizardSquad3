import oracledb
import cx_Oracle

class DBHelper:
    def __init__(self, user, password, host, port, sid):
        self.dsn = f"{host}:{port}/{sid}"
        self.user = user
        self.password = password
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = oracledb.connect(
                user=self.user,
                password=self.password,
                dsn=self.dsn
            )
            self.cursor = self.connection.cursor()
            print("Database connection established")
        except oracledb.DatabaseError as e:
            print(f"Error connecting to the database: {e}")
            raise

    def execute_query(self, query, params=None, commit=True):
        if self.cursor is None:
            raise ValueError("Cursor is not initialized. Call connect() first.")
        try:
            self.cursor.execute(query, params or ())
            if commit:
                self.connection.commit()

            if query.strip().upper().startswith("SELECT") :
                return self.cursor.fetchall()
            else:
                return None
        except oracledb.DatabaseError as e:
            print(f"Exception occured")
            raise

    def close(self):
        if self.cursor:
            self.cursor.close()
            print("Cursor closed.")
        if self.connection:
            self.connection.close()
            print("Database connection closed.")

class SchedulerManager:
    def __init__(self, spark, dsn, user, password):
        # Initialize the SchedulerManager with Spark session and database connection details
        self.spark = spark  # Spark session for processing data
        self.dsn = dsn  # Data Source Name for the Oracle database
        self.user = user  # Username for database authentication
        self.password = password  # Password for database authentication

    def create_scheduler_job(self):
        print("Creating Scheduler Job...")
        try:
            # Establish a connection to the Oracle database with SYSDBA privileges
            connection = cx_Oracle.connect(self.user, self.password, self.dsn, mode=cx_Oracle.SYSDBA)
            cursor = connection.cursor()
            
            # Execute the PL/SQL block to create a scheduler job
            cursor.execute("""
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
            """)
            # Close the cursor and connection after successfully creating the job
            cursor.close()
            connection.close()
            print("Scheduler job created successfully.")
        except cx_Oracle.DatabaseError as e:
            # Handle any database errors that occur during the job creation
            print(f"Error creating scheduler job: {e}")

    def drop_scheduler_job(self):
        print("Dropping Scheduler Job...")
        try:
            # Establish a connection to the Oracle database with SYSDBA privileges
            connection = cx_Oracle.connect(self.user, self.password, self.dsn, mode=cx_Oracle.SYSDBA)
            cursor = connection.cursor()
            
            # Execute the PL/SQL block to drop the existing scheduler job
            cursor.execute("BEGIN DBMS_SCHEDULER.drop_job(job_name => 'CHECK_BUDGET_ALERTS_JOB'); END;")
            
            # Close the cursor and connection after successfully dropping the job
            cursor.close()
            connection.close()
            print("Scheduler job dropped successfully.")
        except cx_Oracle.DatabaseError as e:
            # Handle any database errors that occur during the job drop process
            print(f"Error dropping scheduler job: {e}")

    def close(self):
        # Inform that there is no need to close any persistent connections
        print("SchedulerManager does not maintain a persistent connection. No close operation required.")
