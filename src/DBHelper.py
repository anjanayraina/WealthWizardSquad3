import oracledb

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
            print("Database connection established.")
        except oracledb.DatabaseError as e:
            print(f"Error connecting to the database: {e}")
            raise

    def execute_query(self, query, params=None, commit=False):
        if self.cursor is None:
            raise ValueError("Cursor is not initialized. Call connect() first.")
        try:
            self.cursor.execute(query, params or ())
            if commit:
                self.connection.commit()

            if query.strip().upper().startswith("SELECT"):
                return self.cursor.fetchall()
            else:
                return None
        except oracledb.DatabaseError as e:
            print(f"A budget with this ID already exists.")
            raise

    def close(self):
        if self.cursor:
            self.cursor.close()
            print("Cursor closed.")
        if self.connection:
            self.connection.close()
            print("Database connection closed.")
