import findspark
findspark.init()
from pyspark.sql import SparkSession

class SparkManager:
    """
        A class to manage SparkSession
        - Uses JDBC to connect with Oracle SQL database for reading data
    """

    driver = "oracle.jdbc.driver.OracleDriver" # JDBC Driver

    def __init__(self,user,password,host,port,sid,path_to_jdbc = r".\ojdbc8-21.5.0.0.jar"):
        """
            Used to initialize the SparkSession
            Arguments:
            - `user`: User name in Oracle DB
            - `password`: Password for the user
            - `host`: The host IP where the DB server is accessible at
            - `port`: Port number
            - `sid`: SID name
            - `path_to_jdbc`: Path to the ojdbc.jar file
                - defaults to assuming it is in current directory
        """
        self.jdbc_url = f"jdbc:oracle:thin:@{host}:{port}:{sid}"
        self.__user = user
        self.__password = password
        self.path_to_jdbc = path_to_jdbc
        
        self.spark = SparkSession.builder.appName("Pyspark App")\
        .config("spark.driver.extraClassPath",path_to_jdbc).getOrCreate()

        
    def read_data(self,table):
        """
            Returns a spark dataframe of the `table` specified
        """

        df = self.spark.read.format("jdbc")\
        .option("url",self.jdbc_url) \
        .option("dbtable",table) \
        .option("user",self.__user) \
        .option("password",self.__password) \
        .option("driver",self.driver).load()

        df.show()
        # return df

    def stop_session(self):
        # Stop the SparkSession
        self.spark.stop()


if __name__=="__main__":
    from dotenv import load_dotenv
    import os
    load_dotenv()
    try:
        spark_sess = SparkManager(os.getenv("USER_SYSTEM"),os.getenv("PASSWORD"),
                                  os.getenv("HOST"),os.getenv("PORT"),os.getenv("SID"))
        spark_sess.read_data("budgets")
        spark_sess.stop_session()
    except Exception:
        spark_sess.stop_session()
        raise