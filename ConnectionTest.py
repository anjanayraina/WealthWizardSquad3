import mysql.connector

mydb = mysql.connector.connect(
  host="localhost:1521:1521",
  user="system",
  password="welcome123"
)

print(mydb)