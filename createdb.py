import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="Raja@1234"
) 


mycursor = mydb.cursor()

mycursor.execute("CREATE DATABASE mydatabase")
