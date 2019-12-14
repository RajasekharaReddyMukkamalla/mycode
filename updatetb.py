import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="Raja@1234",
  database="mydatabase"
)

mycursor = mydb.cursor()
sql="WITH cte AS (SELECT b.*, ROW_NUMBER() OVER ( PARTITION BY b.TransactionId ORDER BY b.TransactionTimeStamp DESC) row_num FROM BaseTable3 b)"
mycursor.execute(sql)
mydb.commit()


