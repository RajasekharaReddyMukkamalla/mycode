import mysql.connector
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="Raja@1234",
  database="mydatabase"
)
mycursor = mydb.cursor()
mycursor.execute("DELETE FROM BaseTable2 WHERE TransactionId LIKE ProductQuantity")
mydb.commit()
