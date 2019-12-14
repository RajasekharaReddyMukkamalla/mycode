import mysql.connector
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="Raja@1234",
  database="mydatabase"
)
mycursor = mydb.cursor()
mycursor.execute("ALTER TABLE BaseTable2 MODIFY COLUMN STR_TO_DATE(TransactionTimeStamp, "%d-%b-%Y %H:%i:%S")")
mydb.commit()
print("Effected")
