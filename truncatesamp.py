import mysql.connector
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="Raja@1234",
  database="mydatabase"
)
mycursor = mydb.cursor()
mycursor.execute("TRUNCATE TABLE BaseTable3")
mycursor.execute("TRUNCATE TABLE BaseTable2")
mycursor.execute("TRUNCATE TABLE Sample")
mycursor.execute("TRUNCATE TABLE SampleTable1")
mydb.commit()
