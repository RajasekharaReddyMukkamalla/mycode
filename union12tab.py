import mysql.connector
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="Raja@1234",
  database="mydatabase"
)
mycursor = mydb.cursor()
mycursor.execute("INSERT INTO BaseTable2 SELECT * FROM SampleTable1")
mydb.commit()
