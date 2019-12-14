import mysql.connector
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="Raja@1234",
  database="mydatabase"
)
f = open("C:\\Users\\Subha Reddy\\Downloads\\Day2.txt", "r")
l1=[6,6,20,5,7,30,6,4,12,20,11,11,11]
for x in f:
  l2=[]	
  for i in range(len(l1)):
    l2.append(f.read(l1[i]))
  mycursor = mydb.cursor()
  if(x):
    sql = "INSERT INTO SampleTable1 (TransactionId, CustomerId, CustomerName, CustAddId, ProductId, ProductName, ProductPrice, ProductQuantity, Status, TransactionTimeStamp, OrderedDate, ShipmentDate, DeliveredDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    val = (l2[0],l2[1],l2[2],l2[3],l2[4],l2[5],l2[6],l2[7],l2[8],l2[9],l2[10],l2[11],l2[12])
  mycursor.execute(sql, val)
  mycursor.execute("INSERT INTO BaseTable2 SELECT * FROM SampleTable1")
  mycursor.execute("TRUNCATE TABLE SampleTable1")
  mydb.commit()

print(mycursor.rowcount, "record inserted.")
