import mysql.connector
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="Raja@1234",
  database="mydatabase"
)
"""f = open("C:\\Users\\Subha Reddy\\Downloads\\Day1.txt", "r")
l1=[6,6,20,5,7,30,6,4,12,20,11,11,11]
for x in f:
  l2=[]	
  for i in range(len(l1)):
    l2.append(f.read(l1[i]))"""
mycursor = mydb.cursor()
  #if(x):
sql = "INSERT INTO BaseTable3 (TransactionId, CustomerId, CustomerName, CustAddId, ProductId, ProductName, ProductPrice, ProductQuantity, Status, TransactionTimeStamp, OrderedDate, ShipmentDate, DeliveredDate) SELECT TransactionId, CustomerId, CustomerName, CustAddId, ProductId, ProductName, ProductPrice, ProductQuantity, Status, COALESCE(STR_TO_DATE(TransactionTimeStamp, '%d-%b-%Y %H:%i:%S'),STR_TO_DATE(TransactionTimeStamp, '%d/%b/%Y %H:%i:%S'),STR_TO_DATE(TransactionTimeStamp, '%d %b %Y %H:%i:%S')),OrderedDate, ShipmentDate, DeliveredDate FROM BaseTable2"
mycursor.execute(sql)
mydb.commit()

print(mycursor.rowcount, "record inserted.")
