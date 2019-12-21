#BaseTable2 as lz table, BaseTable3 as Destination Table, Sample as intermediate 
import sys
import mysql.connector
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="Raja@1234",
  database="mydatabase"
)
mycursor = mydb.cursor()

mycursor.execute("TRUNCATE TABLE BaseTable2")
mycursor.execute("TRUNCATE TABLE Sample")

f = open(sys.argv[1], "r")
l1=[6,6,20,5,7,30,6,4,12,20,11,11,11]
for x in f:
  l2=[]	
  for i in range(len(l1)):
    l2.append(f.read(l1[i]))
  mycursor = mydb.cursor()
  if(x):
    sql = "INSERT INTO BaseTable2 (TransactionId, CustomerId, CustomerName, CustAddId, ProductId, ProductName, ProductPrice, ProductQuantity, Status, TransactionTimeStamp, OrderedDate, ShipmentDate, DeliveredDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    val = (l2[0],l2[1],l2[2],l2[3],l2[4],l2[5],l2[6],l2[7],l2[8],l2[9],l2[10],l2[11],l2[12])
  mycursor.execute(sql, val)
f.close()

mycursor.execute("DELETE FROM BaseTable2 WHERE TransactionId LIKE ProductQuantity")

sql = "INSERT INTO BaseTable3 (TransactionId, CustomerId, CustomerName, CustAddId, ProductId, ProductName, ProductPrice, ProductQuantity, Status, TransactionTimeStamp, OrderedDate, ShipmentDate, DeliveredDate) SELECT TransactionId, CustomerId, CustomerName, CustAddId, ProductId, ProductName, ProductPrice, ProductQuantity, Status, COALESCE(STR_TO_DATE(TransactionTimeStamp, '%d-%b-%Y %H:%i:%S'),STR_TO_DATE(TransactionTimeStamp, '%d/%b/%Y %H:%i:%S'),STR_TO_DATE(TransactionTimeStamp, '%d %b %Y %H:%i:%S')),OrderedDate, ShipmentDate, DeliveredDate FROM BaseTable2"
mycursor.execute(sql)

mycursor.execute("INSERT INTO Sample select * from ( select * , ROW_NUMBER () over (partition by TransactionId order by TransactionTimeStamp desc,FIELD(Status,'Delivered   ','Shipped     ','Ordered     ')) as rownum  from BaseTable3 ) b1")
mycursor.execute("UPDATE Sample SET Active_Index='N' where row_num > 1")
mycursor.execute("TRUNCATE TABLE BaseTable3")
mycursor.execute("INSERT INTO BaseTable3(TransactionId, CustomerId, CustomerName, CustAddId, ProductId, ProductName, ProductPrice, ProductQuantity, Status, TransactionTimeStamp, OrderedDate, ShipmentDate, DeliveredDate, Active_Index) select TransactionId, CustomerId, CustomerName, CustAddId, ProductId, ProductName, ProductPrice, ProductQuantity, Status, TransactionTimeStamp, OrderedDate, ShipmentDate, DeliveredDate, Active_Index from Sample")

mydb.commit()