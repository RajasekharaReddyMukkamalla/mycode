#BaseTable2 as LZ table, BaseTable3 as BASE Table 
import sys
import mysql.connector
mydb = mysql.connector.connect(
@ -10,7 +10,7 @@ mydb = mysql.connector.connect(
mycursor = mydb.cursor()

mycursor.execute("TRUNCATE TABLE BaseTable2")
mycursor.execute("TRUNCATE TABLE Sample")
mycursor.execute("TRUNCATE TABLE STG_Table")

f = open(sys.argv[1], "r")
l1=[6,6,20,5,7,30,6,4,12,20,11,11,11]
@ -27,12 +27,16 @@ f.close()

mycursor.execute("DELETE FROM BaseTable2 WHERE TransactionId LIKE ProductQuantity")

sql = "INSERT INTO BaseTable3 (TransactionId, CustomerId, CustomerName, CustAddId, ProductId, ProductName, ProductPrice, ProductQuantity, Status, TransactionTimeStamp, OrderedDate, ShipmentDate, DeliveredDate) SELECT TransactionId, CustomerId, CustomerName, CustAddId, ProductId, ProductName, ProductPrice, ProductQuantity, Status, COALESCE(STR_TO_DATE(TransactionTimeStamp, '%d-%b-%Y %H:%i:%S'),STR_TO_DATE(TransactionTimeStamp, '%d/%b/%Y %H:%i:%S'),STR_TO_DATE(TransactionTimeStamp, '%d %b %Y %H:%i:%S')),OrderedDate, ShipmentDate, DeliveredDate FROM BaseTable2"
sql = "INSERT INTO STG_Table(TransactionId, CustomerId, CustomerName, CustAddId, ProductId, ProductName, ProductPrice, ProductQuantity, Status, TransactionTimeStamp, OrderedDate, ShipmentDate, DeliveredDate) SELECT TransactionId, CustomerId, CustomerName, CustAddId, ProductId, ProductName, ProductPrice, ProductQuantity, Status, COALESCE(STR_TO_DATE(TransactionTimeStamp, '%d-%b-%Y %H:%i:%S'),STR_TO_DATE(TransactionTimeStamp, '%d/%b/%Y %H:%i:%S'),STR_TO_DATE(TransactionTimeStamp, '%d %b %Y %H:%i:%S')),OrderedDate, ShipmentDate, DeliveredDate FROM BaseTable2"
mycursor.execute(sql)
sql="UPDATE STG_Table S set S.Action_Index = 'U' where S.TransactionId in (SELECT b.TransactionId from BaseTable3 b where b.Active_Index = 'Y') "
mycursor.execute(sql)
sql="UPDATE BaseTable3 b set b.Active_Index = 'N' where b.TransactionId in (SELECT S.TransactionId from STG_Table S where S.Action_Index = 'U') "
mycursor.execute(sql)

mycursor.execute("INSERT INTO Sample select * from ( select * , ROW_NUMBER () over (partition by TransactionId order by TransactionTimeStamp desc,FIELD(Status,'Delivered   ','Shipped     ','Ordered     ')) as rownum  from BaseTable3 ) b1")
mycursor.execute("UPDATE Sample SET Active_Index='N' where row_num > 1")
mycursor.execute("TRUNCATE TABLE BaseTable3")
mycursor.execute("INSERT INTO BaseTable3(TransactionId, CustomerId, CustomerName, CustAddId, ProductId, ProductName, ProductPrice, ProductQuantity, Status, TransactionTimeStamp, OrderedDate, ShipmentDate, DeliveredDate, Active_Index) select TransactionId, CustomerId, CustomerName, CustAddId, ProductId, ProductName, ProductPrice, ProductQuantity, Status, TransactionTimeStamp, OrderedDate, ShipmentDate, DeliveredDate, Active_Index from Sample")
sql = "INSERT INTO BaseTable3 (TransactionId, CustomerId, CustomerName, CustAddId, ProductId, ProductName, ProductPrice, ProductQuantity, Status, TransactionTimeStamp, OrderedDate, ShipmentDate, DeliveredDate,Active_Index) SELECT TransactionId, CustomerId, CustomerName, CustAddId, ProductId, ProductName, ProductPrice, ProductQuantity, Status, TransactionTimeStamp, OrderedDate, ShipmentDate, DeliveredDate, Action_Index FROM STG_Table"
mycursor.execute(sql)
sql = "UPDATE BaseTable3 set Active_Index = 'Y' where Active_Index = 'I' or Active_Index = 'U'"
mycursor.execute(sql)

mydb.commit()
