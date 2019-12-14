import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="Raja@1234",
  database="mydatabase"
)

mycursor = mydb.cursor()

mycursor.execute("CREATE TABLE BaseTable2 ( Seqno int(5) primary key auto_increment, TransactionId varchar(6), CustomerID varchar(6), CustomerName VARCHAR(20), CustAddID varchar(5), ProductId varchar(7), ProductName varchar(30), ProductPrice varchar(6), ProductQuantity varchar(4), Status varchar(12), TransactionTimeStamp varchar(20), OrderedDate varchar(11), ShipmentDate varchar(11), DeliveredDate varchar(11), Active_Index varchar(5) default 'Y' )")
