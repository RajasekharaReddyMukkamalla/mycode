import mysql.connector
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="Raja@1234",
  database="mydatabase"
)
mycursor = mydb.cursor()
mycursor.execute("INSERT INTO Sample select * from ( select * , ROW_NUMBER () over (partition by TransactionId order by TransactionTimeStamp desc,FIELD(Status,'Delivered   ','Shipped     ','Ordered     ')) as rownum  from BaseTable3 ) b1")
mycursor.execute("UPDATE Sample SET Active_Index='N' where row_num > 1")
mycursor.execute(" truncate table basetable3")
mycursor.execute("INSERT INTO BaseTable3(TransactionId, CustomerId, CustomerName, CustAddId, ProductId, ProductName, ProductPrice, ProductQuantity, Status, TransactionTimeStamp, OrderedDate, ShipmentDate, DeliveredDate, Active_Index) select TransactionId, CustomerId, CustomerName, CustAddId, ProductId, ProductName, ProductPrice, ProductQuantity, Status, TransactionTimeStamp, OrderedDate, ShipmentDate, DeliveredDate, Active_Index from Sample")
mydb.commit()
