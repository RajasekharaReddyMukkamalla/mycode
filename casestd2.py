#BaseTable2 as LZ table, BaseTable3 as BASE Table 
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
mycursor.execute("TRUNCATE TABLE STG_Table")

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

sql = "INSERT INTO STG_Table(TransactionId, CustomerId, CustomerName, CustAddId, ProductId, ProductName, ProductPrice, ProductQuantity, Status, TransactionTimeStamp, OrderedDate, ShipmentDate, DeliveredDate) SELECT TransactionId, CustomerId, CustomerName, CustAddId, ProductId, ProductName, ProductPrice, ProductQuantity, Status, COALESCE(STR_TO_DATE(TransactionTimeStamp, '%d-%b-%Y %H:%i:%S'),STR_TO_DATE(TransactionTimeStamp, '%d/%b/%Y %H:%i:%S'),STR_TO_DATE(TransactionTimeStamp, '%d %b %Y %H:%i:%S')),OrderedDate, ShipmentDate, DeliveredDate FROM BaseTable2"
mycursor.execute(sql)
sql="UPDATE STG_Table S set S.Action_Index = 'U' where S.TransactionId in (SELECT b.TransactionId from BaseTable3 b where b.Active_Index = 'Y') "
mycursor.execute(sql)
sql="UPDATE BaseTable3 b set b.Active_Index = 'N' where b.TransactionId in (SELECT S.TransactionId from STG_Table S where S.Action_Index = 'U') "
mycursor.execute(sql)

sql = "INSERT INTO BaseTable3 (TransactionId, CustomerId, CustomerName, CustAddId, ProductId, ProductName, ProductPrice, ProductQuantity, Status, TransactionTimeStamp, OrderedDate, ShipmentDate, DeliveredDate,Active_Index) SELECT TransactionId, CustomerId, CustomerName, CustAddId, ProductId, ProductName, ProductPrice, ProductQuantity, Status, TransactionTimeStamp, OrderedDate, ShipmentDate, DeliveredDate, Action_Index FROM STG_Table"
mycursor.execute(sql)
sql = "UPDATE BaseTable3 set Active_Index = 'Y' where Active_Index = 'I' or Active_Index = 'U'"
mycursor.execute(sql)

createdate = sys.argv[1]
sql = "INSERT INTO Audit_key (CreateDate,Source_Name) VALUES (%s,%s)"
val = (createdate[:-4],"TXT")
mycursor.execute(sql, val)

sql = "INSERT INTO Audit_Status(Audit_Key) SELECT max(Audit_Key) from Audit_key"
#val = ("LZ_Policy",,createdate[:-4],createdate[:-4],"TXT")
mycursor.execute(sql)
sql = "UPDATE Audit_Status A set A.TableName = 'LZ_Policy' where A.Audit_Key = (SELECT max(b.Audit_Key) from Audit_key b) and A.Status is NULL "
mycursor.execute(sql)
sql = "UPDATE Audit_Status A set A.CreateDate = (SELECT MAX(b.CreateDate) from Audit_key b) where A.Audit_Key = (SELECT max(b.Audit_Key) from Audit_key b) and A.Status is NULL"
mycursor.execute(sql)
sql = "UPDATE Audit_Status A set A.UpdateDate = (SELECT MAX(b.CreateDate) from Audit_key b) where A.Audit_Key = (SELECT max(b.Audit_Key) from Audit_key b) and A.Status is NULL"
mycursor.execute(sql)
sql = "UPDATE Audit_Status A set A.Source_Count = (SELECT COUNT(TransactionId) from BaseTable2 ) where A.Audit_Key = (SELECT max(b.Audit_Key) from Audit_key b) and A.Status is NULL"
mycursor.execute(sql) 
sql = "UPDATE Audit_Status A set A.Target_Count = (SELECT COUNT(TransactionId) from BaseTable2 ) where A.Audit_Key = (SELECT max(b.Audit_Key) from Audit_key b) and A.Status is NULL"
mycursor.execute(sql)
sql = "UPDATE Audit_Status A set A.Target_Insert = (SELECT COUNT(TransactionId) from BaseTable2 ) where A.Audit_Key = (SELECT max(b.Audit_Key) from Audit_key b) and A.Status is NULL"
mycursor.execute(sql)
sql = "UPDATE Audit_Status A set A.Target_Update = 0 where A.Audit_Key = (SELECT max(b.Audit_Key) from Audit_key b) and A.Status is NULL"
mycursor.execute(sql)
sql = "UPDATE Audit_Status A set A.Status = 'M' where Source_Count = Target_Count and A.Audit_Key = (SELECT max(b.Audit_Key) from Audit_key b) and A.Status is NULL"
mycursor.execute(sql)
sql = "UPDATE Audit_Status A set A.Status = 'N' where Source_Count != Target_Count and A.Audit_Key = (SELECT max(b.Audit_Key) from Audit_key b) and A.Status is NULL"
mycursor.execute(sql)

sql = "INSERT INTO Audit_Status(Audit_Key) SELECT max(Audit_Key) from Audit_key"
mycursor.execute(sql)
sql = "UPDATE Audit_Status A set A.Source_Name = 'LZTAB' where A.Audit_Key = (SELECT max(b.Audit_Key) from Audit_key b) and A.Status is NULL"
mycursor.execute(sql)
sql = "UPDATE Audit_Status A set A.TableName = 'STG_Policy' where A.Audit_Key = (SELECT max(b.Audit_Key) from Audit_key b) and A.Status is NULL"
mycursor.execute(sql)
sql = "UPDATE Audit_Status A set A.CreateDate = (SELECT MAX(b.CreateDate) from Audit_key b) where A.Audit_Key = (SELECT max(b.Audit_Key) from Audit_key b) and A.Status is NULL"
mycursor.execute(sql)
sql = "UPDATE Audit_Status A set A.UpdateDate = (SELECT MAX(b.CreateDate) from Audit_key b) where A.Audit_Key = (SELECT max(b.Audit_Key) from Audit_key b) and A.Status is NULL"
mycursor.execute(sql)
sql = "UPDATE Audit_Status A set A.Source_Count = (SELECT COUNT(TransactionId) from BaseTable2 ) where A.Audit_Key = (SELECT max(b.Audit_Key) from Audit_key b) and A.Status is NULL"
mycursor.execute(sql) 
sql = "UPDATE Audit_Status A set A.Target_Count = (SELECT COUNT(TransactionId) from STG_Table ) where A.Audit_Key = (SELECT max(b.Audit_Key) from Audit_key b) and A.Status is NULL"
mycursor.execute(sql)
sql = "UPDATE Audit_Status A set A.Target_Insert = (SELECT COUNT(TransactionId) from BaseTable2 ) where A.Audit_Key = (SELECT max(b.Audit_Key) from Audit_key b) and A.Status is NULL"
mycursor.execute(sql)
sql = "UPDATE Audit_Status A set A.Target_Update = 0 where A.Audit_Key = (SELECT max(b.Audit_Key) from Audit_key b) and A.Status is NULL"
mycursor.execute(sql)
sql = "UPDATE Audit_Status A set A.Status = 'M' where Source_Count = Target_Count and A.Audit_Key = (SELECT max(b.Audit_Key) from Audit_key b) and A.Status is NULL"
mycursor.execute(sql)
sql = "UPDATE Audit_Status A set A.Status = 'N' where Source_Count != Target_Count and A.Audit_Key = (SELECT max(b.Audit_Key) from Audit_key b) and A.Status is NULL"
mycursor.execute(sql)

sql = "INSERT INTO Audit_Status(Audit_Key) SELECT max(Audit_Key) from Audit_key"
mycursor.execute(sql)
sql = "UPDATE Audit_Status A set A.Source_Name = 'STGTB' where A.Audit_Key = (SELECT max(b.Audit_Key) from Audit_key b) and A.Status is NULL"
mycursor.execute(sql)
sql = "UPDATE Audit_Status A set A.TableName = 'BASE' where A.Audit_Key = (SELECT max(b.Audit_Key) from Audit_key b) and A.Status is NULL"
mycursor.execute(sql)
sql = "UPDATE Audit_Status A set A.CreateDate = (SELECT MAX(b.CreateDate) from Audit_key b) where A.Audit_Key = (SELECT max(b.Audit_Key) from Audit_key b) and A.Status is NULL"
mycursor.execute(sql)
sql = "UPDATE Audit_Status A set A.UpdateDate = (SELECT MAX(b.CreateDate) from Audit_key b) where A.Audit_Key = (SELECT max(b.Audit_Key) from Audit_key b) and A.Status is NULL"
mycursor.execute(sql)
sql = "UPDATE Audit_Status A set A.Source_Count = (SELECT COUNT(TransactionId) from STG_Table) where A.Audit_Key = (SELECT max(b.Audit_Key) from Audit_key b) and A.Status is NULL"
mycursor.execute(sql) 
sql = "UPDATE Audit_Status A set A.Target_Count = (SELECT COUNT(TransactionId) from BaseTable3 ) where A.Audit_Key = (SELECT max(b.Audit_Key) from Audit_key b) and A.Status is NULL"
mycursor.execute(sql)
sql = "UPDATE Audit_Status A set A.Target_Insert = (SELECT COUNT(TransactionId) from STG_Table WHERE Action_Index = 'I') where A.Audit_Key = (SELECT max(b.Audit_Key) from Audit_key b) and A.Status is NULL"
mycursor.execute(sql)
sql = "UPDATE Audit_Status A set A.Target_Update = (SELECT COUNT(TransactionId) from STG_Table WHERE Action_Index = 'U') where A.Audit_Key = (SELECT max(b.Audit_Key) from Audit_key b) and A.Status is NULL"
mycursor.execute(sql)
sql = "UPDATE Audit_Status A set A.Status = 'M' where Source_Count = Target_Count and A.Audit_Key = (SELECT max(b.Audit_Key) from Audit_key b) and A.Status is NULL"
mycursor.execute(sql)
sql = "UPDATE Audit_Status A set A.Status = 'N' where Source_Count != Target_Count and A.Audit_Key = (SELECT max(b.Audit_Key) from Audit_key b) and A.Status is NULL"
mycursor.execute(sql)

mydb.commit()