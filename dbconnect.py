import mysql.connector

mydb = mysql.connector.connect(host="localhost",user="root",passwd="Raja@1234")

print(mydb)
if(mydb):
    print("Good")
else:
    print("BAd")