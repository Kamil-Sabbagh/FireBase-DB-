import pyrebase
import psycopg2
import firebase_admin
import time
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
from datetime import datetime
import timeit

con = psycopg2.connect(database="DVD_Rental", user="postgres",
                       password="postgres", host="127.0.0.1", port="5432")

original_tables = [ ]
db_cursor = con.cursor()
curs = con.cursor()

def GetTableList(t_schema):
    # Retrieve the table list
    s = ""
    s += "SELECT"
    s += " table_schema"
    s += ", table_name"
    s += " FROM information_schema.tables"
    s += " WHERE"
    s += " ("
    s += " table_schema = '" + t_schema + "'"
    s += " )"
    s += " ORDER BY table_schema, table_name;"

    # Retrieve all the rows from the cursor
    db_cursor.execute(s)
    list_tables = db_cursor.fetchall()

    # Print the names of the tables
    for t_name_table in list_tables:
        #print(t_name_table[1] )
        original_tables.append(t_name_table[1])



t_schema = "public"
GetTableList(t_schema)


# firebaseConfig = {
#     'apiKey': "AIzaSyDEjNSqGvvtoZ4c5LDnPoZJggikBgC5-p8",
#     'authDomain': "test-8d0da.firebaseapp.com",
#     'databaseURL': "https://test-8d0da-default-rtdb.firebaseio.com",
#     'projectId': "test-8d0da",
#     'storageBucket': "test-8d0da.appspot.com",
#     'messagingSenderId': "805802577582",
#     'appId': "1:805802577582:web:61dd106aeff2568c4ee73f",
#     'measurementId': "G-DSGCSN7XQG"
#   }
#
#
firebaseConfig = {
    'apiKey': "AIzaSyAfSnRHC913I0_zR5LdUZCk-xZs_YFio54",
    'authDomain': "assignment-2-e7286.firebaseapp.com",
    'databaseURL': "https://assignment-2-e7286-default-rtdb.firebaseio.com",
    'projectId': "assignment-2-e7286",
    'storageBucket': "assignment-2-e7286.appspot.com",
    'messagingSenderId': "926718650475",
    'appId': "1:926718650475:web:14adeb6a5d847e4594292e",
    'measurementId': "G-GWLPMN10JZ" }

firebase = pyrebase.initialize_app(firebaseConfig)

db = firebase.database()

total_time = time.time()


all_tables = { }
for table in original_tables :
    curs.execute(f"SELECT * FROM {table} " )
    colnames = [desc[0] for desc in curs.description]
    data = {}
    a_table = { }
    for index , row in enumerate(curs) :

        data = { }
        for i in range(len(colnames) ) :
            temp = ""
            if not isinstance(row[i], int ) and not isinstance(row[i] ,str ):
                temp = str(row[i])
                data[colnames[i]] = temp
                continue

            data[colnames[i]] = row[i]
        a_table [ index+1 ] = data

    all_tables[table] = [a_table]

# cred = credentials.Certificate('certificate.json')
#
# firebase_admin.initialize_app(cred, {
#     'databaseURL' : 'https://assignment-2-e7286-default-rtdb.firebaseio.com/'
# })
#
# db.reference().delete()
#
# print(all_tables)
print(f"Installation time : {time.time() - total_time} seconds.")
bulking_time = time.time()

db.set(all_tables)
print(f"bulking time : {time.time() - bulking_time} seconds.")
print(f"total time : {time.time() - total_time} second." )
