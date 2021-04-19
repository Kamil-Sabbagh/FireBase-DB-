import pyrebase
import psycopg2
import mysql.connector
import urllib
import sqlite3
import json
import flask
import datetime


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

#for table in original_tables :
#    db.push(table)

for table in original_tables :
    print(table)
    curs.execute(f"SELECT * FROM {table} " )
    colnames = [desc[0] for desc in curs.description]
    data = {}
    for index , row in enumerate(curs) :
        data = { }
        for i in range(len(colnames) ) :
            temp = ""
            if isinstance(row[i], datetime.datetime) :
                temp = str(row[i])
                data[colnames[i]] = temp
                continue

            data[colnames[i]] = row[i]
        db.child(table).child(str(index)).set(data)

    #i = 1
    #for x in db_cursor :
        #print(table , x)
        #

