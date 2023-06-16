import mysql.connector
import csv
import proto_sql

host_glob = 'localhost'
user_glob = 'root'
password_glob = 'Henrique312'
database_glob = 0

def mysqlconnect():

    conn_params = {
        'host':host_glob,
        'user':user_glob,
        'password':password_glob,
        'database': database_glob
    }

    try:
        db_connection=mysql.connector.connect(**conn_params)
    except:
        print("error : Schema not found")
        return 0
    
    print('Connected to server!')
    return db_connection

def mysql_check_table(table:str,cursor):
    try:
        query = ('select * from {}').format(table)
        cursor.execute(query)
        return 1
    except:
        return 0


def mysqlimport():
    
    global database_glob
   
    conn = None
    while not conn :
        print("Select schema: ")
        database_glob = input('>> ')
        conn = mysqlconnect() 
   
    cursor = conn.cursor(dictionary=True,buffered=True)
    
    print('Type the table to import : ')
    table = input('>> ')

    while True:
        if mysql_check_table(table,cursor) :
            break
        else :
            print("error : Table not exists in server")
            print('Type the table to import : ')
            table = input('>> ')   

    if not proto_sql.check_existing_schema(schema=database_glob) : 

        create_folder = None
        while not(create_folder == 'y' or create_folder == 'n') :
            print('Schema not found locally, want to create? (y/n)')
            create_folder = input('>> ')
        
        if create_folder=='y' :
            proto_sql.create_schema(schema=database_glob)
        else :
            return True
        
    if proto_sql.check_existing_table(table,schema=database_glob) :
        overwrite = None
        while not(overwrite == 'y' or overwrite =='n'):
            print('Table already exists, do you wanna overwrite ? (y/n)')
            overwrite = input('>> ')
        if overwrite == 'y':
            headers = cursor.column_names
            proto_sql.write_csv(table,cursor,headers,schema=database_glob)
        elif overwrite == 'n':
            return True
    else : 
        #create a new file with the name of table
        headers = cursor.column_names
        proto_sql.write_csv(table,cursor,headers,schema=database_glob)
            
    
    cursor.close()
    conn.close()

    return True
    
