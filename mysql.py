import MySQLdb
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
        db_connection=MySQLdb.Connection(**conn_params)
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

def write_csv(table:str,cursor,schema):
    path_for_file = proto_sql.catch_table_path(table,schema)
    file = open(path_for_file,"x")
    #TODO

def mysqlimport():
    
    global database_glob
   
    conn = None
    while not conn :
        print("Select schema: ")
        database_glob = input('>> ')
        conn = mysqlconnect() 
   
    cursor = MySQLdb.cursors.SSCursor(conn)
    
    print('Type the table to import : ')
    table = input('>> ')

    if mysql_check_table(table,cursor) :

        if not proto_sql.check_existing_schema(schema=database_glob) : 

            create_folder = None
            while not(create_folder == 'y' or create_folder == 'n') :
                print('Schema not found locally, want to create? (y/n)')
                create_folder = input('>> ')
            
            if create_folder=='y' :
                proto_sql.create_schema(schema=database_glob)
            else :
                return 0
            
        if proto_sql.check_existing_table(table,schema=database_glob) :
            overwrite = None
            while not(overwrite == 'y' or overwrite =='n'):
                print('Table already exists, do you wanna overwrite ? (y/n)')
                overwrite = input('>> ')
            if overwrite == 'y':
                write_csv(table,cursor,schema=database_glob)
            elif overwrite == 'n':
                return 0
        else : 

            #create a new file with the name of table
            write_csv(table,cursor,schema=database_glob)
            
    else :
        print("error : Table not exists in server")
        return 0

    for row in cursor:
        print(row)
    
    cursor.close()
    conn.close()
    
