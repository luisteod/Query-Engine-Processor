import csv
import mysql_handler
import postgres_handler
import os

def insert(table:str, values:str):
    #TODO
    return


#splits the query with it's respectively statements
#and treat accordingly
def proto_token(query : str):

    query = query.replace(', ', ',') 
    query = query.replace(' ,', ',')

    #splits sql command using space as separator 
    query_list = query.split()

    if query_list[0] == 'select':
        #TODO
        return

    elif query_list[0] == 'delete':
        if query_list[1] == 'from':
            query_list.remove('delete')
            query_list.remove('from')

        else:
            print("error: Expecting 'from' after 'delete' clause")
            return 0

    elif query_list[0] == 'insert':
        if query_list[1] == 'into':
            query_list.remove('insert')
            query_list.remove('into')
            table = query_list[0]
            value = query_list[1]
            insert(table,value)

        else:
            print("error: Expecting 'into' clause after 'insert' clause")
            return 0
        
    elif query_list[0] == 'update':
        query_list.remove('update')
        #TODO

    else:
        var = "'" + query + "'" + ':' + 'command not found'
        print(var)
        return False


#returns a pointer to a table
def proto_from(input_table : str = None):

    if input_table is None:
        print("error: expecting expression after 'from' clause")
        return False

    
    #(TODO) a manner to take a especif table from CSV file

    return


def check_existing_schema(schema):
    path = catch_schema_path(schema)
    return os.path.exists(path)

def catch_table_path(table : str,schema):
    path = (catch_schema_path(schema)+'/tables/{}.csv').format(table)
    return path

def check_existing_table(table : str,schema):
    path = catch_table_path(table,schema)
    return os.path.exists(path)

def catch_schema_path(schema: str):
    path = (os.getcwd()+'/schemas/{}').format(schema)
    return path

def create_schema(schema) :
    path = catch_schema_path(schema)
    os.mkdir(path)
    os.mkdir(path+'/tables')

def data_import():
    server=None
    while not(server == 'mysql' or server == 'postgres'):
        print("Select the server (mysql or postgres)")
        server = input('>> ')
    if server == 'mysql':
        mysql_handler.mysqlimport()
    elif server == 'postgres':
        postgres_handler.postgresimport()
    return

def query():
     query = input('>> ')
     proto_token(query)


def main():
    #takes query from user
    answer = None
    while not(answer=='i' or answer=='q'):
        print("Import or query? (i/q)")
        answer = input('>> ')
    if answer=='i':
        data_import()
    else :
        query()    
   
if __name__ == '__main__':
    while True:
        main()