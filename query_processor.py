import csv
import mysql_handler
import postgres_handler
import os

schema = None
table_path = None
join_table_path = None

commands = {}


def write_csv(table: str, cursor, colum_names: list, schema: str) -> bool:
    path_for_file = catch_table_path(table, schema)
    table_data = []
    tables_data = {}  # Dicionário de tabelas(listas) = Nosso banco de dados

    for row in cursor:
        table_data.append(row)

    tables_data[table] = table_data
    # headers = cursor.column_names

    with open(path_for_file, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=colum_names)
        writer.writeheader()
        writer.writerows(table_data)

#returns a list of a dict with keys as column name of a csv file
def read_csv(table_path:str) -> list:
    with open(table_path, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        data = []
        for row in reader:
            data.append(row)
        return data

        """ columns = reader.fieldnames
        table = {}
        lista = []
        for field in columns:
            for row in reader:
                lista.append(row.get(field))
            table.update((field,lista))
            lista.clear() """
            
        

        """ if columns == "*":
            lista = []
            columns = reader.fieldnames
            print(columns)
            for row in reader:
                for iter_col in columns:
                    lista.append(row.get(iter_col))
                print(lista)
                lista.clear()

        else:
            lista = []
            print(columns)
            for row in reader:
                for iter_col in columns:
                    lista.append(row.get(iter_col))
                print(lista)
                lista.clear() """

def tuple_value(data:tuple) -> str:
    return data[0]

def _join() -> bool:

    if commands["on"] != None:
        join_column = tuple_value(commands["on"])
        join_column = join_column.split('=')
        if len(join_column) != 2:
            print("Error : Wrong argument near {}".format(join_column))
            return 0
        else:
            var = join_column[0].split('.')
            table_1 = var[0]
            column_table_1 = var[1]

            var = join_column[1].split('.')
            table_2 = var[0]
            column_table_2 = var[1]
    elif commands["using"] != None:
        join_column = tuple_value(commands["using"])
        var = join_column.split('.')
        table = var[0]
        column = var[1]

def _from() -> list:
    global schema
    global commands
    data = []

    if commands["join"] != None:
        _join()
    else:
        table = tuple_value(commands["from"])
        if check_existing_table(table,schema):
            table_path = catch_table_path(table,schema)
            data = read_csv(table_path=table_path)
            return data


    # if (input_table is None) or (
    #     not check_existing_table(input_table, schema)
    # ):  # error in finding from table
    #     print("error: expecting expression after 'from' clause")
    #     return False
    # else:
    #     table_path = catch_table_path(input_table, schema)
    #     data = read_csv(table_path)
    #     if join_flag == False:            
    #         return True
    #     elif join_flag == True:
    #         if not check_existing_table(
    #             join_table, schema
    #         ):  # error in finding join table
    #             print("error: expecting expression after 'join' clause")
    #             return False
    #         else:
    #             join_table_path = catch_table_path(join_table, schema)
    #             return True


def _select(query_list: list) -> bool:
    if len(query_list) > 2:
        table = query_list[3]
        if query_list[1] == "*":
            columns = "*"
        else:
            columns = query_list[1].split(",")  # catch list of columns required by user
        if len(query_list) > 4:  # user wants a join
            if query_list[5] == "inner":
                if query_list[6] == "join":
                    join_table = query_list[7]
                    join_flag = True
                    if _from(table, join_flag, join_table):  # from with a table to join
                        read_csv(columns)
                        return True
                else:
                    print("error: expected expression after 'inner' clause")
                    return False
        elif _from(table):
            read_csv(columns)  # TODO
        return True
    else:
        print("error : Select statement")


def _update():
    return

def _insert():
    return



# splits the query with it's respectively statements
# and treat accordingly
def parser(query: str) -> bool:

    global commands

    commands = {

    "select": (None),
    "update": (None),
    "set" : (None),
    "insert": (None),
    "delete":(None),
    "into" : (None),
    "values": (None),
    "from": (None),
    "join": (None),
    "on" : (None),
    "using" : (None),
    "where": (None),
    "and": (None),
    "or": (None),
    "order by": (None),
}
    
    query = query.replace(", ", ",")
    query = query.replace(" ,", ",")
    query = query.replace(" =","=")
    query = query.replace("= ","=")

    # splits sql command using space as separator
    query_list = query.split()

    #extract table in query
    if "from" in query_list:
        i = query_list.index("from")
        table = query_list[i+1]
        commands["from"] = table,i
        #extract join argument
        if "join" in query_list:
            i = query_list.index("join")
            join_table = query_list[i+1]
            commands["join"] = join_table,i
            if join_table in commands:
                print("Error : Wrong argument near {}".format(join_table))
                return 0
            if "on" in query_list:
                i = query_list.index("on")
                join_column = query_list[i+1]
                commands["on"] = join_column,i
                if join_column in commands:
                    print("Error : Wrong argument near {}".format(join_column))
                    return 0
            elif "using" in query_list:
                i = query_list.index("using")
                join_column = query_list[i+1]
                commands["using"] = join_column,i
                if join_column in commands:
                    print("Error : Wrong arguments near {}".format(join_column))
                    return 0
            else:
                print("Error : Wrong argument near  {}".format(join_table))
                return 0
            
    elif "update" in query_list:
        i = query_list.index("update")
        table = query_list[i+1]
        commands["update"] = table,i
    elif "into" in query_list:
        i = query_list.index("into")
        table = query_list[i+1]
        commands["into"] = table,i
    else:
        print("Error : Wrong argument near {}".format(table))
        return 0
    
    #verification if arguments is part o commands
    if table in commands:
        print("Error : wrong argument {}".format(table))
        return 0

    #TODO
    data = _from()

    if "select" in query_list:
        i = query_list.index("select")
        # Take argument for select
        columns = query_list[i + 1]
        commands["select"] = columns,i
        _select(data,columns)
    # elif "update" in query_list:
    #     i = query_list.index("set")
    #     #TODO
    # elif "insert" in query_list:
    #     i = query_list.index("values")
    #     #TODO
    # else:
    #     print("Error : unexpected")
    #     return 0

def check_existing_table(table: str, schema:str):
    path = catch_table_path(table, schema)
    return os.path.exists(path)


def catch_table_path(table: str, schema:str):
    path = (catch_schema_path(schema) + "/tables/{}.csv").format(table)
    return path


def check_existing_schema(schema):
    path = catch_schema_path(schema)
    return os.path.exists(path)


def catch_schema_path(schema: str):
    path = (os.getcwd() + "/schemas/{}").format(schema)
    return path


def create_schema(schema):
    path = catch_schema_path(schema)
    os.mkdir(path)
    os.mkdir(path + "/tables")


def data_import():
    server = None
    while not (server == "mysql" or server == "postgres"):
        print("Select the server (mysql or postgres)")
        server = input(">> ")
    if server == "mysql":
        mysql_handler.mysqlimport()
    elif server == "postgres":
        postgres_handler.postgresimport()
    return


def query():
    global schema

    print("Select schema :")
    schema = input(">> ")
    if check_existing_schema(schema):
        print("Type query : ")
        query = input(">> ")
        parser(query)
    else:
        print("error : Schema not found in query_processor server")


def main():
    # takes query from user
    answer = None
    while not (answer == "i" or answer == "q"):
        print("Import or query? (i/q)")
        answer = input(">> ")
    if answer == "i":
        data_import()
    else:
        query()


if __name__ == "__main__":
    while True:
        main()
