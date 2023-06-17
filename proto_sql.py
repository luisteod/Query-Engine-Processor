import csv
import mysql_handler
import postgres_handler
import os

schema = None
table_path = None
join_table_path = None


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


def read_csv(columns: list) -> list:
    with open(table_path, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        if columns == "*":
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
                lista.clear()


"""
returns status of FROM statement and fills the global "table_path" with table 
to be used in the query
"""


def FROM(
    input_table: str = None, join_flag: bool = False, join_table: str = None
) -> bool:
    global table_path
    global join_table_path
    global schema

    if (input_table is None) or (
        not check_existing_table(input_table, schema)
    ):  # error in finding from table
        print("error: expecting expression after 'from' clause")
        return False
    else:
        table_path = catch_table_path(input_table, schema)
        if join_flag == False:
            return True
        elif join_flag == True:
            if not check_existing_table(
                join_table, schema
            ):  # error in finding join table
                print("error: expecting expression after 'join' clause")
                return False
            else:
                join_table_path = catch_table_path(join_table, schema)
                return True


def SELECT(query_list: list) -> bool:
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
                    if FROM(table, join_flag, join_table):  # from with a table to join
                        read_csv(columns)
                        return True
                else:
                    print("error: expected expression after 'inner' clause")
                    return False
        elif FROM(table):
            read_csv(columns)  # TODO
        return True
    else:
        print("error : Select statement")


def INSERT(table: str, values: str):
    # TODO
    return


# splits the query with it's respectively statements
# and treat accordingly
def proto_token(query: str) -> bool:
    query = query.replace(", ", ",")
    query = query.replace(" ,", ",")

    # splits sql command using space as separator
    query_list = query.split()

    if query_list[0] == "select":
        return SELECT(query_list)

    elif query_list[0] == "delete":
        if query_list[1] == "from":
            query_list.remove("delete")
            query_list.remove("from")
        else:
            print("error: Expecting 'from' after 'delete' clause")
            return 0

    elif query_list[0] == "insert":
        if query_list[1] == "into":
            query_list.remove("insert")
            query_list.remove("into")
            table = query_list[0]
            value = query_list[1]
            INSERT(table, value)
        else:
            print("error: Expecting 'into' clause after 'insert' clause")
            return 0

    elif query_list[0] == "update":
        query_list.remove("update")
        # TODO

    else:
        var = "'" + query + "'" + ":" + "command not found"
        print(var)
        return False


def check_existing_table(table: str, schema):
    path = catch_table_path(table, schema)
    return os.path.exists(path)


def catch_table_path(table: str, schema):
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
        proto_token(query)
    else:
        print("error : Schema not found in PROTO_SQL server")


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
