import pyodbc
import csv
import os
from tabulate import tabulate
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(), override=True)

sql_server = os.environ.get("sql_server")
sql_database = os.environ.get("sql_database")
sql_username = os.environ.get("sql_username")
sql_password =os.environ.get("sql_password")
tbl_string = os.environ.get("tbl_string")
directory_dest =os.environ.get("directory_dest")

def get_table_names(server, database, username, password, tbl_string):
    try:
        # Set up the SQL Server connection string
        connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}"

        # Establish the database connection
        connection = pyodbc.connect(connection_string)

        # Create a cursor object to interact with the database
        cursor = connection.cursor()

        # Execute a query to select table names from the Information Schema
        query = f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' and  TABLE_NAME like ('{tbl_string}%');"
        print(query)
        cursor.execute(query)

        # Fetch all table names
        table_names = [row.TABLE_NAME for row in cursor.fetchall()]

        # Close the cursor and connection
        cursor.close()
        connection.close()

        return table_names

    except Exception as e:
        print("Error getting table names:", str(e))
        return None

def get_table_data(server, database, username, password, tbl_string):
    try:
        # Set up the SQL Server connection string
        connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}"

        # Establish the database connection
        connection = pyodbc.connect(connection_string)

        # Create a cursor object to interact with the database
        cursor = connection.cursor()

        # Execute a query to select table names from the Information Schema
        query = f'SELECT * FROM {tbl_string}'
        cursor.execute(query)

        # Fetch all rows from the result set
        rows = cursor.fetchall()
        column_names = [column[0] for column in cursor.description]


        # Close the cursor and connection
        cursor.close()
        connection.close()

        return rows, column_names

    except Exception as e:
        print(f"Error loading table data {tbl_string}")

def export_table_names_to_csv(table_names, directory_dest,sql_server, sql_database, sql_username, sql_password):
     for table_name in table_names:
        file_path = os.path.join(directory_dest, f'{table_name}.csv')
        print(file_path)
        table_data, column_headers = get_table_data(sql_server, sql_database, sql_username, sql_password, table_name)
        try:
            with open(file_path, mode='w', newline='') as file:
                writer = csv.writer(file)
                # Write the header
                writer.writerow(column_headers)
                writer.writerows(table_data)
                print(f"{table_name} Data has been writter to {file_path} ")
        except Exception as e:
            print(f"Error loading table data into the File {file_path}")


# Get table names from Information Schema
table_names_list = get_table_names(sql_server, sql_database, sql_username, sql_password, tbl_string)
# write the each table data into the file
export_table_names_to_csv(table_names_list,directory_dest,sql_server, sql_database, sql_username, sql_password)
# upload the files to the ADLS

