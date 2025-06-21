from flask import Flask, render_template, request, flash
import pandas as pd
import pyodbc
import os
import numpy as np

app = Flask(__name__)
app.secret_key = 'your_secret_key' 

databases = []

connection_string_template = r"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={};DATABASE={};Trusted_Connection=yes;"

csv_directory = ''

def get_db_connection(server, database):
    connection_string = connection_string_template.format(server, database)
    return pyodbc.connect(connection_string)

def get_databases(server):
    try:
        conn = pyodbc.connect(f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};Trusted_Connection=yes;")
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sys.databases")
        databases = [row.name for row in cursor.fetchall()]
        conn.close()
        return databases
    except Exception as e:
        flash(f"Error fetching databases: {str(e)}", 'danger')
        return []


def get_table_names(server, database):
    try:
        conn = get_db_connection(server, database)
        cursor = conn.cursor()
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
        tables = [row.TABLE_NAME for row in cursor.fetchall()]
        conn.close()
        return tables
    except Exception as e:
        flash(f"Error fetching table names: {str(e)}", 'danger')
        return []


def get_matching_files(directory, tables):
    try:
        files = {os.path.splitext(file)[0]: file for file in os.listdir(directory) if os.path.isfile(os.path.join(directory, file))}
        return [table for table in tables if table in files]
    except Exception as e:
        flash(f"Error matching files: {str(e)}", 'danger')
        return []


def handle_data(server, database, selected_tables, action):
    try:
        conn = get_db_connection(server, database)
        cursor = conn.cursor()
        messages = []

        for table_name in selected_tables:
            try:
                file_path = os.path.join(csv_directory, f"{table_name}.csv")
                if not os.path.exists(file_path):
                    messages.append(f"❌ Error: File '{table_name}.csv' not found in directory.")
                    continue

                df = pd.read_csv(file_path, dtype=str)
                df.replace({'NULL': None, 'null': None, '': None}, inplace=True)

                for col in df.columns:
                    if df[col].isnull().all():
                        df[col].fillna('N/A', inplace=True)

                    if col.upper() == 'PERIOD':
                        df[col].fillna('DEFAULT_PERIOD', inplace=True)

                placeholders = ', '.join(['?'] * len(df.columns))
                insert_query = f"INSERT INTO dbo.{table_name} ({', '.join(df.columns)}) VALUES ({placeholders})"
                data = [tuple(map(lambda x: x.item() if isinstance(x, (np.int64, np.float64)) else x, row)) for row in df.replace({np.nan: None}).values]

                if action == 'truncate':
                    cursor.execute(f"TRUNCATE TABLE dbo.{table_name}")
                    conn.commit()
                    cursor.executemany(insert_query, data)
                    conn.commit()
                    messages.append(f"✅ Success: Data truncated and uploaded to '{table_name}'.")

                elif action == 'append':
                    cursor.executemany(insert_query, data)
                    conn.commit()
                    messages.append(f"✅ Success: Data appended to '{table_name}'.")

                elif action == 'delete':
                    cursor.execute(f"DELETE FROM dbo.{table_name}")
                    conn.commit()
                    messages.append(f"✅ Success: Data deleted from '{table_name}'.")

            except Exception as e:
                messages.append(f"❌ Error processing table '{table_name}': {str(e)}")

        return messages

    except Exception as e:
        return [f"❌ Error: {str(e)}"]

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

@app.route('/', methods=['GET', 'POST'])
def home():
    global csv_directory, databases
    tables = []
    messages = []
    selected_server = request.form.get('server', '')
    selected_database = request.form.get('database', '')

    if request.method == 'POST':
        if 'fetch_databases' in request.form:
            databases = get_databases(selected_server)

        if 'fetch_tables' in request.form and selected_database:
            csv_directory = request.form.get('csv_directory', '')
            tables = get_matching_files(csv_directory, get_table_names(selected_server, selected_database))

        if 'upload' in request.form and selected_database:
            selected_tables = request.form.getlist('tables')
            action = request.form.get('action')
            if 'select_all' in selected_tables:
                selected_tables = get_table_names(selected_server, selected_database)
            messages = handle_data(selected_server, selected_database, selected_tables, action)

    return render_template('tanda.html', databases=databases, tables=tables, messages=messages,
                           selected_server=selected_server, selected_database=selected_database)

if __name__ == '__main__':
    app.run(debug=True, port=5000)
