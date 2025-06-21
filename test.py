from flask import Flask, render_template, request
import pandas as pd
import pyodbc
import os
import numpy as np

app = Flask(__name__)

# Server and database details
server = 'HP\SQLEXPRESS'
databases = ['RUTH', 'SANOFIMCE_2024']

connection_string_template = r"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER=HP\SQLEXPRESS;DATABASE={};Trusted_Connection=yes;"

csv_directory = ''

# Connect to database
def get_db_connection(database):
    connection_string = connection_string_template.format(database)
    return pyodbc.connect(connection_string)

# Fetch tables from selected database
def get_table_names(database):
    try:
        conn = get_db_connection(database)
        cursor = conn.cursor()
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
        tables = [row.TABLE_NAME for row in cursor.fetchall()]
        conn.close()
        return tables
    except Exception as e:
        return []

# Match table names with files in the directory (without .csv extension)
def get_matching_files(directory, tables):
    try:
        files = {os.path.splitext(file)[0]: file for file in os.listdir(directory) if os.path.isfile(os.path.join(directory, file))}
        return [table for table in tables if table in files]
    except Exception as e:
        return []

# Truncate and insert data
def truncate_and_insert_data(database, selected_tables):
    try:
        conn = get_db_connection(database)
        cursor = conn.cursor()
        messages = []

        for table_name in selected_tables:
            print(f"Processing table: {table_name}")
            cursor.execute(f"TRUNCATE TABLE dbo.{table_name}")
            conn.commit()
            print(f"Table '{table_name}' truncated successfully.")

            file_path = os.path.join(csv_directory, f"{table_name}.csv")
            if not os.path.exists(file_path):
                messages.append(f"❌ Error: File '{table_name}' not found in directory.")
                continue

            df = pd.read_csv(file_path, dtype=str)
            df.replace({'NULL': None, 'null': None, '': None}, inplace=True)

            # Replace NULLs for non-nullable columns with default values if needed
            for col in df.columns:
                if df[col].isnull().all():  # If entire column is null, fill with some default
                    df[col].fillna('N/A', inplace=True)  # Or any sensible default for your use case

                # Check if specific non-nullable columns need default values
                if col.upper() == 'PERIOD':  # Assuming PERIOD column must not be null
                    df[col].fillna('DEFAULT_PERIOD', inplace=True)

            placeholders = ', '.join(['?'] * len(df.columns))
            insert_query = f"INSERT INTO dbo.{table_name} ({', '.join(df.columns)}) VALUES ({placeholders})"

            # Convert data types to native Python types to avoid ODBC driver issues
            data = [tuple(map(lambda x: x.item() if isinstance(x, (np.int64, np.float64)) else x, row)) for row in df.replace({np.nan: None}).values]

            batch_size = 1000
            for i in range(0, len(data), batch_size):
                cursor.executemany(insert_query, data[i:i + batch_size])

            conn.commit()
            messages.append(f"✅ Success: Data uploaded to '{table_name}'.")

        return messages

    except Exception as e:
        return [f"❌ Error: {str(e)}"]

    finally:
        cursor.close()
        conn.close()


@app.route('/', methods=['GET', 'POST'])
def home():
    global csv_directory
    tables = []
    messages = []

    if request.method == 'POST':
        if 'database' in request.form:
            database = request.form['database']
            csv_directory = request.form.get('csv_directory', '')
            tables = get_matching_files(csv_directory, get_table_names(database))

        if 'upload' in request.form:
            database = request.form['database']
            selected_tables = request.form.getlist('tables')
            if 'select_all' in selected_tables:
                selected_tables = get_table_names(database)
            messages = truncate_and_insert_data(database, selected_tables)

    return render_template('test.html', databases=databases, tables=tables, messages=messages)

if __name__ == '__main__':
    app.run(debug=True, port=5000)
