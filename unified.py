from flask import Flask, render_template, request
import pandas as pd
import pyodbc
import os
import numpy as np

app = Flask(__name__)

# Database connection details
server = 'HP\SQLEXPRESS'
connection_string = r"DRIVER={ODBC Driver 17 for SQL Server};SERVER=HP\SQLEXPRESS;Trusted_Connection=yes;"

# Available databases
databases = ["RUTH", "SANOFIMCE_2024"]

# Function to get tables from the selected database
def get_tables(database):
    try:
        conn = pyodbc.connect(connection_string + f"DATABASE={database};")
        cursor = conn.cursor()
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
        tables = [row.TABLE_NAME for row in cursor.fetchall()]
        conn.close()
        return tables
    except Exception as e:
        return []

# Function to truncate and insert data
def truncate_and_insert_data(database, selected_tables, csv_directory):
    try:
        if not csv_directory:
            return ["❌ Error: No CSV directory selected."]

        conn = pyodbc.connect(connection_string + f"DATABASE={database};")
        cursor = conn.cursor()
        messages = []

        available_files = {f.split('.')[0]: f for f in os.listdir(csv_directory) if f.endswith(".csv")}

        for table in selected_tables:
            if table not in available_files:
                messages.append(f"❌ Error: File for '{table}' not found in the selected directory.")
                continue

            file_path = os.path.join(csv_directory, available_files[table])
            df = pd.read_csv(file_path, dtype=str)
            df.replace({'NULL': np.nan, 'null': np.nan, '': np.nan}, inplace=True)

            cursor.execute(f"TRUNCATE TABLE dbo.{table}")
            conn.commit()

            # Convert data for SQL insertion
            placeholders = ', '.join(['?'] * len(df.columns))
            insert_query = f"INSERT INTO dbo.{table} ({', '.join(df.columns)}) VALUES ({placeholders})"
            data = [tuple(row) for row in df.values]

            batch_size = 1000
            for i in range(0, len(data), batch_size):
                cursor.executemany(insert_query, data[i:i + batch_size])

            conn.commit()
            messages.append(f"✅ Success: Data uploaded to '{table}'.")

        return messages
    except Exception as e:
        conn.rollback()
        return [f"❌ Error: {str(e)}"]
    finally:
        cursor.close()
        conn.close()

@app.route('/', methods=['GET', 'POST'])
def home():
    selected_tables = []
    messages = []
    csv_directory = request.form.get('csv_directory', '')

    if request.method == 'POST':
        database = request.form.get('database')
        selected_tables = request.form.getlist('tables')

        if database and selected_tables:
            messages = truncate_and_insert_data(database, selected_tables, csv_directory)

    return render_template('unified.html', databases=databases, selected_tables=selected_tables, messages=messages)

if __name__ == '__main__':
    app.run(debug=True, port=5000)
