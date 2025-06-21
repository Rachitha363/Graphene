from flask import Flask, render_template, request
import pandas as pd
import pyodbc
import os
import numpy as np

app = Flask(__name__)

# Database connection details
server = 'HP\\SQLEXPRESS'
database = 'SANOFIMCE_2024'
connection_string = r"DRIVER={ODBC Driver 17 for SQL Server};SERVER=HP\SQLEXPRESS;DATABASE=SANOFIMCE_2024;Trusted_Connection=yes;"

# List of tables
table_names = [
    'RAW_BASEPEOPLEMCE', 'RAW_ENGAGEMENT', 'RAW_NPS_ATTRIBUTES', 'RAW_NPS_DEDICATEDPROMOTERS',
    'RAW_NPS_MATRIX', 'RAW_PRESCRIPTIONINTENT', 'RAW_PRESCRIPTIONINTENT_ATTRIBUTES',
    'RAW_PRESCRIPTIONINTENT_MATRIX', 'RAW_PRODUCTSATISFACTION', 'RAW_PRODUCTSATISFACTION_ATTRIBUTES',
    'RAW_PRODUCTSATISFACTION_MATRIX', 'RAW_TOUCHPOINTSENGAGEMENT', 'RAW_TOUCHPOINTSREACH',
    'RAW_TPCONTENT_ATTRIBUTES', 'RAW_TPCONTENTEXECUTION', 'RAW_TPCONTENTEXECUTION_MATRIX',
    'RAW_TPEXECUTION_ATTRIBUTES'
]

csv_directory = r'C:/Users/rachi/OneDrive/Desktop/GRAPHENE/Sanofimce_2024'

# Function to establish database connection
def get_db_connection():
    return pyodbc.connect(connection_string)

# Function to truncate selected tables
def truncate_tables(selected_tables):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        for table in selected_tables:
            print(f"Truncating table: {table}")
            cursor.execute(f"TRUNCATE TABLE dbo.{table}")
            conn.commit()
            print(f"Table '{table}' truncated successfully.")

        return True

    except Exception as e:
        conn.rollback()
        print(f"❌ Error: {str(e)}")
        return False

    finally:
        cursor.close()
        conn.close()

# Function to insert data into selected tables
def insert_data(selected_tables):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        for table_name in selected_tables:
            print(f"Processing table: {table_name}")
            cursor.execute(f"TRUNCATE TABLE dbo.{table_name}")
            conn.commit()
            print(f"Table '{table_name}' truncated successfully.")

            file_path = os.path.join(csv_directory, f"{table_name}.csv")
            if not os.path.exists(file_path):
                print(f"❌ Error: File '{table_name}.csv' not found.")
                continue

            df = pd.read_csv(file_path, dtype=str)
            df.replace({'NULL': np.nan, 'null': np.nan, '': np.nan}, inplace=True)

            placeholders = ', '.join(['?'] * len(df.columns))
            insert_query = f"INSERT INTO dbo.{table_name} ({', '.join(df.columns)}) VALUES ({placeholders})"

            data = [tuple(map(lambda x: float(x) if isinstance(x, np.float64) else x, row)) for row in df.values]

            batch_size = 1000
            for i in range(0, len(data), batch_size):
                cursor.executemany(insert_query, data[i:i + batch_size])

            conn.commit()
            print(f"✅ Success: Data inserted into '{table_name}'.")

        return True

    except Exception as e:
        conn.rollback()
        print(f"❌ Error: {str(e)}")
        return False

    finally:
        cursor.close()
        conn.close()

@app.route('/', methods=['GET', 'POST'])
def home():
    if request.method == 'POST':
        selected_tables = request.form.getlist('tables')

        if 'truncate' in request.form:
            success = truncate_tables(selected_tables)
            message = "Tables truncated successfully!" if success else "Error truncating tables."
        elif 'insert' in request.form:
            success = insert_data(selected_tables)
            message = "Data uploaded successfully!" if success else "Error uploading data."

        return render_template('index_c1.html', tables=table_names, message=message)

    return render_template('index_c1.html', tables=table_names)

if __name__ == '__main__':
    app.run(debug=True, port=5000)
