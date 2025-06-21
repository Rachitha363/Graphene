from flask import Flask, render_template
import pandas as pd
import pyodbc
import os
import numpy as np

app = Flask(__name__)

# Database connection details
server = 'HP\SQLEXPRESS'
database = 'SANOFIMCE_2024'
connection_string = r"DRIVER={ODBC Driver 17 for SQL Server};SERVER=HP\SQLEXPRESS;DATABASE=SANOFIMCE_2024;Trusted_Connection=yes;"

# List of table names (only specify tables, columns will be fetched dynamically)
table_names = [
    'RAW_BASEPEOPLEMCE', 'RAW_ENGAGEMENT', 'RAW_NPS_ATTRIBUTES', 'RAW_NPS_DEDICATEDPROMOTERS', 
    'RAW_NPS_MATRIX', 'RAW_PRESCRIPTIONINTENT', 'RAW_PRESCRIPTIONINTENT_ATTRIBUTES', 
    'RAW_PRESCRIPTIONINTENT_MATRIX', 'RAW_PRODUCTSATISFACTION', 'RAW_PRODUCTSATISFACTION_ATTRIBUTES', 
    'RAW_PRODUCTSATISFACTION_MATRIX', 'RAW_TOUCHPOINTSENGAGEMENT', 'RAW_TOUCHPOINTSREACH', 
    'RAW_TPCONTENT_ATTRIBUTES', 'RAW_TPCONTENTEXECUTION', 'RAW_TPCONTENTEXECUTION_MATRIX', 
    'RAW_TPEXECUTION_ATTRIBUTES'
]

csv_directory = r'C:/Users/rachi/OneDrive/Desktop/GRAPHENE/Sanofimce_2024'

def get_db_connection():
    return pyodbc.connect(connection_string)

def get_table_columns(table_name):
    """Fetch column names for a given table from SQL Server."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{table_name}'
    """)
    columns = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return columns

def insert_data():
    """Reads CSV files and inserts data into SQL Server tables after truncation."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        for table_name in table_names:
            print(f"Processing table: {table_name}")
            cursor.execute(f"TRUNCATE TABLE dbo.{table_name}")
            conn.commit()
            print(f"Table '{table_name}' truncated successfully.")

            file_path = os.path.join(csv_directory, f"{table_name}.csv")
            if not os.path.exists(file_path):
                print(f"❌ Error: File '{table_name}.csv' not found.")
                continue

            columns = get_table_columns(table_name)
            df = pd.read_csv(file_path, dtype=str)
            df.replace({'NULL': np.nan, 'null': np.nan, '': np.nan}, inplace=True)

            # Fill missing columns dynamically
            for col in columns:
                if col not in df.columns:
                    df[col] = np.nan  

            # Convert FLOAT and INTEGER columns
            for col in df.columns:
                if any(keyword in col for keyword in ['SCORE', 'NUM']):  # Identify FLOAT columns
                    df[col] = pd.to_numeric(df[col], errors='coerce').round(4).astype(object)
                elif col in ['PERIOD_KEY', 'COMBI_KEY', 'SEGMENT_KEY', 'MATRIX_COMBI_KEY', 'TO_KEY', 'ROWNUMBER', 'APICALLID']:  # Identify INTEGER columns
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int).astype(object)

            # Prepare SQL INSERT query
            placeholders = ', '.join(['?'] * len(columns))
            insert_query = f"INSERT INTO dbo.{table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            data = [tuple(row) for row in df[columns].values]

            # Batch insert data
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

@app.route('/insert-data', methods=['GET'])
def trigger_data_insert():
    success = insert_data()
    message = "Data uploaded successfully after truncation!" if success else "Error uploading data."
    return render_template('succ3.html', message=message)

@app.route('/')
def home():
    return render_template('index3.html')

if __name__ == '__main__':
    app.run(debug=True, port=5000)
