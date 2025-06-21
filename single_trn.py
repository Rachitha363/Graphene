from flask import Flask, jsonify, render_template, request
import pandas as pd
import pyodbc
import os

app = Flask(__name__)

# Database connection details (Windows Authentication)
server = 'HP\SQLEXPRESS'
database = 'SANOFIMCE_2024'

connection_string = r"DRIVER={ODBC Driver 17 for SQL Server};SERVER=HP\SQLEXPRESS;DATABASE=SANOFIMCE_2024;Trusted_Connection=yes;"

# Initialize the database connection
def get_db_connection():
    conn = pyodbc.connect(connection_string)
    return conn

# Specify the table for upload
table_name = 'RAW_BASEPEOPLEMCE'  # Table name
csv_directory = r'C:/Users/rachi/OneDrive/Desktop/GRAPHENE/Sanofimce_2024'  # CSV directory

def insert_data():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Truncate the table before inserting new data
        cursor.execute(f"TRUNCATE TABLE dbo.{table_name}")
        conn.commit()
        print(f"Table '{table_name}' truncated successfully.")

        # Define the file path
        csv_file = f"{table_name}.csv"
        file_path = os.path.join(csv_directory, csv_file)

        if not os.path.exists(file_path):
            print(f"Error: File '{csv_file}' not found in the specified directory.")
            return False

        print(f"Processing file: {csv_file}")

        # Read CSV into a DataFrame
        df = pd.read_csv(file_path, dtype=str)
        df.replace('NULL', None, inplace=True)  # Replace 'NULL' string with None

        expected_columns = [
            'PERIOD_KEY', 'COMBI_KEY', 'SEGMENT_KEY', 
            'NUMBER_OF_PPL_3M_6M', 'NUM_REACH_PPL', 'NUM_NOT_REACH_PPL', 
            'ROWNUMBER', 'APICALLID'
        ]

        if not all(col in df.columns for col in expected_columns):
            print("Error: CSV file is missing required columns.")
            return False

        # Convert columns to their expected data types
        type_conversions = {
            'PERIOD_KEY': int, 'COMBI_KEY': int, 'SEGMENT_KEY': int,
            'NUMBER_OF_PPL_3M_6M': float, 'NUM_REACH_PPL': float, 'NUM_NOT_REACH_PPL': float,
            'ROWNUMBER': int, 'APICALLID': int
        }

        for col, dtype in type_conversions.items():
            try:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(dtype)
            except ValueError:
                print(f"Warning: Could not convert column '{col}' to {dtype}. Setting invalid values to None.")
                df[col] = None

        # SQL INSERT statement
        placeholders = ', '.join(['?'] * len(expected_columns))
        insert_query = f"INSERT INTO dbo.{table_name} ({', '.join(expected_columns)}) VALUES ({placeholders})"
        
        # Convert DataFrame rows to a list of tuples
        data = [tuple(row) for row in df[expected_columns].values]
        
        # Insert data in batches
        batch_size = 1000
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            cursor.executemany(insert_query, batch)

        # Commit transaction
        conn.commit()
        print(f"Success: Data inserted into '{table_name}' successfully.")
        return True

    except Exception as e:
        conn.rollback()  # Rollback if error occurs
        print(f"Error: {str(e)}")
        return False

    finally:
        cursor.close()  # Close cursor
        conn.close()  # Close connection

@app.route('/insert-data', methods=['GET'])
def trigger_data_insert():
    success = insert_data()
    if success:
        return render_template('succ1.html', message="Data uploaded successfully after truncation!")
    else:
        return render_template('succ1.html', message="Error uploading data.")

@app.route('/')
def home():
    return render_template('index1.html')

if __name__ == '__main__':
    app.run(debug=True, port=5000)
