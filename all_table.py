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

# Define tables with exact columns from the database schema
tables = {
    'RAW_BASEPEOPLEMCE': ['PERIOD_KEY', 'COMBI_KEY', 'SEGMENT_KEY', 'NUMBER_OF_PPL_3M_6M', 'NUM_REACH_PPL', 'NUM_NOT_REACH_PPL', 'ROWNUMBER', 'APICALLID'],
    'RAW_ENGAGEMENT': ['PERIOD_KEY', 'COMBI_KEY', 'SEGMENT_KEY', 'NUM_POS_PPL', 'NUM_NEG_NEUT_PPL', 'NUM_POS_PPL_ALL_TP', 'NUM_NEG_NEUT_PPL_ALL_TP', 'ROWNUMBER', 'APICALLID'],
    'RAW_NPS_ATTRIBUTES': ['PERIOD_KEY', 'COMBI_KEY', 'SEGMENT_KEY', 'TONE_KEY', 'THEME', 'NUM_PPL_ATTR', 'ROWNUMBER', 'APICALLID'],
    'RAW_NPS_DEDICATEDPROMOTERS': ['PERIOD_KEY', 'COMBI_KEY', 'SEGMENT_KEY', 'NUM_ACTIVE_PROMOTER_PPL', 'NUM_PASSIVE_PROMOTER_PPL', 'NUM_DETRACTOR_PPL', 'ROWNUMBER', 'APICALLID'],
    'RAW_NPS_MATRIX': ['PERIOD_KEY', 'MATRIX_COMBI_KEY', 'SEGMENT_KEY', 'NUM_ACTIVE_PROMOTER_SANOFIENGAGED_PPL', 'NUM_PASSIVE_PROMOTER_SANOFIENGAGED_PPL','NUM_PASSIVE_NEUTRAL_SANOFIENGAGED_PPL','NUM_DETRACTOR_SANOFIENGAGED_PPL', 'ROWNUMBER', 'APICALLID'],
    'RAW_PRESCRIPTIONINTENT': ['PERIOD_KEY', 'COMBI_KEY', 'SEGMENT_KEY', 'TO_KEY', 'NUM_PRESCRIPTION_PPL', 'ROWNUMBER', 'APICALLID'],
    'RAW_PRESCRIPTIONINTENT_ATTRIBUTES': ['PERIOD_KEY', 'COMBI_KEY', 'SEGMENT_KEY', 'TONE_KEY', 'THEME', 'NUM_PRESCRIPTION_PPL_ATTR', 'ROWNUMBER', 'APICALLID'],
    'RAW_PRESCRIPTIONINTENT_MATRIX': ['PERIOD_KEY', 'MATRIX_COMBI_KEY', 'SEGMENT_KEY', 'TO_KEY', 'NUM_PRESCRIPTION_SANOFIENGAGED_PPL', 'ROWNUMBER', 'APICALLID'],
    'RAW_PRODUCTSATISFACTION': ['PERIOD_KEY', 'COMBI_KEY', 'SEGMENT_KEY', 'NUM_POS_PRODUCTS_PPL', 'ROWNUMBER', 'APICALLID'],
    'RAW_PRODUCTSATISFACTION_ATTRIBUTES': ['PERIOD_KEY', 'COMBI_KEY', 'SEGMENT_KEY', 'TONE_KEY', 'THEME', 'NUM_POS_PRODUCTS_PPL_ATTR', 'ROWNUMBER', 'APICALLID'],
    'RAW_PRODUCTSATISFACTION_MATRIX': ['PERIOD_KEY', 'MATRIX_COMBI_KEY', 'SEGMENT_KEY', 'NUM_POS_PRODUCTS_SANOFIENGAGED_PPL', 'ROWNUMBER', 'APICALLID'],
    'RAW_TOUCHPOINTSENGAGEMENT': ['PERIOD_KEY', 'COMBI_KEY', 'SEGMENT_KEY', 'TOUCHPOINT_GROUP_KEY', 'TOUCHPOINT_KEY', 'NUM_POS_ENGAGED_PPL_TP', 'ROWNUMBER', 'APICALLID'],
    'RAW_TOUCHPOINTSREACH': ['PERIOD_KEY', 'COMBI_KEY', 'SEGMENT_KEY', 'TOUCHPOINT_GROUP_KEY', 'TOUCHPOINT_KEY', 'NUM_REACH_PPL_TP', 'ROWNUMBER', 'APICALLID'],
    'RAW_TPCONTENT_ATTRIBUTES': ['PERIOD_KEY', 'COMBI_KEY', 'SEGMENT_KEY', 'TONE_KEY', 'THEME', 'NUM_POS_CONTENT_OVERALL_PPL', 'NUM_POS_CONTENT_F2F_PPL', 'NUM_POS_CONTENT_DIGITAL_PPL', 'NUM_POS_CONTENT_REMOTE_PPL', 'NUM_POS_CONTENT_OTHERS_PPL', 'ROWNUMBER', 'APICALLID'],
    'RAW_TPCONTENTEXECUTION': ['PERIOD_KEY', 'COMBI_KEY', 'SEGMENT_KEY', 'NUM_POS_CONTENT_EXECUTION_PPL_TP', 'NUM_POS_CONTENT_PPL_TP', 'NUM_POS_EXECUTION_PPL_TP', 'ROWNUMBER', 'APICALLID'],
    'RAW_TPCONTENTEXECUTION_MATRIX': ['PERIOD_KEY', 'MATRIX_COMBI_KEY', 'SEGMENT_KEY','NUM_POS_ENGAGED_PPL','NUM_POS_CONTENT_PPL', 'NUM_POS_CONTENT_SANOFIENGAGED_PPL','NUM_POS_CONTENT_COMPETITORSENGAGED_PPL','NUM_POS_EXECUTION_BOTHENGAGED_PPL','ROWNUMBER', 'APICALLID'],
    'RAW_TPEXECUTION_ATTRIBUTES': ['PERIOD_KEY', 'COMBI_KEY', 'SEGMENT_KEY', 'TONE_KEY', 'THEME', 'NUM_POS_EXECUTION_OVERALL_PPL', 'NUM_POS_EXECUTION_F2F_PPL', 'NUM_POS_EXECUTION_DIGITAL_PPL', 'NUM_POS_EXECUTION_REMOTE_PPL', 'NUM_POS_EXECUTION_OTHERS_PPL', 'ROWNUMBER', 'APICALLID']
}

csv_directory = r'C:/Users/rachi/OneDrive/Desktop/GRAPHENE/Sanofimce_2024'

def get_db_connection():
    return pyodbc.connect(connection_string)
def insert_data():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        for table_name, columns in tables.items():
            print(f"Processing table: {table_name}")
            cursor.execute(f"TRUNCATE TABLE dbo.{table_name}")
            conn.commit()
            print(f"Table '{table_name}' truncated successfully.")

            file_path = os.path.join(csv_directory, f"{table_name}.csv")
            if not os.path.exists(file_path):
                print(f"❌ Error: File '{table_name}.csv' not found.")
                continue

            df = pd.read_csv(file_path, dtype=str)  # Read as string to avoid conversion errors
            df.replace({'NULL': np.nan, 'null': np.nan, '': np.nan}, inplace=True)

            # ✅ Fill missing columns with NULL values
            missing_columns = [col for col in columns if col not in df.columns]
            if missing_columns:
                print(f"⚠️ Warning: Missing columns in '{table_name}': {missing_columns}")
                for col in missing_columns:
                    df[col] = np.nan  # Fill missing columns with NULL

            # ✅ Convert FLOAT columns safely
            for col in columns:
                if 'SCORE' in col or 'NUM' in col:  # Identifying FLOAT columns
                    df[col] = pd.to_numeric(df[col], errors='coerce')  # Convert to float, invalid values to NaN
                    df[col] = df[col].round(4)  # ✅ Limit precision to 4 decimal places
                    df[col] = df[col].replace({np.nan: None})  # ✅ Convert NaN to None (NULL in SQL)
                    df[col] = df[col].astype(object)  # ✅ Ensure compatibility with pyodbc

            # ✅ Convert INTEGER columns safely
            for col in ['PERIOD_KEY', 'COMBI_KEY', 'SEGMENT_KEY', 'MATRIX_COMBI_KEY', 'TO_KEY', 'ROWNUMBER', 'APICALLID']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                    df[col] = df[col].astype(object)

            # ✅ Prepare SQL INSERT statement
            placeholders = ', '.join(['?'] * len(columns))
            insert_query = f"INSERT INTO dbo.{table_name} ({', '.join(columns)}) VALUES ({placeholders})"

            # ✅ Convert NumPy data types to standard Python types before inserting
            data = [tuple(map(lambda x: float(x) if isinstance(x, np.float64) else x, row)) for row in df[columns].values]

            # ✅ Insert data in batches
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
    return render_template('succ2.html', message=message)

@app.route('/')
def home():
    return render_template('index2.html')

if __name__ == '__main__':
    app.run(debug=True, port=5000)
