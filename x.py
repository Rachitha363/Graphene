import pyodbc

server = r'HP\SQLEXPRESS'  # Updated with your actual server name
database = 'SANOFIMCE_2024'  # Ensure this database exists in SQL Server

try:
    conn = pyodbc.connect(f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;")
    print("✅ Connected to SQL Server")
    conn.close()
except Exception as e:
    print(f"❌ Connection Failed: {str(e)}")
