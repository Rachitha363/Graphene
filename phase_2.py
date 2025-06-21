import pyodbc
from flask import Flask, request, jsonify, render_template

app = Flask(__name__)

def fetch_tables(server, database, username, password):
    try:
        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'RAW_%'")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        return tables
    except Exception as e:
        return str(e)

def run_tests(server, database, username, password, tables):
    try:
        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        )
        cursor = conn.cursor()
        results = []
        
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            results.append(f"✅ {table} has data" if count > 0 else f"❌ {table} is empty")
            
            cursor.execute(f"SELECT 1 FROM {table} WHERE PERIOD_KEY <> 3")
            if cursor.fetchone():
                results.append(f"❌ {table} has incorrect PERIOD_KEY")
            else:
                results.append(f"✅ PERIOD_KEY is always 3 in {table}")
            
            # Detect correct key column
            cursor.execute("""
                SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = ? AND COLUMN_NAME IN ('COMBI_KEY', 'MATRIX_COMBI_KEY')
            """, (table,))
            key_column_row = cursor.fetchone()
            key_column = key_column_row[0] if key_column_row else None  # Assign column dynamically

            # If key column exists, check for NULL values
            if key_column:
                cursor.execute(f"SELECT 1 FROM {table} WHERE PERIOD_KEY IS NULL OR SEGMENT_KEY IS NULL OR {key_column} IS NULL")
                if cursor.fetchone():
                    results.append(f"❌ {table} has NULL values in key columns ({key_column})")
                else:
                    results.append(f"✅ No NULL values in key columns of {table} ({key_column})")
            else:
                results.append(f"⚠ Skipping COMBI_KEY check for {table} (Neither COMBI_KEY nor MATRIX_COMBI_KEY found)")
        
        conn.close()
        return results
    except Exception as e:
        return [str(e)]

@app.route('/fetch_tables', methods=['POST'])
def get_tables():
    data = request.json
    tables = fetch_tables(data['server'], data['database'], data['username'], data['password'])
    return jsonify(tables)

@app.route('/run_tests', methods=['POST'])
def execute_tests():
    data = request.json
    results = run_tests(data['server'], data['database'], data['username'], data['password'], data['tables'])
    return jsonify(results)

@app.route('/')
def home():
    return render_template('phase_2.html')

if __name__ == '__main__':
    app.run(debug=True)
