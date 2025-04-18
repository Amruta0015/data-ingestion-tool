from flask import Flask, request, jsonify, send_file, send_from_directory
from flask_cors import CORS
import pandas as pd
from clickhouse_driver import Client
import os
import tempfile

# Initialize Flask app
app = Flask(__name__)
CORS(app)

# Global variable to store ClickHouse connection
ch_client = None

# Serve frontend files
@app.route('/')
def serve_index():
    return send_from_directory(os.path.join(os.path.dirname(__file__), '../frontend'), 'index.html')

@app.route('/frontend/<path:path>')
def serve_frontend(path):
    return send_from_directory(os.path.join(os.path.dirname(__file__), '../frontend'), path)

# ClickHouse API endpoints
@app.route('/connect-clickhouse', methods=['POST'])
def connect_clickhouse():
    global ch_client
    data = request.json
    try:
        ch_client = Client(
            host=data['host'],
            port=data['port'],
            database=data['database'],
            user=data['user'],
            password=data.get('jwt_token', ''),
            secure=True if str(data['port']) in ['9440', '8443'] else False
        )
        return jsonify({'status': 'success', 'message': 'Connected to ClickHouse'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 400

@app.route('/list-tables', methods=['GET'])
def list_tables():
    try:
        tables = ch_client.execute('SHOW TABLES')
        return jsonify({'status': 'success', 'tables': [table[0] for table in tables]})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 400

@app.route('/get-columns/<table>', methods=['GET'])
def get_columns(table):
    try:
        columns = ch_client.execute(f'DESCRIBE TABLE {table}')
        return jsonify({'status': 'success', 'columns': [col[0] for col in columns]})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 400

@app.route('/export-data', methods=['POST'])
def export_data():
    data = request.json
    try:
        columns = ','.join(data['columns']) if data['columns'] else '*'
        query = f'SELECT {columns} FROM {data["table"]}'
        
        result = ch_client.execute(query, with_column_types=True)
        columns = [col[0] for col in result[1]]
        rows = result[0]
        
        fd, path = tempfile.mkstemp(suffix='.csv')
        df = pd.DataFrame(rows, columns=columns)
        df.to_csv(path, index=False)
        
        return send_file(path, as_attachment=True, download_name=f'{data["table"]}_export.csv')
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 400

@app.route('/import-data', methods=['POST'])
def import_data():
    try:
        if 'file' not in request.files:
            return jsonify({'status': 'error', 'message': 'No file uploaded'}), 400
        
        file = request.files['file']
        table_name = request.form.get('table_name', 'imported_data')
        
        df = pd.read_csv(file)
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join([f'{col} String' for col in df.columns])}
        ) ENGINE = MergeTree()
        ORDER BY tuple()
        """
        ch_client.execute(create_table_sql)
        
        ch_client.insert_dataframe(f'INSERT INTO {table_name} VALUES', df)
        
        return jsonify({
            'status': 'success',
            'message': f'Imported {len(df)} records to table {table_name}'
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 400

if __name__ == '__main__':
    app.run(debug=True)