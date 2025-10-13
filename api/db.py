from pyhive import hive
from typing import List, Dict

def get_hive_connection():
    conn = hive.Connection(host='localhost', port=10000, username='ctnorhafizah')
    return conn

def fetch_query(sql: str) -> List[Dict]:
    conn = get_hive_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    
    # Strip table prefix if present
    columns = [col[0].split('.')[-1] for col in cursor.description]
    
    rows = cursor.fetchall()
    result = [dict(zip(columns, row)) for row in rows]
    
    cursor.close()
    conn.close()
    return result

