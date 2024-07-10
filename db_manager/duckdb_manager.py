import duckdb
from typing import Dict, List, Any

class DuckDBManager:
    def __init__(self, db_path: str):
        self.conn = duckdb.connect(db_path)

    def execute_ddl(self, ddl: str):
        self.conn.execute(ddl)

    def insert_data(self, data: Dict[str, List[Dict[str, Any]]]):
        for table_name, table_data in data.items():
            if table_data:
                columns = ', '.join(table_data[0].keys())
                placeholders = ', '.join(['?' for _ in table_data[0]])
                query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                self.conn.executemany(query, [tuple(row.values()) for row in table_data])

    def close(self):
        self.conn.close()