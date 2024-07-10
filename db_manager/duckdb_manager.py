import os
import duckdb
from typing import Dict, List, Any

class DuckDBManager:
    def __init__(self, db_path: str = 'data-modeling-practice.db', replace_existing: bool = False):
        if os.path.exists(db_path) and replace_existing:
            print(f"Existing file {db_path} has been removed. Recreating...")
            os.remove(db_path)
        
        self._conn = duckdb.connect(db_path)

    def execute_ddl(self, ddl: str) -> None:
            self._conn.sql(ddl)

    def show_all_tables(self) -> str:
        return self._conn.sql('SHOW ALL TABLES;')

    def insert_row(self, table_name: str, row: Dict[str, Any]):
        columns = ', '.join(row.keys())
        placeholders = ', '.join(['?'] * len(row))
        values = tuple(row.values())

        self._conn.execute(f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})", values)

    def is_unique_value(self, table_name: str, col_name: str, value: str):
        self._conn.sql('CREATE TABLE IF NOT EXISTS unique_values (table_name VARCHAR, column_name VARCHAR, value VARCHAR)')
        result = self._conn.execute(
            "SELECT 1 FROM unique_values WHERE table_name = ? AND column_name = ? AND value = ?", 
            (table_name, col_name, str(value))
            ).fetchone()
        
        if result is None:
            self._conn.execute(f"INSERT INTO unique_values (table_name, column_name, value) VALUES (?, ?, ?)", (table_name, col_name, str(value)))
        
        return result is None
        
    def get_conn(self):
        return self._conn

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None
    
    def __del__(self):
        self.close()