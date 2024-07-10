import os
import duckdb
from faker import Faker
from schema_generator.schema_definition import load_schema
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta, date
from tqdm import tqdm

fake = Faker()

ALLOWED_CUSTOM_CONSTRAINTS = {'BETWEEN', 'AFTER', 'BEFORE'}
ALLOWED_DATA_TYPES = {'INTEGER', 'FLOAT', 'VARCHAR', 'DATE'}

def generate_fake_data(schema: Dict[str, Any], rows: int, db_path: str) -> duckdb.DuckDBPyConnection:
    conn = create_db_connection(db_path)
    validate_schema_data_types(schema) # Validate schema data types
    create_tables_in_duckdb(schema, conn) # Create tables in DuckDB
    
    # Insert data into tables
    insert_data_into_tables(schema, rows, conn)

    # Apply custom constraints
    # for table_name, table_def in schema.items():
    #     for col_name, custom_constraint in table_def.get('custom_constraints', {}).items():
    #         print(f'Applying custom constraint {custom_constraint}')
    #         validate_custom_constraint(custom_constraint)
    #         apply_custom_constraint(conn, table_name, col_name, custom_constraint)
    
    return conn

def create_db_connection(db_path: str) -> duckdb.DuckDBPyConnection:
    if os.path.exists(db_path): # Delete the existing database file
        os.remove(db_path)
        print(f"Existing database file '{db_path}' has been deleted.")
    
    conn = duckdb.connect(db_path) # Create a new database file by connecting to it
    print(f"New database file '{db_path}' has been created.")

    return conn

def create_tables_in_duckdb(schema: Dict[str, Any], conn: duckdb.DuckDBPyConnection) -> None:
    for table_name, table_def in schema.items():
        columns = [f"{col_name} {col_type}" for col_name, col_type in table_def['columns'].items()]
        ddl = f"CREATE OR REPLACE TABLE {table_name} (\n" + ",\n".join(columns) + "\n);"
        conn.sql(ddl)    

def insert_data_into_tables(schema: Dict[str, Any], rows: int, conn: duckdb.DuckDBPyConnection) -> None:
    for table_name, table_def in schema.items():
        print(f'Inserting data into {table_name}')
        for iterator in tqdm(range(rows)):
            # if iterator % 100 == 0:
                # print(f'\tOn row {iterator} of {rows}')
            row = {}
            for col_name, col_def in table_def['columns'].items():
                col_type, col_constraints = parse_col_definition(col_def)
                if 'PRIMARY KEY' in col_constraints.upper():
                    row[col_name] = iterator
                elif 'REFERENCES' not in col_constraints.upper() and ('UNIQUE' in col_constraints.upper()) and 'PRIMARY KEY' not in col_constraints.upper():
                    row[col_name] = generate_unique_value(conn, table_name, col_name, col_type)
                elif 'REFERENCES' not in col_constraints.upper():
                    row[col_name] = generate_fake_value(col_type)  # Extract the base type
                else:
                    row[col_name] = fake.pyint(min_value=0, max_value=rows-1) # Set the foreign key to a random int in the range of the primary keys
            insert_row(conn, table_name, row) 
        print('\tDone inserting data')

def parse_col_definition(col_def: str) -> list:
    col_def = col_def.split() # Split the col_def into parts
    if len(col_def) == 1: # If there's only one word, return it and an empty string
        return col_def[0], ''
    col_type = col_def[0] # Separate the first word and the rest of the string
    constraints = ' '.join(col_def[1:])

    return col_type, constraints

def parse_foreign_key(col_def: str) -> (str, str):
    reference_part = col_def.split(' ')[2]
    references_list = reference_part.replace('(', ' ').replace(')', '').split(' ')
    referenced_table = references_list[0]
    referenced_column = references_list[1]
    return referenced_table, referenced_column

def generate_fake_value(data_type: str, date_between_start: Optional[datetime.date]=None, date_between_end: Optional[datetime.date]=None) -> Any:
    if data_type == 'INTEGER':
        return fake.random_int(min=1, max=100000)
    elif data_type == 'FLOAT':
        return fake.pyfloat(left_digits=3, right_digits=2)
    elif data_type == 'VARCHAR':
        return fake.word()
    elif data_type == 'DATE':
        if date_between_end is None and date_between_start is None:
            return fake.date()
        elif date_between_start is None:
            return fake.date_between(end_date=date_between_end)
        elif date_between_end is None:
            return fake.date_between(start_date=date_between_start)
        else:
            return fake.date_between(start_date=date_between_start, end_date=date_between_end)
    else:
        return ValueError(f"Unsupported data type '{data_type}'")

def generate_unique_value(conn: duckdb.DuckDBPyConnection, table_name: str, col_name: str, data_type: str) -> Any:
    while True:
        value = generate_fake_value(data_type) + generate_fake_value(data_type)
        conn.execute('CREATE TABLE IF NOT EXISTS unique_values (table_name VARCHAR, column_name VARCHAR, value VARCHAR)')
        result = conn.execute("SELECT 1 FROM unique_values WHERE table_name = ? AND column_name = ? AND value = ?", (table_name, col_name, str(value))).fetchone()
        if result is None:
            conn.execute("INSERT INTO unique_values (table_name, column_name, value) VALUES (?, ?, ?)", (table_name, col_name, str(value)))
            return value

def validate_custom_constraint(custom_constraint: str) -> None:
    for keyword in ALLOWED_CUSTOM_CONSTRAINTS:
        if keyword in custom_constraint:
            return
    raise ValueError(f"Unsupported custom constraint: {custom_constraint}")

def validate_schema_data_types(schema: Dict[str, Any]) -> None:
    for table_name, table_def in schema.items():
        for col_name, col_type in table_def['columns'].items():
            base_type = col_type.split()[0]
            if base_type not in ALLOWED_DATA_TYPES:
                raise ValueError(f"Unsupported data type '{base_type}' for column '{col_name}' in table '{table_name}'")

def insert_row(conn: duckdb.DuckDBPyConnection, table_name: str, row: Dict[str, Any]) -> None:
    columns = ', '.join(row.keys())
    placeholders = ', '.join(['?'] * len(row))
    values = tuple(row.values())
    conn.execute(f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})", values)

def apply_custom_constraint(conn: duckdb.DuckDBPyConnection, table_name: str, col_name: str, custom_constraint: str) -> None:
    if 'BETWEEN' in custom_constraint:
        min_val, max_val = map(int, custom_constraint.split('BETWEEN')[1].split('AND'))
        query = f"UPDATE {table_name} SET {col_name} = RANDOM() * ({max_val} - {min_val}) + {min_val} WHERE {col_name} IS NULL;"
        conn.execute(query)
    elif 'AFTER' in custom_constraint:
        referenced_table, referenced_column = custom_constraint.split('AFTER')[1].strip().split('.')

        query = f"""
        UPDATE {table_name}
        SET {col_name} = (
            SELECT {referenced_column} + INTERVAL (RANDOM() * 1000) DAY
            FROM {referenced_table}
            ORDER BY RANDOM()
            LIMIT 1
        )
        WHERE {col_name} IS NULL;
        """
        conn.execute(query)
    elif 'BEFORE' in custom_constraint:
        referenced_table, referenced_column = custom_constraint.split('BEFORE')[1].strip().split('.')
        query = f"""
        UPDATE {table_name}
        SET {col_name} = (
            SELECT {referenced_column} - INTERVAL '1 day' * RANDOM() * 1000
            FROM {referenced_table}
            ORDER BY RANDOM()
            LIMIT 1
        )
        WHERE {col_name} IS NULL;
        """
        conn.execute(query)

def generate_data(schema_file: str, rows: int, db_path: str) -> duckdb.DuckDBPyConnection:
    schema = load_schema(schema_file)
    return generate_fake_data(schema, rows, db_path)