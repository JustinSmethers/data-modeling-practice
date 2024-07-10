import os
import duckdb
from faker import Faker
from db_manager import DuckDBManager
from schema_generator.schema_definition import load_schema
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta, date
from tqdm import tqdm

fake = Faker()

def insert_data_into_tables(schema: Dict[str, Any], rows: int, db: DuckDBManager) -> None:
    for table_name, table_def in schema.items(): # Iterate over the tables listed in the schema
        print(f'Inserting data into {table_name}')
        
        for iterator in tqdm(range(rows)): # Generate the data row by row
            row = {}
            for col_name, col_def in table_def['columns'].items(): # Iterate over the columns in the row
                col_type, col_constraints = parse_col_definition(col_def)
                if 'PRIMARY KEY' in col_constraints.upper():
                    # Generate the primary key as a numeric iterator equal to the number of rows
                    row[col_name] = iterator
                
                elif 'REFERENCES' not in col_constraints.upper() \
                     and 'UNIQUE' in col_constraints.upper() \
                     and 'PRIMARY KEY' not in col_constraints.upper():    
                    # Generate unique values for non-primary-key, non-foreign-key, unique fields
                    row[col_name] = generate_unique_value(db, table_name, col_name, col_type)

                elif 'REFERENCES' not in col_constraints.upper():
                    # Generate generic fake values for non-id, non-unique fields
                    row[col_name] = generate_fake_value(col_type)  # Extract the base type

                else:
                    # Generate foreign keys as a random int in the range of the primary keys (since PKs are sequential, bounded ints)
                    row[col_name] = fake.pyint(min_value=0, max_value=rows-1)
            
            db.insert_row(table_name, row) # Insert the fully constructed row into the database
        print('\tDone inserting data')

def parse_col_definition(col_def: str) -> list:
    col_def = col_def.split() # Split the col_def into parts
    if len(col_def) == 1: # If there's only one word, return it and an empty string
        return col_def[0], ''
    col_type = col_def[0] # Separate the first word and the rest of the string
    constraints = ' '.join(col_def[1:])

    return col_type, constraints

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

def generate_unique_value(db: DuckDBManager, table_name: str, col_name: str, data_type: str) -> Any:
    value = generate_fake_value(data_type)
    while True:
        if db.is_unique_value(table_name, col_name, value):
            return value
        value += generate_fake_value(data_type)

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

def generate_data(schema: Dict[str, Any], rows: int, db: DuckDBManager) -> None:
    insert_data_into_tables(schema, rows, db)

        # Apply custom constraints
    # for table_name, table_def in schema.items():
    #     for col_name, custom_constraint in table_def.get('custom_constraints', {}).items():
    #         print(f'Applying custom constraint {custom_constraint}')
    #         validate_custom_constraint(custom_constraint)
    #         apply_custom_constraint(conn, table_name, col_name, custom_constraint)