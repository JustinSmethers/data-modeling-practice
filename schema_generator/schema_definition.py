import yaml
from typing import Dict, Any

ALLOWED_DATA_TYPES = {'INTEGER', 'FLOAT', 'VARCHAR', 'DATE'}
ALLOWED_CUSTOM_CONSTRAINTS = {'BETWEEN', 'AFTER', 'BEFORE'}


def load_schema(file_path):
    with open(file_path, 'r') as file:
        schema = yaml.safe_load(file)
        validate_schema_data_types(schema)
        validate_custom_constraints(schema)
        return schema
    
def validate_schema_data_types(schema: Dict[str, Any]) -> None:
    for table_name, table_def in schema.items():
        for col_name, col_type in table_def['columns'].items():
            base_type = col_type.split()[0]
            if base_type not in ALLOWED_DATA_TYPES:
                raise ValueError(f"Unsupported data type '{base_type}' for column '{col_name}' in table '{table_name}'")
            
def validate_custom_constraints(schema: Dict[str, Any]) -> None:
    for table_name, table_def in schema.items():
        for col_name, custom_constraint in table_def.get('custom_constraints', {}).items():
            custom_constraint_type = custom_constraint.split(' ')[0]
            if custom_constraint_type not in ALLOWED_CUSTOM_CONSTRAINTS:
                raise ValueError(f"Unsupported custom constraint '{custom_constraint}' in schema for column '{col_name}' in table '{table_name}'")


def load_ddl(schema: dict) -> str:
    ddl = ""
    for table_name, table_def in schema.items():
        columns = [f"{col_name} {col_type}" for col_name, col_type in table_def['columns'].items()]
        ddl += f"CREATE OR REPLACE TABLE {table_name} (\n" + ",\n".join(columns) + "\n);"
    return ddl