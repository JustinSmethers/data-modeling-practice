import yaml

def load_schema(file_path: str) -> dict:
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)



def generate_ddl(schema: dict) -> str:
    ddl = ""
    for table_name, table_def in schema.items():
        columns = [f"{col_name} {col_type}" for col_name, col_type in table_def['columns'].items()]
        constraints = table_def.get('constraints', {})
        constraint_statements = [f"{col_name} {constraint}" for col_name, constraint in constraints.items()]
        
        ddl += f"CREATE TABLE {table_name} (\n"
        ddl += ",\n".join(columns + constraint_statements)
        ddl += "\n);\n\n"

        print(f'columns {columns}')
        print(f'constraints {constraints}')
        print(f'constraint_statements {constraint_statements}')
        print(f'ddl {ddl}')
    return ddl