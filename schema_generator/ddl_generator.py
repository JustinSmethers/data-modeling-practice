from schema_generator.schema_definition import load_schema, load_ddl
from typing import Dict, Any

def generate_ddl(file_path: str) -> str:
    schema = load_schema(file_path)
    return load_ddl(schema)

def generate_schema(file_path: str) -> Dict[str, Any]:
    return load_schema(file_path)

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python ddl_generator.py <schema_file_path>")
        sys.exit(1)
    
    schema_file_path = sys.argv[1]
    ddl = generate_schema(schema_file_path)
    print(ddl)
