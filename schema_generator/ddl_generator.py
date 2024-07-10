from schema_generator.schema_definition import load_schema, generate_ddl

def generate_schema(file_path: str) -> str:
    schema = load_schema(file_path)
    ddl = generate_ddl(schema)
    return ddl

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python ddl_generator.py <schema_file_path>")
        sys.exit(1)
    
    schema_file_path = sys.argv[1]
    ddl = generate_schema(schema_file_path)
    print(ddl)
