import rich_click as click
from schema_generator import generate_schema, generate_ddl
from data_generator import generate_data
from db_manager import DuckDBManager
import subprocess
import os

@click.group()
def cli():
    """Data Modeling Practice CLI"""
    pass

@cli.command()
@click.argument('schema_file')
def test_schema_function(schema_file):
    generate_ddl(schema_file)

@cli.command()
@click.argument('schema_file')
def preview_ddl_command(schema_file):
    click.echo(f"DDL command:\n {generate_ddl(schema_file)}")

@cli.command()
@click.argument('schema_file')
@click.option('--db_path', default='data-modeling-practice.db', help='Path where to create the database file')
def create_db_with_schema(schema_file, db_path):
    """Generate schema from YAML file"""
    if os.path.exists(db_path):
        confirm = click.confirm(f'db file already exists at {db_path}. Do you want to remove it and re-create the db?', default=True)
        if confirm:
            os.remove(db_path)
            ddl = generate_ddl(schema_file)
            db = DuckDBManager()
            db.execute_ddl(ddl)
            click.echo(f"Schema created from {schema_file}")
    else:
        ddl = generate_ddl(schema_file)
        db = DuckDBManager()
        db.execute_ddl(ddl)
        click.echo(f"Schema created from {schema_file}")

@cli.command()
def show_all_tables():
    db = DuckDBManager()
    click.echo(db.show_all_tables())

@cli.command()
@click.argument('schema_file')
@click.option('--rows', default=1000, help='Number of rows to generate')
@click.option('--db_path', default='data-modeling-practice.db', help='Path where to create the database file')
def generate_fake_data(schema_file, rows, db_path):
    """Generate fake data based on schema"""
    db = DuckDBManager(db_path)
    schema = generate_schema(schema_file)
    generate_data(schema, rows, db)
    click.echo(f"Generated {rows} rows of fake data")

@cli.command()
def run_dbt():
    """Run dbt models"""
    result = subprocess.run(['dbt', 'run'], cwd='./dbt_project', capture_output=True, text=True)
    click.echo(result.stdout)

@cli.command()
def close_db_connection():
    db = DuckDBManager()
    db.close()

if __name__ == '__main__':
    cli()