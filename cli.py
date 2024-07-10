import rich_click as click
from schema_generator import generate_schema
from data_generator import generate_data
from db_manager import DuckDBManager
import subprocess

@click.group()
def cli():
    """Data Modeling Practice CLI"""
    pass

@cli.command()
@click.argument('schema_file')
def test_schema_function(schema_file):
    generate_schema(schema_file)

@cli.command()
@click.argument('schema_file')
def create_schema(schema_file):
    """Generate schema from YAML file"""
    schema = generate_schema(schema_file)
    db = DuckDBManager()
    db.execute_ddl(schema)
    click.echo(f"Schema created from {schema_file}")

@cli.command()
@click.argument('schema_file')
@click.option('--rows', default=1000, help='Number of rows to generate')
@click.option('--db_path', default='data-modeling-practice.db', help='Path where to create the database file')
def generate_fake_data(schema_file, rows, db_path):
    """Generate fake data based on schema"""
    data = generate_data(schema_file, rows, db_path)
    # db = DuckDBManager()
    # db.insert_data(data)
    click.echo(f"Generated {rows} rows of fake data")

@cli.command()
def run_dbt():
    """Run dbt models"""
    result = subprocess.run(['dbt', 'run'], cwd='./dbt_project', capture_output=True, text=True)
    click.echo(result.stdout)

if __name__ == '__main__':
    cli()