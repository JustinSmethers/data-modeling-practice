import unittest
import os
import duckdb
from data_generator.fake_data_generator import (
    generate_fake_data, 
    generate_fake_value, 
    validate_custom_constraint, 
    validate_schema_data_types, 
    apply_custom_constraint, 
    generate_data
)
from schema_generator.schema_definition import load_schema
from datetime import datetime, timedelta, date
from typing import Dict, Any

class TestFakeDataGenerator(unittest.TestCase):

    def setUp(self):
        self.schema = {
            'customers': {
                'columns': {
                    'id': 'INTEGER PRIMARY KEY',
                    'name': 'VARCHAR',
                    'email': 'VARCHAR UNIQUE',
                    'created_at': 'DATE'
                },
            },
            'orders': {
                'columns': {
                    'id': 'INTEGER PRIMARY KEY',
                    'customer_id': 'INTEGER REFERENCES customers(id)',
                    'order_date': 'DATE',
                    'total_amount': 'FLOAT'
                },
                'custom_constraints': {
                    'order_date': 'AFTER customers.created_at'
                }
            }
        }
        self.db_path = 'test_data_modeling_practice.db'
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def tearDown(self):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_generate_fake_value(self):
        self.assertIsInstance(generate_fake_value('INTEGER'), int)
        self.assertIsInstance(generate_fake_value('FLOAT'), float)
        self.assertIsInstance(generate_fake_value('VARCHAR'), str)
        self.assertIsInstance(generate_fake_value('DATE'), str)

    def test_validate_custom_constraint(self):
        validate_custom_constraint('BETWEEN 1 AND 100')
        validate_custom_constraint('AFTER customers.created_at')
        validate_custom_constraint('BEFORE orders.order_date')
        with self.assertRaises(ValueError):
            validate_custom_constraint('INVALID CONSTRAINT')

    def test_validate_schema_data_types(self):
        validate_schema_data_types(self.schema)
        invalid_schema = {
            'customers': {
                'columns': {
                    'id': 'INVALID_TYPE'
                }
            }
        }
        with self.assertRaises(ValueError):
            validate_schema_data_types(invalid_schema)

    def test_apply_custom_constraint(self):
        conn = duckdb.connect(':memory:')
        conn.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, created_at DATE);")
        conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER REFERENCES customers(id), order_date DATE);")
        conn.execute("INSERT INTO customers (id, created_at) VALUES (?, ?)", (1, (datetime.now() - timedelta(days=10)).date()))
        conn.execute("INSERT INTO orders (id, customer_id, order_date) VALUES (?, ?, ?)", (1, 1, None))
        
        apply_custom_constraint(conn, 'orders', 'order_date', 'AFTER customers.created_at')
        
        result = conn.execute("SELECT order_date FROM orders WHERE id = 1").fetchone()
        self.assertIsNotNone(result[0])
        self.assertIsInstance(result[0], date)

    def test_generate_fake_data(self):
        conn = generate_fake_data(self.schema, 10, self.db_path)
        customers = conn.execute("SELECT * FROM customers").fetchall()
        orders = conn.execute("SELECT * FROM orders").fetchall()
        self.assertEqual(len(customers), 10)
        self.assertEqual(len(orders), 10)

        # Query table schema from DuckDB
        customer_columns = [col[0] for col in conn.execute("DESCRIBE SELECT * FROM CUSTOMERS").fetchall()]
        order_columns = [col[0] for col in conn.execute("DESCRIBE SELECT * FROM ORDERS").fetchall()]

        self.assertIn('id', customer_columns)
        self.assertIn('order_date', order_columns)


if __name__ == '__main__':
    unittest.main()