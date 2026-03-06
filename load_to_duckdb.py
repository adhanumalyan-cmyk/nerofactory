import duckdb
import os

conn = duckdb.connect('factory.duckdb')
conn.execute("DROP TABLE IF EXISTS production")
conn.execute("DROP TABLE IF EXISTS inventory")
conn.execute("DROP TABLE IF EXISTS employees")
conn.execute("DROP TABLE IF EXISTS transactions")

conn.execute("CREATE TABLE production AS SELECT * FROM 'backend/data/production_logs.csv'")
conn.execute("CREATE TABLE inventory AS SELECT * FROM 'backend/data/inventory_logs.csv'")
conn.execute("CREATE TABLE employees AS SELECT * FROM 'backend/data/employee_logs.csv'")
conn.execute("CREATE TABLE transactions AS SELECT * FROM 'backend/data/transactions.csv'")

print("Tables created.")
conn.close()