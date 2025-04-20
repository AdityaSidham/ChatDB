import csv
import mysql.connector
import os

def load_csv_to_mysql(csv_path, base_name):
    db_name = "chatdb"  # unified database for all tables
    table_name = base_name.lower()

    # Step 1: Connect to MySQL root to create shared DB
    root_conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="12345",
        allow_local_infile=True
    )
    root_cursor = root_conn.cursor()
    root_cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
    root_cursor.close()
    root_conn.close()

    # Step 2: Reconnect to the shared database
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="12345",
        database=db_name,
        allow_local_infile=True
    )
    cursor = conn.cursor()

    # Step 3: Read headers from CSV
    with open(csv_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        headers = next(reader)
    columns = ", ".join([f"`{col}` TEXT" for col in headers])

    # Step 4: Create table and load data
    cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
    cursor.execute(f"CREATE TABLE `{table_name}` ({columns})")
    cursor.execute(f"""
        LOAD DATA LOCAL INFILE '{os.path.abspath(csv_path)}'
        INTO TABLE `{table_name}`
        FIELDS TERMINATED BY ',' 
        OPTIONALLY ENCLOSED BY '"'
        LINES TERMINATED BY '\n'
        IGNORE 1 ROWS
    """)
    conn.commit()
    cursor.close()
    conn.close()

def run_mysql_query(sql):
    db_name = "chatdb"
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="12345",
        database=db_name,
        allow_local_infile=True
    )
    cursor = conn.cursor()
    cursor.execute(sql)
    if cursor.with_rows:
        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]
        result = [dict(zip(col_names, row)) for row in rows]
    else:
        result = "Query executed successfully"
    cursor.close()
    conn.close()
    return result
