#!/usr/bin/env python3

import snowflake.connector
import sqlparse

# ---------------------------------------------------------------
# Connection details — fill these in
# ---------------------------------------------------------------
ACCOUNT = "account"
USER = "user"
PASSWORD = "password"
WAREHOUSE = "Warehouse"
DATABASE = "Database Name"
SCHEMA = "Schema Name"
ROLE = "Role Name"

# ---------------------------------------------------------------
# Connect to Snowflake
# ---------------------------------------------------------------
ctx = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA,
    role=ROLE
)

cs = ctx.cursor()

# ---------------------------------------------------------------
# Read SQL from file
# ---------------------------------------------------------------
sql_file_path = "snowflake.sql"  # Or your correct file path

with open(sql_file_path, "r") as f:
    sql_script = f.read()

# ---------------------------------------------------------------
# Execute each statement with sqlparse
# ---------------------------------------------------------------
try:
    statements = sqlparse.split(sql_script)

    for index, statement in enumerate(statements, start=1):
        stmt = statement.strip()
        if not stmt:
            continue  # skip empty

        print(f"\n [Statement #{index}] ----------------------")
        print(stmt)
        print("-------------------------------------------------------")

        try:
            cs.execute(stmt)

            if cs.description:  # if results to fetch
                rows = cs.fetchall()
                print(f"Results for Statement #{index}:")
                for row in rows:
                    print(row)
            else:
                print(f"Statement #{index} executed successfully (no rows).")

        except Exception as e:
            print(f"Error in Statement #{index}: {e}")

finally:
    cs.close()
    ctx.close()
    print("\n All statements processed. Connection closed.")