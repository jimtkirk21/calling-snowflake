# Snowflake SQL Runner with Python

This repository contains a simple Python script to connect to Snowflake, read SQL statements from a file, execute them one-by-one, and print results for each statement.  
It uses [`sqlparse`](https://github.com/andialbrecht/sqlparse) to safely split multi-statement SQL scripts.

---

## Project Structure

.
├── calling_snowflake.py # Python script
├── snowflake.sql # Your SQL statements
└── README.md # This file

---

## Requirements

- Python 3.8+
- Snowflake Python Connector
- `sqlparse` module

---

## Installation

1. Clone the repo

   ```bash
   git clone https://github.com/jimtkirk/your-repo.git
   cd your-repo
Create a virtual environment (optional but recommended)

bash
Copy
Edit
python -m venv venv
source venv/bin/activate  # macOS/Linux
venv\Scripts\activate     # Windows
Install dependencies

bash
Copy
Edit
pip install snowflake-connector-python sqlparse
Configuration
In calling_snowflake.py, fill in your Snowflake credentials:

python
Copy
Edit
ACCOUNT = "<your_account>.snowflakecomputing.com"
USER = "<your_user>"
PASSWORD = "<your_password>"
WAREHOUSE = "<your_warehouse>"
DATABASE = "<your_database>"
SCHEMA = "<your_schema>"
ROLE = "<your_role>"
Do not commit your credentials to a public repository.
Use environment variables or a secrets manager in production.

Your SQL File
Put all your SQL statements in snowflake.sql.

Example:

sql
Copy
Edit
USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS MY_DB;
USE DATABASE MY_DB;
-- Add more SQL statements here...
Separate each statement with a ;.

Run the Script
bash
Copy
Edit
python calling_snowflake.py
The script will:

Connect to Snowflake

Read your snowflake.sql file

Split it into statements

Execute each statement in order

Print results for each statement

Example Output
pgsql
Copy
Edit
[Statement #1] ----------------------
USE ROLE ACCOUNTADMIN
-------------------------------------------------------
Statement #1 executed successfully (no results).

[Statement #2] ----------------------
CREATE DATABASE IF NOT EXISTS MY_DB
-------------------------------------------------------
Results for Statement #2:
('MY_DB successfully created.',)
Notes
Uses sqlparse.split() to handle ; safely.

Handles empty statements.

Closes connection and cursor gracefully.

Add your own error handling or logging as needed.

Contributing
Pull requests welcome. Feel free to submit issues or improvements.

License
MIT License. See LICENSE file.