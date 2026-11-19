import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from faker import Faker

# Base connection parameters for PostgreSQL
DB_PARAMS = {
    "user": "postgres",
    "password": "1431",
    "host": "localhost",
    "port": "5432"
}

TARGET_DB = "OID_db"

def create_database():
    """Connects to system DB and creates a new database if it does not exist."""
    try:
        conn = psycopg2.connect(dbname="postgres", **DB_PARAMS)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        # Check if target database already exists
        cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s;", (TARGET_DB,))
        exists = cursor.fetchone()

        if not exists:
            cursor.execute(f'CREATE DATABASE "{TARGET_DB}";')
            print(f"Database '{TARGET_DB}' created successfully.")
        else:
            print(f"Database '{TARGET_DB}' already exists.")

        cursor.close()
        conn.close()
    except Exception as error:
        print(f"Error while creating database: {error}")
        raise

def main():
    # 1. Create the database
    create_database()

    # Initialize Faker for English data generation
    fake = Faker("en_US")

    conn = None
    cursor = None

    try:
        # 2. Connect to the newly created database
        conn = psycopg2.connect(dbname=TARGET_DB, **DB_PARAMS)
        cursor = conn.cursor()
        print(f"Connected to database '{TARGET_DB}'.")

        # -------------------------------------------------------------
        # CREATE TABLE
        # -------------------------------------------------------------
        create_table_query = """
        DROP TABLE IF EXISTS employees;
        CREATE TABLE employees (
            id SERIAL PRIMARY KEY,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            salary NUMERIC(10, 2) CHECK (salary > 0),
            hire_date DATE DEFAULT CURRENT_DATE
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        print("Table 'employees' created successfully.")

        # -------------------------------------------------------------
        # INSERT GENERATED DATA
        # -------------------------------------------------------------
        employees_data = []
        for _ in range(15):
            first_name = fake.first_name()
            last_name = fake.last_name()
            email = fake.unique.email()
            salary = round(fake.random_number(digits=5, fix_len=True) / 10, 2)
            hire_date = fake.date_between(start_date="-5y", end_date="today")
            employees_data.append((first_name, last_name, email, salary, hire_date))

        insert_query = """
        INSERT INTO employees (first_name, last_name, email, salary, hire_date)
        VALUES (%s, %s, %s, %s, %s);
        """
        cursor.executemany(insert_query, employees_data)
        conn.commit()
        print(f"Inserted {len(employees_data)} generated employee records.")

        # -------------------------------------------------------------
        # SELECT DATA
        # -------------------------------------------------------------
        print("\n--- SELECT: Employees with salary > 5000 ---")
        select_query = """
        SELECT id, first_name, last_name, email, salary
        FROM employees
        WHERE salary > %s
        ORDER BY salary DESC;
        """
        cursor.execute(select_query, (5000,))
        rows = cursor.fetchall()

        for row in rows:
            print(f"ID: {row[0]} | Name: {row[1]} {row[2]} | Email: {row[3]} | Salary: ${row[4]}")

        # -------------------------------------------------------------
        # UPDATE DATA
        # -------------------------------------------------------------
        # Update salary for the first employee in the table
        cursor.execute("SELECT id FROM employees ORDER BY id LIMIT 1;")
        target_id = cursor.fetchone()[0]

        update_query = "UPDATE employees SET salary = salary + %s WHERE id = %s RETURNING salary;"
        cursor.execute(update_query, (1500.00, target_id))
        new_salary = cursor.fetchone()[0]
        conn.commit()
        print(f"\n--- UPDATE: Employee ID {target_id} salary updated to ${new_salary} ---")

        # -------------------------------------------------------------
        # DELETE DATA
        # -------------------------------------------------------------
        # Delete employees hired before 3 years ago
        delete_query = """
        DELETE FROM employees
        WHERE hire_date < CURRENT_DATE - INTERVAL '3 years'
        RETURNING id, first_name, last_name;
        """
        cursor.execute(delete_query)
        deleted_records = cursor.fetchall()
        conn.commit()

        print(f"\n--- DELETE: Removed {len(deleted_records)} employees hired more than 3 years ago ---")
        for emp in deleted_records:
            print(f"Removed ID: {emp[0]} | Name: {emp[1]} {emp[2]}")

    except Exception as error:
        print(f"Database operation failed: {error}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            print("\nPostgreSQL connection closed.")

if __name__ == "__main__":
    main()