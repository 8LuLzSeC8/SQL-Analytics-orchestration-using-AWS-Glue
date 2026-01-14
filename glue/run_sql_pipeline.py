import psycopg2
import os

SQL_FILES = [
    "sql/03_core_fct_trips.sql",
    "sql/04_dq_framework.sql",
    "sql/05_dq_validations.sql",
    "sql/06_dq_tests.sql"
]

def load_sql(path):
    with open(path, "r") as f:
        return f.read()

def main():
    conn = psycopg2.connect(
        host=os.environ["PG_HOST"],
        port=os.environ["PG_PORT"],
        dbname=os.environ["PG_DB"],
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"]
    )
    conn.autocommit = True

    cur = conn.cursor()

    for sql_file in SQL_FILES:
        print(f"Running {sql_file}")
        sql = load_sql(sql_file)
        cur.execute(sql)

    cur.close()
    conn.close()

    print("SQL pipeline completed successfully")

if __name__ == "__main__":
    main()
