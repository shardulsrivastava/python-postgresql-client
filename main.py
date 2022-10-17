import psycopg2
import os
import time

DATABASE_URL = os.environ.get("DATABASE_URL", "localhost")
DATABASE_NAME = os.environ.get("DATABASE_NAME", "tgam")
DATABASE_USERNAME = os.environ.get("DATABASE_USERNAME", "postgres")
DATABASE_PASSWORD = os.environ.get("DATABASE_PASSWORD", "postgres")
DATABASE_PORT = os.environ.get("DATABASE_PORT", "5432")

DATABASE_PORT = os.environ.get("DATABASE_PORT", "5432")

SLEEP_TIME = os.environ.get("SLEEP_TIME", "0.1")

def connect():
    try:
        return psycopg2.connect(
            dbname=DATABASE_NAME,
            user=DATABASE_USERNAME,
            password=DATABASE_PASSWORD,
            host=DATABASE_URL,
            port=DATABASE_PORT
        )
    except psycopg2.OperationalError:
        print("Database is not reachable")


def execute_ping(connection):
    #print(connection.closed)  # 0
    # this query will fail because the db is no longer connected
    try:
        cur = connection.cursor()
        cur.execute('SELECT 1')
        print("Database is reachable.")
    except psycopg2.OperationalError:
        pass
    #print(connection.closed)  # 2


def ping_postgres():
    print("Connecting to the database =>", DATABASE_URL)
    connection = connect()
    while True:
        execute_ping(connection)
        # Sleep for 50 miliseconds by default
        time.sleep(float(SLEEP_TIME))


if __name__ == '__main__':
    ping_postgres()
