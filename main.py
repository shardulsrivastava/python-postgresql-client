import psycopg2
import os
import time
import logging
import sys
from pprint import pformat

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', stream=sys.stdout, level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')

DATABASE_URL = os.environ.get("DATABASE_URL", "localhost")
DATABASE_NAME = os.environ.get("DATABASE_NAME", "tgam")
DATABASE_USERNAME = os.environ.get("DATABASE_USERNAME", "postgres")
DATABASE_PASSWORD = os.environ.get("DATABASE_PASSWORD", "postgres")
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
    except Exception as e:
        logging.error(f"Database connection could not be established => {e}")


def execute_ping(connection):
    #print(connection.closed)  # 0
    # this query will fail because the db is no longer connected
    try:
        cur = connection.cursor()
        cur.execute('SELECT 1')
        logging.info("Database is reachable.")
    except Exception as e:
        logging.error(f"Database is not reachable => {e}")
    #print(connection.closed)  # 2


def ping_postgres():
    logging.info(f"Connecting to the database => {DATABASE_URL}")
    connection = connect()

    logging.info(f"Connection established => {pformat(connection)}")
    while True:
        execute_ping(connection)
        # Sleep for 50 miliseconds by default
        time.sleep(float(SLEEP_TIME))


if __name__ == '__main__':
    logging.info("Start Python Postgres Client")
    ping_postgres()
