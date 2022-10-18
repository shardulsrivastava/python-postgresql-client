import psycopg2
from psycopg2 import pool
import os
import time
import logging
import sys
from pprint import pformat

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', stream=sys.stdout, level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')

DATABASE_URL = os.environ.get("DATABASE_URL", "localhost")
DATABASE_NAME = os.environ.get("DATABASE_NAME", "tgam")
DATABASE_USERNAME = os.environ.get("DATABASE_USERNAME", "postgres")
DATABASE_PASSWORD = os.environ.get("DATABASE_PASSWORD", "postgres")
DATABASE_PORT = os.environ.get("DATABASE_PORT", "5432")

SLEEP_TIME = os.environ.get("SLEEP_TIME", "1")


def create_connection_pool():
    return psycopg2.pool.SimpleConnectionPool(1, 20, database=DATABASE_NAME,
                                              user=DATABASE_USERNAME,
                                              password=DATABASE_PASSWORD,
                                              host=DATABASE_URL,
                                              port=DATABASE_PORT)


def execute_ping(connection):
    try:
        cur = connection.cursor()
        cur.execute('SELECT 1')
        logging.info("Database is reachable.")
    except Exception as e:
        logging.error(f"Database is not reachable => {e}")


def ping_postgres():
    logging.info(f"Connecting to the database => {DATABASE_URL}")
    connection_pool = create_connection_pool()
    while True:
        try:
            connection = connection_pool.getconn()
            if connection:
                # if connection.closed returns 0 then database is connected, and it's closed for non-zero value.
                if connection.closed == 0:
                    execute_ping(connection)
                else:
                    logging.info(f"Database is not reachable. Connection not active => {connection}")
                connection_pool.putconn(connection)
            else:
                logging.info(f"Database is not reachable")
        except:
            logging.info(f"Database is not reachable")

        # Sleep for 50 miliseconds by default
        time.sleep(float(SLEEP_TIME))


if __name__ == '__main__':
    logging.info("Starting Python PostgreSQL Client")
    ping_postgres()
