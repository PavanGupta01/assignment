import os

from dotenv import load_dotenv
import psycopg2
import mysql.connector

# .env file should replaced by secret manager
# and will be different for environments e.g. development, qa, stage and production.
load_dotenv(dotenv_path="setup/.env")


# Database connections


# Postgres connection
def get_pg_connection():
    return psycopg2.connect(
        host="localhost",  # Keeping it to localhost for testing.
        port=5433,  # Updated port to match Docker mapping
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


# MySQL connection
def get_mysql_connection():
    return mysql.connector.connect(
        host="localhost",  # Keeping it to localhost for testing.
        port=3306,  # Updated port to match Docker mapping
        database=os.getenv("MYSQL_DATABASE"),
        user=os.getenv("MYSQL_user"),
        password=os.getenv("MYSQL_ROOT_PASSWORD"),
    )
