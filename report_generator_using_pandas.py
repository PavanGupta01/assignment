# Builtin Imports
import os

# Thirdparty Imports
import pandas as pd
import boto3

# Local Imports
from db.connections import get_mysql_connection, get_pg_connection
from aws_integration import upload_to_s3, send_email_with_attachment

START_DATE = "2021-01-01"
END_DATE = "2024-09-21"
S3_BUCKET_NAME = "lesson-completion-report"


def get_users(user_ids: tuple) -> pd.DataFrame:
    """
    :returns DataFrame of users from Postgres
    """
    pg_conn = get_pg_connection()

    # Fetch active users from PostgreSQL
    query = f"""
    SELECT user_id, user_name
    FROM mindtickle_users
    WHERE active_status = 'active'
    AND user_id IN {user_ids}
    """
    active_users = pd.read_sql_query(query, pg_conn)

    pg_conn.close()

    return active_users


def get_lessons() -> pd.DataFrame:
    """
    :returns DataFrame of users and respective number of lessons taken by the users.
    """
    mysql_conn = get_mysql_connection()

    # Fetch lesson completions from MySQL
    query = f"""
    SELECT user_id, completion_date, COUNT(lesson_id) as lessons_completed
    FROM lesson_completion
    WHERE completion_date BETWEEN '{START_DATE}' AND '{END_DATE}'
    GROUP BY user_id, completion_date
    """
    user_lessons = pd.read_sql_query(query, mysql_conn)

    mysql_conn.close()

    return user_lessons


# Data aggregation
def aggregate_data():
    user_lessons = get_lessons()
    distinct_user_ids = tuple(user_lessons["user_id"].unique())
    active_users = get_users(distinct_user_ids)

    # Merge active users with lessons data
    combined_data = pd.merge(user_lessons, active_users, on="user_id", how="left")
    combined_data = combined_data.dropna(subset=["user_name"])

    return combined_data


# CSV generation
def generate_csv(data: pd.DataFrame, filename="report.csv"):
    data["completion_date"] = pd.to_datetime(data["completion_date"])
    data.to_csv(filename, index=False, columns=["user_name", "lessons_completed", "completion_date"])


# Main function
if __name__ == "__main__":
    combined_data = aggregate_data()

    generate_csv(combined_data)
