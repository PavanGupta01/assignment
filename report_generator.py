# Builtin Imports
import csv

# Thirdparty Imports
import boto3

# Local Imports
from db.connections import get_mysql_connection, get_pg_connection
from aws_integration import upload_to_s3, send_email_with_attachment

START_DATE = "2021-01-01"
END_DATE = "2024-09-21"
S3_BUCKET_NAME = "lesson-completion-report"


def get_users(user_ids: tuple) -> dict:
    """
    :returns List of users from Postgres
    """
    pg_conn = get_pg_connection()
    pg_cursor = pg_conn.cursor()

    # Fetch active users from PostgreSQL
    # NOTE Query can be replaced with the ORM (SQLAlchemy) models
    # NOTE For large data volume we might want to paginate the query with LIMIT and OFFSET
    pg_query = """
    SELECT user_id, user_name
    FROM mindtickle_users
    WHERE active_status = 'active'
    AND user_id in {0}
    """.format(
        user_ids
    )
    pg_cursor.execute(pg_query)
    active_users = dict(pg_cursor.fetchall())

    pg_cursor.close()
    pg_conn.close()

    return active_users


def get_lessons() -> list[tuple]:
    """
    :returns List of users and respective number of lessons taken by the users.
    """

    mysql_conn = get_mysql_connection()
    mysql_cursor = mysql_conn.cursor()

    # Fetch lesson completions from MySQL
    mysql_query = """
    SELECT user_id, completion_date, COUNT(lesson_id) as lessons_completed
    FROM lesson_completion
    WHERE completion_date between "{0}" AND "{1}"
    GROUP BY user_id, completion_date
    """.format(
        START_DATE, END_DATE
    )
    mysql_cursor.execute(mysql_query)
    user_lessons = mysql_cursor.fetchall()

    mysql_cursor.close()
    mysql_conn.close()

    return user_lessons


# Data aggregation
def aggregate_data():
    # Assuming lessons will have a less data as compared to users.
    user_lessons = get_lessons()
    distinct_user_ids = tuple({user_id for user_id, _, _ in user_lessons})
    active_users = get_users(distinct_user_ids)

    return user_lessons, active_users


# CSV generation
def generate_csv(user_lessons, users, filename="report.csv"):
    with open(filename, mode="w") as file:
        writer = csv.writer(file)
        writer.writerow(["User Name", "Lessons Completed", "Date"])

        for user_id, completion_date, lessons_completed in user_lessons:
            user_name = users.get(user_id)
            if user_name:
                writer.writerow([user_name, lessons_completed, completion_date])


# Main function
if __name__ == "__main__":
    user_lessons, active_users = aggregate_data()

    generate_csv(user_lessons, active_users)
    upload_to_s3("report.csv", S3_BUCKET_NAME)
    send_email_with_attachment(
        "recipient@example.com", "Daily Report", "Please find the attached lesson completion report.", "report.csv"
    )
