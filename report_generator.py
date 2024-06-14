# Builtin Imports
import csv

# Thirdparty Imports
import boto3

# Local Imports
from db.connections import get_mysql_connection, get_pg_connection
from aws_integration import upload_to_s3, send_email_with_attachment

# REPORT_TARGET_DATE = date.today()
REPORT_TARGET_DATE = "2023-09-22"
S3_BUCKET_NAME = "lesson-completion-report"

# ToDo
# Rename var / methods
# Support for date range


def get_users() -> list[tuple]:
    """
    :returns List of users from Postgres
    """
    pg_conn = get_pg_connection()
    pg_cursor = pg_conn.cursor()

    # Fetch active users from PostgreSQL
    pg_query = """
    SELECT user_id, user_name
    FROM mindtickle_users
    WHERE active_status = 'active'
    """
    pg_cursor.execute(pg_query)
    active_users = pg_cursor.fetchall()

    pg_cursor.close()
    pg_conn.close()

    return active_users


def get_lessons(active_user_ids: tuple) -> list[tuple]:
    """
    :param Tuple of active user ids
    :returns List of users and respective number of lessons taken by the users.
    """
    # ToDo Check how many inactive users system has if that population is quite less than
    # It might make sense to remove active_user_ids parameter
    # as lessons are likely to be taken by active users only.

    mysql_conn = get_mysql_connection()
    mysql_cursor = mysql_conn.cursor()

    # Fetch lesson completions from MySQL
    mysql_query = """
    SELECT user_id, COUNT(lesson_id) as lessons_completed
    FROM lesson_completion
    WHERE completion_date = "{0}"
    AND user_id in {1}
    GROUP BY user_id
    """.format(
        REPORT_TARGET_DATE, active_user_ids
    )
    mysql_cursor.execute(mysql_query)
    user_lessons = mysql_cursor.fetchall()

    mysql_cursor.close()
    mysql_conn.close()

    return user_lessons


# Data aggregation
def aggregate_data():
    active_users = get_users()
    active_user_ids = tuple({user_id for user_id, _ in active_users})
    user_lessons = get_lessons(active_user_ids)

    return user_lessons, active_users


# CSV generation
def generate_csv(user_lessons, users, filename="report.csv"):
    with open(filename, mode="w") as file:
        writer = csv.writer(file)
        writer.writerow(["User Name", "Lessons Completed", "Date"])

        for user_id, lessons_completed in user_lessons:
            user_name = next(user_name for uid, user_name in users if uid == user_id)
            writer.writerow([user_name, lessons_completed, REPORT_TARGET_DATE])


# Main function
if __name__ == "__main__":
    # print("Sample report")
    user_lessons, active_users = aggregate_data()

    print(user_lessons)
    print(active_users)

    generate_csv(user_lessons, active_users)
    # upload_to_s3("report.csv", S3_BUCKET_NAME)
    # send_email_with_attachment(
    #     "recipient@example.com", "Daily Report", "Please find the attached report.", "report.csv"
    # )
