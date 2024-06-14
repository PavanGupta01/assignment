import boto3
from botocore.exceptions import NoCredentialsError


# S3 upload
def upload_to_s3(filename, bucket_name):
    s3 = boto3.client("s3")
    try:
        s3.upload_file(filename, bucket_name, filename)
        print("Upload Successful")
    except FileNotFoundError:
        print("The file was not found")
    except NoCredentialsError:
        print("Credentials not available")


# SES email
def send_email_with_attachment(to_email, subject, body, filename, from_email="your_verified_email@example.com"):
    ses = boto3.client("ses")
    with open(filename, "rb") as file:
        attachment = file.read()

    response = ses.send_raw_email(
        Source=from_email,
        Destinations=[to_email],
        RawMessage={
            "Data": (
                f"From: {from_email}\n"
                f"To: {to_email}\n"
                f"Subject: {subject}\n"
                f"MIME-Version: 1.0\n"
                f'Content-Type: multipart/mixed; boundary="simple-boundary"\n\n'
                f"--simple-boundary\n"
                f"Content-Type: text/plain\n\n"
                f"{body}\n\n"
                f"--simple-boundary\n"
                f'Content-Type: text/csv; name="{filename}"\n'
                f'Content-Disposition: attachment; filename="{filename}"\n'
                f"Content-Transfer-Encoding: base64\n\n"
                f"{attachment.decode('utf-8')}\n"
                f"--simple-boundary--"
            ).encode("utf-8")
        },
    )
    print("Email sent! Message ID:", response["MessageId"])
