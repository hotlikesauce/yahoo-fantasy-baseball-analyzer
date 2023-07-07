import smtplib, os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from dotenv import load_dotenv

load_dotenv()

def send_failure_email(error_message,file):
    #Get gmail pass
    password = os.environ.get('GMAIL_PASSWORD')
    sender_email = os.environ.get('GMAIL')
    receiver_email = os.environ.get('GMAIL')

    message = MIMEMultipart("alternative")
    message["Subject"] = "Summertime Sadness Error: "+file+" Failure"
    message["From"] = sender_email
    message["To"] = receiver_email

    text = f"An error occurred in your function:\n\n{error_message}"
    part1 = MIMEText(text, "plain")
    message.attach(part1)

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.ehlo()
        server.starttls()
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message.as_string())
        server.close()
        print("Email sent successfully")
    except Exception as e:
        print("Failed to send email. Error:", str(e))

def send_csvs(zip_file):
    #Get gmail pass
    password = os.environ.get('GMAIL_PASSWORD')
    sender_email = os.environ.get('GMAIL')
    receiver_email = os.environ.get('GMAIL_AUSTIN')


    message = MIMEMultipart("alternative")
    message["Subject"] = "Summertime Sadness Weekly Data Export"
    message["From"] = sender_email
    message["To"] = receiver_email

    text = f"Please see attached for weekly data exports "
    message.attach(MIMEText(text, "plain"))


    # Attach the zip file
    with open(zip_file, "rb") as file:
        part = MIMEBase("application", "zip")
        part.set_payload(file.read())
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f"attachment; filename=Summertime_Sadness.zip"
        )
        message.attach(part)

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.ehlo()
        server.starttls()
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message.as_string())
        server.close()
        print("Email sent successfully")
    except Exception as e:
        print("Failed to send email. Error:", str(e))
    