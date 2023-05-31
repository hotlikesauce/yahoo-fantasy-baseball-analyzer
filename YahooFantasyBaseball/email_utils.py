import smtplib, os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
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