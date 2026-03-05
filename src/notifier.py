import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import toml
import logging
import os

logger = logging.getLogger('dq_engine')

def send_summary_email(html_content, failed_count, secrets_path="secrets.toml"):
    try:
        secrets = toml.load(secrets_path)['email']
    except Exception as e:
        logger.error(f"Could not load email config: {e}")
        return

    password = os.environ.get("SMTP_PASSWORD") or secrets.get('password')
    if not password:
        logger.error("No SMTP password provided for summary email.")
        return

    # Determine subject base on failures
    if failed_count > 0:
        subject = f"🚨 DQ FAILURE Summary: {failed_count} Tests Failed"
    else:
        subject = "✅ DQ SUCCESS Summary: All checks passed"

    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = secrets['sender_email']
    msg['To'] = ", ".join(secrets['recipients'])
    
    # Attach the passed HTML
    msg.attach(MIMEText(html_content, 'html'))

    try:
        server = smtplib.SMTP(secrets['smtp_server'], secrets['smtp_port'])
        server.starttls()
        server.login(secrets['sender_email'], password)
        server.sendmail(secrets['sender_email'], secrets['recipients'], msg.as_string())
        server.quit()
        logger.info("HTML Summary email sent.")
    except Exception as e:
        logger.error(f"Failed to send HTML summary email: {e}")