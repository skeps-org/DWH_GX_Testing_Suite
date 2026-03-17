import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import toml
import logging
import os

logger = logging.getLogger('dq_engine')

def send_summary_email(html_content, failed_count, secrets_path="secrets.toml", attachment_path=None):
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
    
    # Attach file if provided
    if attachment_path and os.path.exists(attachment_path):
        with open(attachment_path, 'rb') as f:
            part = MIMEApplication(f.read(), Name=os.path.basename(attachment_path))
            part['Content-Disposition'] = f'attachment; filename="{os.path.basename(attachment_path)}"'
            msg.attach(part)

    try:
        server = smtplib.SMTP(secrets['smtp_server'], secrets['smtp_port'])
        server.starttls()
        server.login(secrets['sender_email'], password)
        server.sendmail(secrets['sender_email'], secrets['recipients'], msg.as_string())
        server.quit()
        logger.info("HTML Summary email sent.")
    except Exception as e:
        logger.error(f"Failed to send HTML summary email: {e}")