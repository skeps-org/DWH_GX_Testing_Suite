import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import toml
import logging

logger = logging.getLogger('dq_engine')

def send_alert_email(failed_df, secrets_path="secrets.toml"):
    try:
        secrets = toml.load(secrets_path)['email']
    except Exception as e:
        logger.error(f"Could not load email config: {e}")
        return

    if failed_df.empty:
        return

    style = """
    <style>
        table { border-collapse: collapse; width: 100%; font-family: Arial, sans-serif; font-size: 14px; }
        th { background-color: #333; color: white; padding: 10px; border: 1px solid #ddd; text-align: left; }
        td { padding: 8px; border: 1px solid #ddd; }
        .critical { background-color: #ffe6e6; color: #b30000; font-weight: bold; }
        .warning { background-color: #fff4e6; color: #cc7a00; }
        .error { background-color: #e0e0e0; color: #333; font-style: italic; }
        .header { background-color: #d32f2f; color: white; padding: 15px; text-align: center; border-radius: 5px; }
    </style>
    """

    rows_html = ""
    for _, row in failed_df.iterrows():
        severity = row.get('severity', 'warning')
        status = row.get('status', '')
        
        row_class = "warning"
        if status == "CRITICAL_ERROR": row_class = "error"
        elif severity == "critical": row_class = "critical"
            
        error_txt = f"<br><small>{row.get('error_msg', '')}</small>" if row.get('error_msg') else ""

        rows_html += f"""
        <tr class="{row_class}">
            <td>{row['lender']}</td>
            <td>{row['test_name']}</td>
            <td>{row.get('failed_rows', 0)}</td>
            <td>{severity.upper()}</td>
            <td>{status}{error_txt}</td>
        </tr>
        """

    body = f"""
    <html>
    <head>{style}</head>
    <body>
        <div class="header"><h2>🚨 Data Quality Alert</h2></div>
        <p>Issues detected in <b>{len(failed_df)}</b> tests.</p>
        <table>
            <thead>
                <tr><th>Lender</th><th>Test Case</th><th>Failures</th><th>Severity</th><th>Status</th></tr>
            </thead>
            <tbody>{rows_html}</tbody>
        </table>
    </body>
    </html>
    """

    msg = MIMEMultipart()
    msg['Subject'] = f"🚨 DQ FAILURE: {len(failed_df)} Issues Detected"
    msg['From'] = secrets['sender_email']
    msg['To'] = ", ".join(secrets['recipients'])
    msg.attach(MIMEText(body, 'html'))

    try:
        server = smtplib.SMTP(secrets['smtp_server'], secrets['smtp_port'])
        server.starttls()
        server.login(secrets['sender_email'], secrets['password'])
        server.sendmail(secrets['sender_email'], secrets['recipients'], msg.as_string())
        server.quit()
        logger.info("Alert email sent.")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")