import requests

SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/XXXX/YYYY/ZZZZ"

def send_slack_alert(message):
    """Send alert to Slack channel."""
    payload = {"text": f":rotating_light: {message}"}
    try:
        requests.post(SLACK_WEBHOOK_URL, json=payload)
    except Exception as e:
        print("Slack alert failed:", e)

import smtplib
from email.mime.text import MIMEText

EMAIL_FROM = "alerts@company.com"
EMAIL_TO = ["dataops@company.com"]
SMTP_SERVER = "smtp.company.com"

def send_email_alert(subject, body):
    """Send email alert."""
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(EMAIL_TO)
    try:
        with smtplib.SMTP(SMTP_SERVER) as server:
            server.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())
    except Exception as e:
        print("Email alert failed:", e)

def extend_run_notebook_with_verification()
# Determine if compliance failed
if status == "FAILED" or not row_count_match or not data_quality_match:
    alert_msg = (
        f"Migration Alert!\n"
        f"Notebook: {notebook_config['path']}\n"
        f"Params: {notebook_config['params']}\n"
        f"Status: {status}\n"
        f"Row Count Match: {row_count_match}\n"
        f"Data Quality Match: {data_quality_match}\n"
        f"Error: {error_msg if error_msg else 'N/A'}"
    )

    # Send alerts
    send_slack_alert(alert_msg)
    send_email_alert("Migration Compliance Alert", alert_msg)
