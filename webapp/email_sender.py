"""
Email sender - sends report HTML to contacts via SMTP.
"""
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path


def send_report_email(app_config, brand, report, recipients):
    """
    Send the client report HTML as an email to all recipients.

    app_config: Flask app.config dict with SMTP_* keys
    brand: brand dict from DB
    report: report dict from DB
    recipients: list of contact dicts with 'name' and 'email'
    """
    smtp_host = app_config.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = app_config.get("SMTP_PORT", 587)
    smtp_user = app_config.get("SMTP_USER", "")
    smtp_password = app_config.get("SMTP_PASSWORD", "")
    from_name = app_config.get("SMTP_FROM_NAME", "Agency Reports")
    from_email = app_config.get("SMTP_FROM_EMAIL", smtp_user)

    if not smtp_user or not smtp_password:
        raise ValueError("SMTP not configured. Set SMTP_USER and SMTP_PASSWORD environment variables.")

    # Read the client report HTML
    client_path = report.get("client_path", "")
    if not client_path or not Path(client_path).exists():
        raise FileNotFoundError(f"Client report not found: {client_path}")

    with open(client_path, "r", encoding="utf-8") as f:
        html_content = f.read()

    month = report.get("month", "")
    subject = f"{brand['display_name']} - Monthly Performance Report - {month}"

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)

        for contact in recipients:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = f"{from_name} <{from_email}>"
            msg["To"] = contact["email"]

            # Plain text fallback
            text_part = MIMEText(
                f"Hi {contact['name']},\n\n"
                f"Your monthly performance report for {brand['display_name']} ({month}) is ready.\n"
                f"Please view this email in an HTML-capable client to see the full report.\n\n"
                f"Best regards,\n{from_name}",
                "plain",
            )

            html_part = MIMEText(html_content, "html")

            msg.attach(text_part)
            msg.attach(html_part)

            server.sendmail(from_email, contact["email"], msg.as_string())
