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


def send_beta_welcome_email(app_config, tester, temp_password, login_url):
    """
    Send welcome email to an approved beta tester with login credentials
    and setup instructions.
    """
    smtp_host = app_config.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = app_config.get("SMTP_PORT", 587)
    smtp_user = app_config.get("SMTP_USER", "")
    smtp_password = app_config.get("SMTP_PASSWORD", "")
    from_name = app_config.get("SMTP_FROM_NAME", "GroMore")
    from_email = app_config.get("SMTP_FROM_EMAIL", smtp_user)

    if not smtp_user or not smtp_password:
        raise ValueError("SMTP not configured.")

    name = tester.get("name", "there")
    email = tester["email"]
    subject = "Welcome to the GroMore Beta!"

    html = f"""
    <div style="font-family:Inter,Arial,sans-serif;max-width:600px;margin:0 auto;padding:24px;">
        <h2 style="color:#4f46e5;">Welcome to GroMore, {name}!</h2>
        <p>You've been approved for the GroMore beta program. Here's everything you need to get started.</p>

        <div style="background:#f0f0ff;border-radius:10px;padding:20px;margin:20px 0;">
            <h3 style="margin-top:0;">Your Login Credentials</h3>
            <p><strong>Login URL:</strong> <a href="{login_url}">{login_url}</a></p>
            <p><strong>Email:</strong> {email}</p>
            <p><strong>Temporary Password:</strong> {temp_password}</p>
            <p style="font-size:0.85em;color:#666;">Please change your password after your first login.</p>
        </div>

        <h3>Getting Started - 3 Steps</h3>
        <ol style="line-height:1.8;">
            <li><strong>Log in</strong> using the credentials above</li>
            <li><strong>Connect your Google account</strong> via Settings &gt; Connections</li>
            <li><strong>Connect your Facebook account</strong> via Settings &gt; Connections</li>
        </ol>

        <div style="background:#fff7ed;border-radius:10px;padding:20px;margin:20px 0;">
            <h3 style="margin-top:0;">Facebook Setup (Important)</h3>
            <p>To connect Facebook ads data, you may need to:</p>
            <ol style="line-height:1.8;">
                <li>Accept any <strong>tester invitations</strong> sent to your Facebook account</li>
                <li>If prompted, register as a Facebook developer at <strong>developers.facebook.com</strong></li>
            </ol>
            <p style="font-size:0.85em;color:#666;">We'll send tester invites separately if needed.</p>
        </div>

        <div style="background:#f0fdf4;border-radius:10px;padding:20px;margin:20px 0;">
            <h3 style="margin-top:0;">Share Your Feedback</h3>
            <p>As a beta tester, your feedback is invaluable. Use the <strong>Feedback</strong>
            link in your dashboard sidebar to report bugs, request features, or share what you like.</p>
        </div>

        <p>Questions? Reply to this email and we'll help you out.</p>
        <p style="color:#666;">- The GroMore Team</p>
    </div>
    """

    text = (
        f"Welcome to GroMore, {name}!\n\n"
        f"Login URL: {login_url}\n"
        f"Email: {email}\n"
        f"Temporary Password: {temp_password}\n\n"
        f"Steps:\n"
        f"1. Log in with the credentials above\n"
        f"2. Connect your Google account via Settings > Connections\n"
        f"3. Connect your Facebook account via Settings > Connections\n\n"
        f"Use the Feedback link in your sidebar to share bugs, features, or comments.\n\n"
        f"- The GroMore Team"
    )

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"{from_name} <{from_email}>"
        msg["To"] = email
        msg.attach(MIMEText(text, "plain"))
        msg.attach(MIMEText(html, "html"))
        server.sendmail(from_email, email, msg.as_string())
