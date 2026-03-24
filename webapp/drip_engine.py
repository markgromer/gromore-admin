"""
Drip email engine - processes pending drip sends via SMTP.

Merge fields available in email body_html and subject:
  {{name}}           - lead's name
  {{email}}          - lead's email
  {{unsubscribe_url}} - one-click unsubscribe link
  {{signup_url}}     - link to sign up / get started
  {{assess_url}}     - link to run another assessment
"""
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def _merge(template, data):
    """Simple {{key}} replacement."""
    result = template
    for key, val in data.items():
        result = result.replace("{{" + key + "}}", str(val))
    return result


def process_pending_drips(app_config, db):
    """Check for due drip sends and fire them. Returns (sent, failed) counts."""
    smtp_host = app_config.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = app_config.get("SMTP_PORT", 587)
    smtp_user = app_config.get("SMTP_USER", "")
    smtp_password = app_config.get("SMTP_PASSWORD", "")
    from_name = app_config.get("SMTP_FROM_NAME", "GroMore")
    from_email = app_config.get("SMTP_FROM_EMAIL", smtp_user)
    app_url = app_config.get("APP_URL", "https://gromore-admin.onrender.com")

    if not smtp_user or not smtp_password:
        return 0, 0

    pending = db.get_pending_drip_sends()
    if not pending:
        # Also check for completed enrollments
        db.check_and_complete_finished_enrollments()
        return 0, 0

    sent = 0
    failed = 0

    try:
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_password)
    except Exception:
        # Can't connect - record failures
        for p in pending:
            db.record_drip_send(p["enrollment_id"], p["step_id"], p["current_step"], "failed", "SMTP connection error")
        return 0, len(pending)

    for p in pending:
        merge_data = {
            "name": p.get("name") or "there",
            "email": p["email"],
            "unsubscribe_url": f"{app_url}/client/unsubscribe/{p['enrollment_id']}",
            "signup_url": f"{app_url}/client/login",
            "assess_url": f"{app_url}/client/assess",
        }

        subject = _merge(p["subject"], merge_data)
        body_html = _merge(p["body_html"], merge_data)
        body_text = _merge(p.get("body_text") or "", merge_data)

        # Append unsubscribe footer
        unsub_link = merge_data["unsubscribe_url"]
        body_html += (
            f'\n<div style="margin-top:32px;padding-top:16px;border-top:1px solid #e5e7eb;'
            f'text-align:center;font-size:12px;color:#9ca3af;">'
            f'<a href="{unsub_link}" style="color:#9ca3af;">Unsubscribe</a>'
            f'</div>'
        )
        if body_text:
            body_text += f"\n\n---\nUnsubscribe: {unsub_link}"

        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = f"{from_name} <{from_email}>"
            msg["To"] = p["email"]
            msg["List-Unsubscribe"] = f"<{unsub_link}>"

            if body_text:
                msg.attach(MIMEText(body_text, "plain"))
            msg.attach(MIMEText(body_html, "html"))

            server.sendmail(from_email, p["email"], msg.as_string())
            db.record_drip_send(p["enrollment_id"], p["step_id"], p["current_step"], "sent")
            sent += 1
        except Exception as exc:
            db.record_drip_send(p["enrollment_id"], p["step_id"], p["current_step"], "failed", str(exc)[:200])
            failed += 1

    try:
        server.quit()
    except Exception:
        pass

    # Mark enrollments that have finished all steps
    db.check_and_complete_finished_enrollments()

    return sent, failed
