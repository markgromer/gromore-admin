"""
Background jobs blueprint - report generation triggers, scheduled tasks.
"""
from flask import Blueprint, current_app, flash, redirect, url_for, session, request
from datetime import datetime

jobs_bp = Blueprint("jobs", __name__)


@jobs_bp.route("/generate-all", methods=["POST"])
def generate_all_reports():
    if "user_id" not in session:
        return redirect(url_for("login"))

    month = request.form.get("month", datetime.now().strftime("%Y-%m"))
    db = current_app.db
    brands = db.get_all_brands()

    from webapp.report_runner import run_report_for_brand

    success_count = 0
    fail_count = 0
    for brand in brands:
        try:
            result = run_report_for_brand(db, brand, month)
            if result["success"]:
                success_count += 1
            else:
                fail_count += 1
        except Exception:
            fail_count += 1

    flash(f"Generated reports: {success_count} succeeded, {fail_count} failed", "success" if fail_count == 0 else "warning")
    return redirect(url_for("dashboard"))


@jobs_bp.route("/send-all", methods=["POST"])
def send_all_reports():
    if "user_id" not in session:
        return redirect(url_for("login"))

    month = request.form.get("month", datetime.now().strftime("%Y-%m"))
    db = current_app.db

    from webapp.email_sender import send_report_email

    sent = 0
    skipped = 0
    brands = db.get_all_brands()
    for brand in brands:
        reports = db.get_brand_reports(brand["id"], limit=1)
        if not reports or reports[0]["month"] != month:
            skipped += 1
            continue
        report = reports[0]
        if report.get("sent_at"):
            skipped += 1
            continue
        contacts = db.get_brand_contacts(brand["id"])
        recipients = [c for c in contacts if c.get("auto_send")]
        if not recipients:
            skipped += 1
            continue
        try:
            send_report_email(current_app.config, brand, report, recipients)
            db.mark_report_sent(report["id"])
            sent += 1
        except Exception:
            skipped += 1

    flash(f"Sent {sent} reports, skipped {skipped}", "success" if sent > 0 else "info")
    return redirect(url_for("dashboard"))
