"""
Background jobs blueprint - report generation triggers, scheduled tasks.
"""
from flask import Blueprint, current_app, flash, redirect, url_for, session, request, jsonify
from datetime import datetime
import os
import logging

logger = logging.getLogger(__name__)

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


@jobs_bp.route("/sync-sng-revenue", methods=["POST"])
def sync_sng_revenue_all():
    """Batch sync SNG revenue for all brands with Sweep and Go configured.
    Pulls previous complete month payment data for each brand."""
    if "user_id" not in session:
        return redirect(url_for("login"))

    db = current_app.db
    brands = db.get_all_brands()

    from webapp.crm_bridge import sng_sync_revenue

    synced = 0
    skipped = 0
    for brand in brands:
        if brand.get("crm_type") != "sweepandgo" or not brand.get("crm_api_key"):
            skipped += 1
            continue
        try:
            sng_sync_revenue(brand, db)
            synced += 1
        except Exception:
            skipped += 1

    flash(f"SNG revenue synced: {synced} brands, {skipped} skipped", "success" if synced > 0 else "info")
    return redirect(url_for("dashboard"))


@jobs_bp.route("/run-agents", methods=["POST"])
def run_agents_all():
    """Batch run AI agent analysis for all brands with OpenAI keys.
    Can be triggered manually from admin or via Render cron."""
    if "user_id" not in session:
        return redirect(url_for("login"))

    db = current_app.db
    month = request.form.get("month", datetime.now().strftime("%Y-%m"))
    brands = db.get_all_brands()

    from webapp.agent_brains import run_all_agents

    ran = 0
    skipped = 0
    total_findings = 0
    for brand in brands:
        api_key = brand.get("openai_api_key")
        if not api_key:
            skipped += 1
            continue
        try:
            # Clear old findings for the month before re-running
            db.clear_agent_findings(brand["id"], month)
            results = run_all_agents(db, brand, brand["id"], api_key, month=month)
            count = sum(
                len(r.get("findings", [])) for r in results.values() if r
            )
            total_findings += count
            ran += 1
        except Exception:
            skipped += 1

    flash(
        f"Agent run complete: {ran} brands analyzed, {total_findings} findings, {skipped} skipped",
        "success" if ran > 0 else "info",
    )
    return redirect(url_for("dashboard"))


def _verify_cron_secret():
    """Check Bearer token or X-Cron-Secret header against CRON_SECRET env var.
    Returns True if authorized, False otherwise."""
    cron_secret = (
        os.environ.get("CRON_SECRET", "")
        or os.environ.get("SECRET_KEY", "")
        or current_app.config.get("SECRET_KEY", "")
    )
    if not cron_secret:
        return False
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer ") and auth_header[7:] == cron_secret:
        return True
    if request.headers.get("X-Cron-Secret", "") == cron_secret:
        return True
    return False


@jobs_bp.route("/cron/run-agents", methods=["POST"])
def cron_run_agents():
    """Cron-triggered agent runs for all brands. No session needed.
    Auth via CRON_SECRET if present, otherwise falls back to SECRET_KEY.
    Supports Bearer token or X-Cron-Secret header.
    """
    if not _verify_cron_secret():
        return jsonify({"error": "unauthorized"}), 401

    db = current_app.db
    month = datetime.now().strftime("%Y-%m")
    brands = db.get_all_brands()

    from webapp.agent_brains import run_all_agents

    ran = 0
    skipped = 0
    total_findings = 0
    errors = []
    for brand in brands:
        api_key = brand.get("openai_api_key")
        if not api_key:
            skipped += 1
            continue
        try:
            db.clear_agent_findings(brand["id"], month)
            results = run_all_agents(db, brand, brand["id"], api_key, month=month)
            count = sum(
                len(r.get("findings", [])) for r in results.values() if r
            )
            total_findings += count
            ran += 1
        except Exception as e:
            skipped += 1
            errors.append(f"{brand.get('display_name', '?')}: {str(e)[:80]}")

    logger.info("Cron agent run: %d brands, %d findings, %d skipped", ran, total_findings, skipped)
    return jsonify({
        "ok": True,
        "ran": ran,
        "skipped": skipped,
        "total_findings": total_findings,
        "errors": errors[:10],
    })
