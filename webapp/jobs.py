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


@jobs_bp.route("/sync-ghl-revenue", methods=["POST"])
def sync_ghl_revenue_all():
    """Batch sync GHL revenue for all brands with GoHighLevel configured."""
    if "user_id" not in session:
        return redirect(url_for("login"))

    db = current_app.db
    brands = db.get_all_brands()

    from webapp.crm_bridge import pull_gohighlevel_revenue

    synced = 0
    skipped = 0
    target_month = datetime.now().strftime("%Y-%m")
    for brand in brands:
        if brand.get("crm_type") != "gohighlevel" or not brand.get("crm_api_key"):
            skipped += 1
            continue
        try:
            revenue, deal_count, error = pull_gohighlevel_revenue(brand, target_month)
            if not error:
                db.upsert_brand_month_finance(
                    brand["id"], target_month, revenue, deal_count,
                    f"GHL sync: {deal_count} deals",
                )
                synced += 1
            else:
                skipped += 1
        except Exception:
            skipped += 1

    flash(f"GHL revenue synced: {synced} brands, {skipped} skipped", "success" if synced > 0 else "info")
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


@jobs_bp.route("/cron/refresh-dashboards", methods=["POST"])
def cron_refresh_dashboards():
    """Daily cron: refresh dashboard snapshots for all active brands.

    Pulls live data from Google/Meta APIs, assembles the full dashboard
    payload, and stores it so client logins are sub-second.
    Auth via CRON_SECRET (same as cron_run_agents).
    """
    if not _verify_cron_secret():
        return jsonify({"error": "unauthorized"}), 401

    import json as _json
    db = current_app.db
    month = datetime.now().strftime("%Y-%m")
    brand_ids = db.get_stale_dashboard_brands(month, max_age_hours=20)

    from webapp.report_runner import get_analysis_and_suggestions_for_brand
    from webapp.client_portal import _get_campaigns_cached, _assemble_dashboard_payload

    refreshed = 0
    skipped = 0
    errors = []
    for bid in brand_ids:
        brand = db.get_brand(bid)
        if not brand:
            skipped += 1
            continue
        try:
            analysis, suggestions = get_analysis_and_suggestions_for_brand(
                db, brand, month, force_refresh=True
            )
            if not analysis:
                skipped += 1
                continue
            campaigns_data = {}
            try:
                campaigns_data = _get_campaigns_cached(db, brand, month, force_sync=True)
            except Exception:
                pass
            dashboard_data = _assemble_dashboard_payload(
                db, brand, bid, month, analysis, suggestions, campaigns_data
            )
            db.upsert_dashboard_snapshot(
                bid, month,
                _json.dumps(dashboard_data, default=str),
                source="cron",
            )
            refreshed += 1
        except Exception as e:
            skipped += 1
            errors.append(f"{brand.get('display_name', '?')}: {str(e)[:80]}")

    logger.info("Cron dashboard refresh: %d refreshed, %d skipped", refreshed, skipped)
    return jsonify({
        "ok": True,
        "refreshed": refreshed,
        "skipped": skipped,
        "errors": errors[:10],
    })


@jobs_bp.route("/cron/warren-nurture", methods=["POST"])
def cron_warren_nurture():
    """Periodic cron: run Warren nurture follow-ups for stale leads.

    Should run every 1-2 hours. Checks all enabled brands for leads
    that need follow-up and generates contextual messages.
    Auth via CRON_SECRET.
    """
    if not _verify_cron_secret():
        return jsonify({"error": "unauthorized"}), 401

    from webapp.warren_nurture import process_nurture_queue, check_for_ghosted_leads

    db = current_app.db
    sent, skipped = process_nurture_queue(db)

    # Also check for ghosted leads (72 hours no reply)
    ghosted = 0
    try:
        conn = db._conn()
        rows = conn.execute(
            "SELECT id FROM brands WHERE sales_bot_enabled = 1"
        ).fetchall()
        conn.close()
        for row in rows:
            ghosted += check_for_ghosted_leads(db, row["id"])
    except Exception as e:
        logger.warning("Ghosted lead check failed: %s", e)

    logger.info("Warren nurture: %d sent, %d skipped, %d ghosted", sent, skipped, ghosted)
    return jsonify({
        "ok": True,
        "sent": sent,
        "skipped": skipped,
        "ghosted": ghosted,
    })


@jobs_bp.route("/cron/warren-payment-reminders", methods=["POST"])
def cron_warren_payment_reminders():
    """Daily cron: send Warren payment reminders for upcoming SNG bill dates."""
    if not _verify_cron_secret():
        return jsonify({"error": "unauthorized"}), 401

    from webapp.warren_billing import process_payment_reminders

    stats = process_payment_reminders(current_app.db, current_app.config)
    logger.info(
        "Warren payment reminders: %d sent, %d failed, %d skipped across %d brands",
        stats.get("sent", 0),
        stats.get("failed", 0),
        stats.get("skipped", 0),
        stats.get("brands", 0),
    )
    return jsonify({"ok": True, **stats})


@jobs_bp.route("/cron/warren-appointment-reminders", methods=["POST"])
def cron_warren_appointment_reminders():
    """Recurring cron: send day-ahead SNG appointment reminders after each brand's local send time."""
    if not _verify_cron_secret():
        return jsonify({"error": "unauthorized"}), 401

    from webapp.warren_appointments import process_appointment_reminders

    stats = process_appointment_reminders(current_app.db, current_app.config)
    logger.info(
        "Warren appointment reminders: %d sent, %d failed, %d skipped across %d brands",
        stats.get("sent", 0),
        stats.get("failed", 0),
        stats.get("skipped", 0),
        stats.get("brands", 0),
    )
    return jsonify({"ok": True, **stats})


@jobs_bp.route("/cron/warren-crm-event-actions", methods=["POST"])
def cron_warren_crm_event_actions():
    """Recurring cron: process queued CRM event automations for Warren."""
    if not _verify_cron_secret():
        return jsonify({"error": "unauthorized"}), 401

    from webapp.warren_crm_events import process_pending_crm_event_actions

    stats = process_pending_crm_event_actions(current_app.db, current_app.config, limit=250)
    logger.info(
        "Warren CRM event actions: %d sent, %d failed, %d deferred across queued events",
        stats.get("sent", 0),
        stats.get("failed", 0),
        stats.get("deferred", 0),
    )
    return jsonify({"ok": True, **stats})
