"""Background runner for scheduled local rank heatmap scans."""

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone


log = logging.getLogger(__name__)
_runner_started = False


def _env_flag(name, default=False):
    value = str(os.environ.get(name, "")).strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on"}


def _utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def process_due_heatmap_schedules(app, limit=5):
    db = app.db
    due_schedules = db.get_due_heatmap_schedules(now_iso=_utc_now(), limit=limit)
    stats = {"checked": len(due_schedules), "complete": 0, "failed": 0, "skipped": 0}
    if not due_schedules:
        return stats

    from webapp.client_portal import (
        _finalize_heatmap_scan,
        _next_heatmap_run_at,
        _prepare_heatmap_scan_kwargs,
    )

    for schedule in due_schedules:
        schedule_id = schedule.get("id")
        brand_id = schedule.get("brand_id")
        next_run_at = _next_heatmap_run_at(
            schedule.get("day_of_week"),
            schedule.get("run_time"),
            schedule.get("timezone"),
        )
        db.update_heatmap_schedule_run(
            schedule_id,
            next_run_at=next_run_at,
            last_run_at=_utc_now(),
            last_status="running",
            last_error="",
        )

        scan_id = None
        try:
            brand = db.get_brand(brand_id)
            if not brand:
                stats["skipped"] += 1
                db.update_heatmap_schedule_run(
                    schedule_id,
                    last_status="skipped",
                    last_error="Brand not found",
                )
                continue

            scan_kwargs, error = _prepare_heatmap_scan_kwargs(
                db,
                brand,
                keyword=schedule.get("keyword"),
                radius_miles=schedule.get("radius_miles"),
                grid_size=schedule.get("grid_size"),
                center_lat=schedule.get("center_lat"),
                center_lng=schedule.get("center_lng"),
                provider_preference=schedule.get("provider_preference") or "google",
            )
            if error:
                raise RuntimeError(error)

            scan_id = db.save_heatmap_scan(
                brand_id,
                scan_kwargs["keyword"],
                scan_kwargs["grid_size"],
                scan_kwargs["radius_miles"],
                scan_kwargs["center_lat"],
                scan_kwargs["center_lng"],
                "[]",
                0,
                status="pending",
                debug_json=json.dumps({"status": "scheduled", "schedule_id": schedule_id}),
            )
            payload = _finalize_heatmap_scan(**scan_kwargs)
            db.update_heatmap_scan(
                scan_id,
                brand_id,
                results_json=json.dumps(payload["results"]),
                avg_rank=payload["avg_rank"],
                status="complete",
                error_message="",
                debug_json=json.dumps(payload.get("debug") or {}),
            )
            db.update_heatmap_schedule_run(
                schedule_id,
                last_scan_id=scan_id,
                last_status="complete",
                last_error="",
            )
            stats["complete"] += 1
        except Exception as exc:
            error = str(exc)[:2000]
            if scan_id:
                db.update_heatmap_scan(
                    scan_id,
                    brand_id,
                    status="failed",
                    error_message=error,
                    debug_json=json.dumps({"error": error, "schedule_id": schedule_id}),
                )
            db.update_heatmap_schedule_run(
                schedule_id,
                last_scan_id=scan_id,
                last_status="failed",
                last_error=error,
            )
            stats["failed"] += 1
            log.exception("[HeatmapRunner] Scheduled heatmap scan failed for schedule %s", schedule_id)

    return stats


def start_background_heatmap_runner(app):
    global _runner_started
    if _runner_started:
        return False
    if _env_flag("DISABLE_BACKGROUND_HEATMAP_RUNNER"):
        log.info("[HeatmapRunner] Background runner disabled by env flag")
        return False

    interval_seconds = max(300, int(os.environ.get("HEATMAP_RUNNER_INTERVAL_SECONDS") or 900))
    startup_delay_seconds = max(0, int(os.environ.get("HEATMAP_RUNNER_STARTUP_DELAY_SECONDS") or 90))

    def _loop():
        if startup_delay_seconds:
            time.sleep(startup_delay_seconds)
        while True:
            try:
                with app.app_context():
                    stats = process_due_heatmap_schedules(app)
                    if stats.get("checked"):
                        log.info(
                            "[HeatmapRunner] Due-scan cycle: %d complete, %d failed, %d skipped",
                            stats.get("complete", 0),
                            stats.get("failed", 0),
                            stats.get("skipped", 0),
                        )
            except Exception:
                log.exception("[HeatmapRunner] Due-scan cycle failed")
            time.sleep(interval_seconds)

    thread = threading.Thread(target=_loop, name="heatmap-runner", daemon=True)
    thread.start()
    _runner_started = True
    log.info(
        "[HeatmapRunner] Started background heatmap runner (interval=%ss, startup_delay=%ss)",
        interval_seconds,
        startup_delay_seconds,
    )
    return True
