"""Background appointment reminder runner for Render web instances.

This acts as a backup to external cron so day-ahead reminder checks still run
from the web service even if the platform cron misses a cycle.
"""

import logging
import os
import threading
import time


log = logging.getLogger(__name__)

_runner_started = False


def _env_flag(name, default=False):
    value = str(os.environ.get(name, "")).strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on"}


def start_background_appointment_runner(app):
    """Start a daemon thread that periodically runs appointment reminders.

    This is intentionally limited to deployed web instances so tests and
    one-off local scripts do not spawn extra workers.
    """
    global _runner_started

    if _runner_started:
        return False
    if _env_flag("DISABLE_BACKGROUND_APPOINTMENT_RUNNER"):
        log.info("[AppointmentRunner] Background runner disabled by env flag")
        return False

    interval_seconds = max(60, int(os.environ.get("APPOINTMENT_RUNNER_INTERVAL_SECONDS") or 300))
    startup_delay_seconds = max(0, int(os.environ.get("APPOINTMENT_RUNNER_STARTUP_DELAY_SECONDS") or 45))

    def _loop():
        if startup_delay_seconds:
            time.sleep(startup_delay_seconds)

        while True:
            try:
                with app.app_context():
                    from webapp.warren_appointments import process_appointment_reminders

                    stats = process_appointment_reminders(app.db, app.config)
                    log.info(
                        "[AppointmentRunner] Reminder cycle: %d sent, %d failed, %d skipped across %d brands",
                        stats.get("sent", 0),
                        stats.get("failed", 0),
                        stats.get("skipped", 0),
                        stats.get("brands", 0),
                    )
            except Exception:
                log.exception("[AppointmentRunner] Background reminder cycle failed")
            time.sleep(interval_seconds)

    thread = threading.Thread(
        target=_loop,
        name="appointment-reminder-runner",
        daemon=True,
    )
    thread.start()
    _runner_started = True
    log.info(
        "[AppointmentRunner] Started background reminder runner (interval=%ss, startup_delay=%ss)",
        interval_seconds,
        startup_delay_seconds,
    )
    return True
