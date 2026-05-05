"""Background runner for social posts WARREN queues instead of platform-scheduling."""

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


def start_background_social_post_runner(app):
    global _runner_started
    if _runner_started:
        return False
    if _env_flag("DISABLE_BACKGROUND_SOCIAL_POST_RUNNER"):
        log.info("[SocialPostRunner] Background runner disabled by env flag")
        return False

    interval_seconds = max(60, int(os.environ.get("SOCIAL_POST_RUNNER_INTERVAL_SECONDS") or 300))
    startup_delay_seconds = max(0, int(os.environ.get("SOCIAL_POST_RUNNER_STARTUP_DELAY_SECONDS") or 60))

    def _loop():
        if startup_delay_seconds:
            time.sleep(startup_delay_seconds)
        while True:
            try:
                with app.app_context():
                    from webapp.social_publisher import process_due_social_posts

                    stats = process_due_social_posts(app.db)
                    if stats.get("checked"):
                        log.info(
                            "[SocialPostRunner] Due-post cycle: %d published, %d failed, %d skipped",
                            stats.get("published", 0),
                            stats.get("failed", 0),
                            stats.get("skipped", 0),
                        )
            except Exception:
                log.exception("[SocialPostRunner] Due-post cycle failed")
            time.sleep(interval_seconds)

    thread = threading.Thread(target=_loop, name="social-post-runner", daemon=True)
    thread.start()
    _runner_started = True
    log.info(
        "[SocialPostRunner] Started background social post runner (interval=%ss, startup_delay=%ss)",
        interval_seconds,
        startup_delay_seconds,
    )
    return True
