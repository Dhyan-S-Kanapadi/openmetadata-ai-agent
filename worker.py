"""
Long-running DataSentinel worker.

Run this process when you want DataSentinel to wake up automatically and keep
monitoring OpenMetadata.
"""

from __future__ import annotations

import argparse
import logging
import signal
import time

from monitor import start_scheduler


LOGGER = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the DataSentinel worker.")
    parser.add_argument(
        "--interval-minutes",
        type=int,
        default=1,
        help="How often DataSentinel should run a monitoring cycle.",
    )
    parser.add_argument(
        "--no-immediate-run",
        action="store_true",
        help="Start the scheduler without running an immediate first scan.",
    )
    parser.add_argument(
        "--no-auto-trigger",
        action="store_true",
        help="Disable autonomous failed-pipeline trigger actions.",
    )
    parser.add_argument(
        "--no-notify",
        action="store_true",
        help="Disable customer notifications.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
    stop_requested = False

    def _request_stop(signum, _frame):
        nonlocal stop_requested
        LOGGER.info("Received signal %s. Stopping DataSentinel worker...", signum)
        stop_requested = True

    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)

    scheduler = start_scheduler(
        interval_minutes=args.interval_minutes,
        trigger_failed_pipelines=not args.no_auto_trigger,
        notify_customer=not args.no_notify,
        run_immediately=not args.no_immediate_run,
    )

    LOGGER.info(
        "DataSentinel worker running every %s minute(s). Press Ctrl+C to stop.",
        args.interval_minutes,
    )

    try:
        while not stop_requested:
            time.sleep(1)
    finally:
        scheduler.shutdown(wait=False)
        LOGGER.info("DataSentinel worker stopped.")


if __name__ == "__main__":
    main()
