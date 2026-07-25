"""
cli.py
======
Shared entry-point plumbing: scale flags, logging, and stage timing.

Every stage used to hand-roll the same mutually-exclusive `--sample/--full`
argparse group and print progress with bare `print`. Three things were missing
as a result:

  * a third scale ("mid") for a realistic local shuffle test before paying GCP;
  * structured logs (a failed cloud run left nothing machine-readable behind);
  * per-stage runtime AND work-volume, which the assignment explicitly asks for
    ("זמן ריצה" and "מספר הפעולות החישוביות הנדרשות") for EVERY stage. Only
    two of twelve stages reported either.

`stage()` is a context manager that solves the last two at once: it logs start
and finish, and appends one row to outputs/statistics/timings.jsonl that the
cross-method comparison later renders as a table.
"""
from __future__ import annotations

import argparse
import contextlib
import logging
import os
import resource
import sys
import time

from src import config

_LOG_FORMAT = "%(asctime)s %(levelname)-5s %(name)s | %(message)s"


# ------------------------------------------------------------------ arguments
def scale_parser(description: str = "") -> argparse.ArgumentParser:
    """Standard parser with the three dataset scales."""
    ap = argparse.ArgumentParser(description=description)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--sample", action="store_true", help="small sample (fast)")
    g.add_argument("--mid", action="store_true", help="mid-scale local run")
    g.add_argument("--full", action="store_true", help="the whole dataset")
    return ap


def scale_of(args) -> str:
    if getattr(args, "full", False):
        return "full"
    if getattr(args, "mid", False):
        return "mid"
    return "sample"


# -------------------------------------------------------------------- logging
def setup_logging(name: str) -> logging.Logger:
    """
    Console logging that reads like the old prints but carries level/timestamp.
    Honours LOG_LEVEL so a cloud run can be turned up without a code change.
    """
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    root = logging.getLogger()
    if not root.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(_LOG_FORMAT, "%H:%M:%S"))
        root.addHandler(handler)
    root.setLevel(level)
    # py4j narrates every JVM call at INFO; that buries our own output and is
    # never what anyone is debugging.
    for noisy in ("py4j", "py4j.clientserver", "py4j.java_gateway"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    return logging.getLogger(name)


def _peak_rss_mb() -> float:
    """Driver peak RSS. Linux reports KiB, macOS reports bytes."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / (1024 * 1024) if sys.platform == "darwin" else peak / 1024


@contextlib.contextmanager
def stage(name: str, scale: str, log: logging.Logger | None = None):
    """
    Time a stage and record its cost.

    Yields a dict the caller fills with work-volume counters, e.g.
        with stage("m5_exact", scale) as st:
            st["trips"] = n_trips
            st["shuffle_records"] = n_windows

    On exit the merged record lands in outputs/statistics/timings.jsonl. Failures
    are recorded too (ok=False) -- a stage that died after 40 minutes is exactly
    the datapoint you want when the cluster bill arrives.
    """
    log = log or setup_logging(name)
    metrics: dict = {}
    t0 = time.time()
    ok = True
    log.info("START %s (scale=%s)", name, scale)
    try:
        yield metrics
    except BaseException:
        ok = False
        raise
    finally:
        elapsed = time.time() - t0
        record = {
            "stage": name,
            "scale": scale,
            "ok": ok,
            "wall_s": round(elapsed, 2),
            "peak_rss_mb": round(_peak_rss_mb(), 1),
            **metrics,
        }
        log.info("END   %s in %.1fs (%s)", name, elapsed, "ok" if ok else "FAILED")
        try:
            from src import storage

            storage.append_jsonl(
                storage.out_path("statistics", "timings.jsonl"), record)
        except Exception as exc:  # noqa: BLE001 - telemetry must never fail a run
            log.warning("could not record timings: %s", exc)


def paths_for(args, resolution: int | None = None) -> dict:
    """Convenience: parsed args -> the dataset path bundle for that scale."""
    return config.dataset_paths(scale_of(args), resolution)
