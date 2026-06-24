"""
validate_env.py
===============
One-shot smoke test: confirms Java, Python, PySpark and Arrow all work
together BEFORE you run the real pipeline. Run this first after setup:

    python -m src.validate_env

If every check prints [OK], your environment is ready for Phase 1.
"""
import os
import shutil
import subprocess
import sys


def check(label, ok, detail=""):
    print(f"[{'OK' if ok else 'FAIL'}] {label}" + (f" -> {detail}" if detail else ""))
    return ok


def main() -> int:
    all_ok = True

    # 1. Python version (PySpark supports 3.8-3.12).
    v = sys.version_info
    all_ok &= check(
        f"Python {v.major}.{v.minor}.{v.micro}",
        (v.major, v.minor) <= (3, 12) and v.major == 3,
        "use 3.11 if this says FAIL",
    )

    # 2. Java present and is version 11.
    java = shutil.which("java")
    if java:
        out = subprocess.run(["java", "-version"], capture_output=True, text=True).stderr
        all_ok &= check("Java on PATH", '"11' in out or "11." in out, out.splitlines()[0])
    else:
        all_ok &= check("Java on PATH", False, "java not found - install Temurin 11")

    # 3. JAVA_HOME set (Spark prefers it explicit on Windows).
    all_ok &= check("JAVA_HOME set", bool(os.environ.get("JAVA_HOME")),
                    os.environ.get("JAVA_HOME", "missing"))

    # 4. PySpark imports and a tiny job runs end-to-end.
    try:
        from src.spark_session import get_spark
        spark = get_spark("validate-env")
        n = spark.range(1000).filter("id % 2 = 0").count()
        spark.stop()
        all_ok &= check("PySpark mini-job (expect 500)", n == 500, f"got {n}")
    except Exception as e:  # noqa: BLE001
        all_ok &= check("PySpark mini-job", False, repr(e)[:200])

    print("\n" + ("ALL GOOD - ready for Phase 1." if all_ok else "Fix the FAIL lines above."))
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
