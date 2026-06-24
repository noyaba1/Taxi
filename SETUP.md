# Local Environment Setup & Validation (Windows + VS Code)

This document gets Phase 1 running and validated locally. Follow it top to
bottom; each step has a verification command so you never proceed on a broken
foundation.

---

## 1. Install Java 11

Spark runs on the JVM. Spark 3.5 is validated on **Java 11** (Java 8 also works;
Java 17+ needs extra flags — avoid for simplicity).

```powershell
winget install --id EclipseAdoptium.Temurin.11.JDK -e --scope machine
```

**Validate:**
```powershell
java -version
# Expected: openjdk version "11.0.xx" ... Temurin
```

If `java` is not found after install, close and reopen the terminal (PATH is
refreshed only for new shells).

---

## 2. Set JAVA_HOME (Spark needs it explicit on Windows)

```powershell
# Find the install dir (adjust version in the path it prints):
Get-ChildItem "C:\Program Files\Eclipse Adoptium"

# Set it permanently for your user:
[Environment]::SetEnvironmentVariable("JAVA_HOME",
  "C:\Program Files\Eclipse Adoptium\jdk-11.0.xx-hotspot", "User")
```
Reopen the terminal, then **validate:** `echo $env:JAVA_HOME`.

---

## 3. Install Python 3.11

PySpark 3.5 supports Python 3.8–3.12 only. You have 3.13 (unsupported) — install
3.11 alongside it; they coexist.

```powershell
winget install --id Python.Python.3.11 -e
```
**Validate:** `py -3.11 --version`  → `Python 3.11.x`

---

## 4. Create & activate the virtual environment

From the project root:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1      # prompt should now start with (.venv)
python --version                   # MUST say 3.11.x now
```

> If activation is blocked by execution policy:
> `Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned`

---

## 5. Install dependencies & point Spark at this Python

```powershell
pip install -r requirements.txt

# Make Spark workers use THIS interpreter (not system 3.13):
$env:PYSPARK_PYTHON = (Resolve-Path .\.venv\Scripts\python.exe).Path
```
**Validate:** `python -c "import pyspark; print(pyspark.__version__)"` → `3.5.1`

---

## 6. winutils.exe (Hadoop shim for Windows)

Spark's Parquet writer uses Hadoop file APIs that need `winutils.exe` +
`hadoop.dll` on Windows. Without them, writes fail with
`UnsatisfiedLinkError` / `NullPointerException (NativeIO)`.

1. Download `winutils.exe` and `hadoop.dll` for **Hadoop 3.3.x**
   (e.g. github.com/cdarlint/winutils → `hadoop-3.3.x/bin`).
2. Put both in `C:\hadoop\bin`.
3. Set:
   ```powershell
   [Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")
   $env:Path += ";C:\hadoop\bin"
   ```
Reopen the terminal after this.

---

## 7. Validate the whole stack

```powershell
python -m src.validate_env
```
Expected — every line `[OK]` and finally `ALL GOOD - ready for Phase 1.`

---

## 8. Run Phase 1 on the sample

```powershell
python -m src.make_sample 5000     # build a small sample from train.csv
python -m src.clean_data --sample  # parse + clean + write Parquet
```

### Expected output (numbers approximate)
```
[make_sample] full file rows = 1,710,670
[make_sample] wrote ~5,000 trips to ...\data\sample\train_sample.csv_dir
...
==================================================
  total trips       : 5,000
  valid trips       : 4,7xx  (9x.x%)
  dropped (invalid) : 2xx
==================================================
[clean] wrote clean parquet -> ...\trips_clean_sample.parquet
```
A `data/processed/trips_clean_sample.parquet/` folder appears (Spark writes a
directory of part-files, not a single file — this is normal and correct).

---

## VS Code tips

- **Select the venv interpreter:** `Ctrl+Shift+P` → "Python: Select Interpreter"
  → choose `.venv\Scripts\python.exe`. This makes the integrated terminal and
  the debugger use 3.11 automatically.
- **Run as a module, not a file:** always `python -m src.clean_data`, never
  `python src/clean_data.py`. The `-m` form makes `from src import config` work.
- Recommended extensions: *Python*, *Pylance*, *Jupyter*.

---

## Common errors & fixes

| Symptom | Cause | Fix |
|---------|-------|-----|
| `JAVA_HOME is not set` / `java not recognized` | Java missing or PATH stale | Install Temurin 11, set `JAVA_HOME`, reopen terminal |
| `Py4JJavaError` right at `getOrCreate()`, mentions class version | Java 17+ or Python 3.13 | Use Java 11 + Python 3.11 venv |
| `UnsatisfiedLinkError` / `NativeIO` on write | winutils missing | Step 6 |
| `python worker ... version mismatch` | workers using system 3.13 | set `PYSPARK_PYTHON` to the venv python |
| `from src import config` → ModuleNotFoundError | ran the file directly | run with `python -m src.<module>` from project root |
| Writes are extremely slow / OneDrive busy | project on OneDrive | move to `C:\dev\taxi` |
| `Port already in use` Spark UI warning | a previous Spark session lingering | harmless; or close other Spark apps |
| OutOfMemory on `--full` | 1.9 GB on a small laptop | lower `spark.driver.memory`, or just rely on the sample for dev |
