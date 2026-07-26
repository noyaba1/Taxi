"""
build_devdoc.py  --  the developer handbook, as a .docx
=======================================================
Writes `build/docs/Porto_Taxi_Dev_Guide.docx`: how this project runs, why it is
built the way it is, and every trap that cost real time.

Audience is the developer (you, in six months), not the grader. It assumes you
will need to re-run something, debug something, or explain something, and gives
the reason behind each rule so you can tell when it stops applying.
"""
import pathlib

from docx import Document
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Inches, Pt, RGBColor

OUT = pathlib.Path("build/docs/Porto_Taxi_Dev_Guide.docx")
OUT.parent.mkdir(parents=True, exist_ok=True)

INK = RGBColor(0x1A, 0x1A, 0x1A)
MUTED = RGBColor(0x5A, 0x5A, 0x5A)
ACCENT = RGBColor(0x1C, 0x5C, 0xAB)
WARN = RGBColor(0xB0, 0x3A, 0x1A)
GOOD = RGBColor(0x0A, 0x6B, 0x2A)
CODE_BG = "F2F3F5"
NOTE_BG = "EEF4FC"
WARN_BG = "FDF0EA"

doc = Document()


def _setup():
    st = doc.styles["Normal"]
    st.font.name = "Calibri"
    st.font.size = Pt(10.5)
    st.paragraph_format.space_after = Pt(7)
    st.paragraph_format.line_spacing = 1.18
    for s in doc.sections:
        s.left_margin = s.right_margin = Inches(0.95)
        s.top_margin = s.bottom_margin = Inches(0.85)
    for name, size, color, before in (("Heading 1", 19, ACCENT, 22),
                                      ("Heading 2", 14, INK, 16),
                                      ("Heading 3", 11.5, INK, 12)):
        h = doc.styles[name]
        h.font.name = "Calibri"
        h.font.size = Pt(size)
        h.font.bold = True
        h.font.color.rgb = color
        h.paragraph_format.space_before = Pt(before)
        h.paragraph_format.space_after = Pt(5)


def shade(p, hexfill):
    el = OxmlElement("w:shd")
    el.set(qn("w:val"), "clear")
    el.set(qn("w:fill"), hexfill)
    p._p.get_or_add_pPr().append(el)


def para(text="", size=10.5, bold=False, color=INK, italic=False,
         space_after=7, align=None):
    p = doc.add_paragraph()
    p.paragraph_format.space_after = Pt(space_after)
    if align:
        p.alignment = align
    r = p.add_run(text)
    r.font.size, r.font.bold, r.font.italic = Pt(size), bold, italic
    r.font.color.rgb = color
    return p


def rich(*parts, space_after=7):
    """rich(("plain ", {}), ("bold", {'b':True}), ...)"""
    p = doc.add_paragraph()
    p.paragraph_format.space_after = Pt(space_after)
    for text, kw in parts:
        r = p.add_run(text)
        r.font.size = Pt(kw.get("size", 10.5))
        r.font.bold = kw.get("b", False)
        r.font.italic = kw.get("i", False)
        r.font.name = kw.get("font", "Calibri")
        if kw.get("font") == "Consolas":
            r.font.size = Pt(kw.get("size", 9.5))
        r.font.color.rgb = kw.get("c", INK)
    return p


def code(lines, bg=CODE_BG):
    if isinstance(lines, str):
        lines = lines.strip("\n").split("\n")
    for i, ln in enumerate(lines):
        p = doc.add_paragraph()
        p.paragraph_format.space_after = Pt(1 if i < len(lines) - 1 else 9)
        p.paragraph_format.space_before = Pt(6 if i == 0 else 0)
        p.paragraph_format.left_indent = Inches(0.16)
        r = p.add_run(ln or " ")
        r.font.name, r.font.size = "Consolas", Pt(9)
        r.font.color.rgb = RGBColor(0x10, 0x24, 0x40)
        shade(p, bg)


def callout(title, text, bg=NOTE_BG, tcolor=ACCENT):
    p = doc.add_paragraph()
    p.paragraph_format.space_before = Pt(8)
    p.paragraph_format.space_after = Pt(0)
    p.paragraph_format.left_indent = Inches(0.12)
    r = p.add_run(title)
    r.font.size, r.font.bold, r.font.color.rgb = Pt(9.5), True, tcolor
    shade(p, bg)
    p2 = doc.add_paragraph()
    p2.paragraph_format.space_after = Pt(9)
    p2.paragraph_format.left_indent = Inches(0.12)
    r2 = p2.add_run(text)
    r2.font.size, r2.font.color.rgb = Pt(10), INK
    shade(p2, bg)


def bullets(items, style="List Bullet"):
    for it in items:
        if isinstance(it, tuple):
            p = doc.add_paragraph(style=style)
            p.paragraph_format.space_after = Pt(3)
            r = p.add_run(it[0]); r.font.bold = True; r.font.size = Pt(10.5)
            r2 = p.add_run(" — " + it[1]); r2.font.size = Pt(10.5)
        else:
            p = doc.add_paragraph(it, style=style)
            p.paragraph_format.space_after = Pt(3)
            for r in p.runs:
                r.font.size = Pt(10.5)


def table(headers, rows, widths=None, size=9.5):
    t = doc.add_table(rows=1, cols=len(headers))
    t.style = "Light Grid Accent 1"
    t.alignment = WD_TABLE_ALIGNMENT.LEFT
    for i, h in enumerate(headers):
        c = t.rows[0].cells[i]
        c.text = ""
        r = c.paragraphs[0].add_run(str(h))
        r.font.bold, r.font.size = True, Pt(size)
    for row in rows:
        cells = t.add_row().cells
        for i, v in enumerate(row):
            cells[i].text = ""
            p = cells[i].paragraphs[0]
            p.paragraph_format.space_after = Pt(2)
            r = p.add_run(str(v))
            r.font.size = Pt(size)
            if i == 0:
                r.font.bold = True
    if widths:
        for r_ in t.rows:
            for i, w in enumerate(widths):
                r_.cells[i].width = Inches(w)
    doc.add_paragraph().paragraph_format.space_after = Pt(3)
    return t


# ===========================================================================
_setup()

# ---------------------------------------------------------------- cover ----
para("PORTO TAXI TRAJECTORY ANALYSIS", 11, True, ACCENT, space_after=2)
para("Developer Guide", 30, True, INK, space_after=4)
para("How the project runs, why it is built this way, and every trap that cost "
     "real time.", 12, color=MUTED, space_after=16)
table(["", ""],
      [["Dataset", "1,710,670 trips · 442 taxis · Porto 2013–2014 · GPS every 15 s"],
       ["Engine", "PySpark 3.5.1 on Java 17 / Python 3.11"],
       ["Cloud", "GCP DataProc, 1 master + 5 workers (n2-standard-4), europe-west1"],
       ["Repo", "branch Noya · github.com/noyaba1/Taxi"],
       ["Deliverable", "top-100 popular sub-routes at ≥1/3/5/10/20/40 km, four methods"]],
      widths=[1.3, 5.4])

doc.add_page_break()

# =================================================================== 1 =====
doc.add_heading("1. What the project actually does", level=1)
para("Every stage of this pipeline exists to support one idea, and if you "
     "remember nothing else, remember this:")
callout("THE ONE IDEA",
        "Snap each GPS point to an H3 hexagon (resolution 9, ~174 m edge) and "
        "collapse consecutive repeats. A trip becomes a STRING over a cell "
        "alphabet. A sub-route is then a contiguous SUBSTRING, 'popular' is "
        "substring SUPPORT, and 'long' is ground length. The whole problem "
        "becomes classical string processing, and one representation feeds all "
        "four mining methods — which is the only reason cross-method comparison "
        "means anything.")
code("8939220f027ffff > 8939220f023ffff > 8939220f02fffff > 8939220f02bffff > …")
rich(("Support is counted two ways on purpose: ", {}),
     ("distinct trips", {"b": True}),
     (" and ", {}), ("distinct taxis", {"b": True}),
     (". With only 442 vehicles over a year, a corridor driven 200 times by one "
      "commuter looks identical to a genuinely public route unless you separate "
      "them. The deliverable carries both columns.", {}))

# =================================================================== 2 =====
doc.add_heading("2. Environment — the two things that break Spark silently", level=1)
para("Both of these fail in ways that look like a bug in your code, not a bug in "
     "your setup. That is why there is a gate.")
table(["", "Required", "Symptom when wrong"],
      [["Java", "8, 11 or 17", "opaque JVM / reflection errors deep in the stack"],
       ["Python", "3.8–3.12 (use 3.11)", "PYTHON_VERSION_MISMATCH, and only once a "
                                         "pandas_udf actually executes"]],
      widths=[0.8, 1.7, 4.2])
rich(("This machine ships Java 26 and Python 3.13, so both defaults are wrong. ",
      {}),
     ("spark_session.py", {"font": "Consolas"}),
     (" locates a supported JDK itself and pins ", {}),
     ("PYSPARK_PYTHON", {"font": "Consolas"}),
     (" to the running interpreter — you should not need to export anything.", {}))
code("""
.venv/bin/python -m src.validate_env      # the gate: run this first, always
""")
para("It checks the Python version, resolves a supported JDK, imports every "
     "third-party module the pipeline needs, confirms the raw data is reachable, "
     "and runs a real Spark job including an Arrow pandas_udf round-trip. If "
     "Arrow is broken, encoding and feature stages are useless, so it is tested "
     "before anything expensive starts.")
callout("ALWAYS USE .venv/bin/python",
        "Not `python`, not `python3`. The system interpreter is 3.13 and will "
        "fail inside a pandas_udf, which is a long way from where you started.",
        WARN_BG, WARN)

# =================================================================== 3 =====
doc.add_heading("3. Running it locally", level=1)
doc.add_heading("3.1 The scales", level=2)
para("Everything is scale-parameterised. Bigger scales are for confidence, not "
     "for different answers.")
table(["Flag", "Trips", "Use it for"],
      [["--sample", "5,000", "correctness; seconds per stage"],
       ["--mid", "200,000", "does it still hold when the data grows?"],
       ["--scale s400k", "400,000", "scaling-law data point"],
       ["--scale s800k", "800,000", "scaling-law data point"],
       ["--full", "1,710,670", "the graded run (~33 min locally)"]],
      widths=[1.5, 1.2, 4.0])
code("""
# build a scale first (except --full, which reads the raw CSV directly)
.venv/bin/python -m src.make_sample --sample
.venv/bin/python -m src.make_sample --scale s400k

# run the pipeline + all verifiers
.venv/bin/python -m src.run_pipeline --sample --verify

# the full run needs more room than the defaults
SPARK_SHUFFLE_PARTS=200 SPARK_DRIVER_MEM=10g \\
  .venv/bin/python -m src.run_pipeline --full

# tests
.venv/bin/python -m pytest tests/ -q          # 56 tests
""")

doc.add_heading("3.2 What the pipeline runs, and at which scale", level=2)
para("Stages are gated per scale in run_pipeline.STAGES. CI asserts the cloud "
     "script submits every full-scale stage, so the two cannot drift apart.")
table(["Stage", "Module", "Scales"],
      [["Phase 1 clean", "clean_data", "all"],
       ["Phase 2 features", "feature_engineering", "all"],
       ["Phase 2 statistics", "summarize_features", "all"],
       ["Phase 4 H3 encoding", "spatial_encoding", "all"],
       ["M5 exact mining (baseline)", "route_mining_exact", "sample"],
       ["M6 closed sub-routes", "route_mining_closed", "sample"],
       ["M7 approx vs exact", "route_mining_approx", "sample"],
       ["M7 approx (sketches only)", "route_mining_approx --approx-only", "mid, full"],
       ["M12 suffix array (D)", "route_mining_suffix_array", "all"],
       ["M8 maximal-frequent (B)", "route_mining_maximal", "sample"],
       ["M9 clustering (A)", "route_mining_clustering", "all"],
       ["M10 transition graph (C)", "route_mining_graph", "all"],
       ["M11 anomalies", "anomaly_analysis", "all"],
       ["M16 method comparison", "evaluation", "all"],
       ["M17 held-out validation", "validate_holdout", "all"],
       ["M18 temporal analysis", "temporal_analysis", "all"],
       ["M15 visualization (map)", "visualization", "all"]],
      widths=[2.2, 2.9, 1.0])
rich(("Why M5/M6/M8 are sample-only: ", {"b": True}),
     ("they share one O(n²) window table. Measured at 200k, M5 OOMs and M8 "
      "spilled 21 GB without finishing. Method D produces the same "
      "maximal-frequent answer in a single pass with zero support "
      "disagreements — so B is demonstrated, not scaled.", {}))

doc.add_heading("3.3 The four methods", level=2)
table(["", "Module", "Approach"],
      [["A", "route_mining_clustering", "MinHash-LSH → star clustering → longest "
                                        "run shared by ≥60% of members"],
       ["B", "route_mining_maximal", "n-gram support table → maximal-frequent "
                                     "(sample scale only)"],
       ["C", "route_mining_graph", "cell-transition graph → PageRank zones + "
                                   "dominant-flow heavy paths"],
       ["D", "route_mining_suffix_array", "generalised suffix array + LCP "
                                          "intervals. EXACT and scalable — "
                                          "carries the deliverable"]],
      widths=[0.4, 2.2, 3.9])
callout("SUPPORT IS NOT COMPARABLE ACROSS METHODS",
        "B and D report only MAXIMAL sub-routes — a route is emitted only if no "
        "one-cell extension is itself frequent. A and C have no such constraint, "
        "so they can report a short, very common PREFIX that D suppresses as "
        "redundant. Measured at ≥1 km: A's top route is in 79,952 trips and its "
        "best extension is still in 64,936 (not maximal); D reports 14,330 for a "
        "route that cannot be extended. Both are exact counts of different "
        "things. Read the column down, never across.", WARN_BG, WARN)

doc.add_page_break()

# =================================================================== 4 =====
doc.add_heading("4. Conventions you must not break", level=1)
doc.add_heading("4.1 Paths come from one function", level=2)
rich(("config.dataset_paths(scale)", {"font": "Consolas"}),
     (" is the ONLY place a parquet filename is built. Six modules once "
      "reconstructed them independently and the mismatch shipped. Do not "
      "reintroduce it.", {}))

doc.add_heading("4.2 All output I/O goes through src/storage.py", level=2)
rich(("Never use ", {}), ("open()", {"font": "Consolas"}), (" or ", {}),
     ("os.makedirs", {"font": "Consolas"}),
     (" for anything under OUTPUT_BASE. On a ", {}),
     ("gs://", {"font": "Consolas"}),
     (" base those silently create a local directory literally named ", {}),
     ("gs:", {"font": "Consolas"}),
     (" and the cloud run loses its outputs. ", {}),
     ("storage", {"font": "Consolas"}),
     (" dispatches to the Hadoop FileSystem for any path with a URI scheme.", {}))
callout("A TRAP INSIDE THE TRAP",
        "storage.read_text originally used readFully() into a py4j array. The "
        "JVM fills its own array but py4j does not reflect the write back, so "
        "every report was written correctly and read back as a correctly-sized "
        "buffer of NUL bytes. It now decodes JVM-side via commons-io IOUtils. "
        "tests/test_storage.py exercises this against file://, which is a real "
        "Hadoop FileSystem and takes the identical code path to gs://.")

doc.add_heading("4.3 Access Spark rows by column name", level=2)
rich(("Never ", {}), ("r[0]", {"font": "Consolas"}),
     (". Adding TAXI_ID to a frame once turned a positional ", {}),
     ("r[0]", {"font": "Consolas"}),
     (" into an int and broke Method C at full scale.", {}))

doc.add_heading("4.4 Reports derive their conclusions", level=2)
para("evaluation.py used to end with a hard-coded paragraph asserting which "
     "methods agreed. Re-running on different data produced a report that "
     "contradicted its own tables. Every conclusion is now computed from the "
     "matrix directly above it. This paid off in a way that is easy to miss: a "
     "timezone bug moved mean temporal overlap 0.78 → 0.81, and the derived "
     "verdict flipped from 'a fair summary with a real caveat' to 'the corridors "
     "are structural'. A hand-written paragraph would have stayed wrong.")

doc.add_heading("4.5 Verifiers must exit non-zero", level=2)
para("Eight of nine verifiers once printed VERIFICATION FAILED and exited 0, so "
     "--verify reported OK regardless. A check that cannot fail is "
     "documentation, not verification.")

doc.add_heading("4.6 The two guards", level=2)
rich(("Guard 1 — corrupt trajectories are EXCLUDED, not flagged. ", {"b": True}),
     ("Sub-route length is measured between cell centres, so a window spanning a "
      "GPS dropout reports the width of the gap as route length. Measured "
      "without the guard: worst window 53.6 km per cell-hop (45× the physical "
      "bound) and 563 windows ≥10 km built across gaps, one claiming 59.8 km "
      "from 19 cells — straight into the graded lists. Two layers: "
      "spatial_encoding drops is_anomalous trips, and every miner splits "
      "trajectories at any hop above config.max_cell_hop_km() (~1.18 km at res "
      "9). That limit is DERIVED — retained-speed bound plus cell quantisation — "
      "not tuned until the output looked nice.", {}))
rich(("Guard 2 — support floors are ABSOLUTE, not percentages. ", {"b": True}),
     ("A percentage floor is scale-dependent the wrong way: 0.01% is 2 trips on "
      "the sample but 171 at 1.71M, so the long length bands empty out AS THE "
      "DATA GROWS. SUPPORT_MIN_SUP_GRID is absolute; the equivalent X% is "
      "reported alongside.", {}))

# =================================================================== 5 =====
doc.add_heading("5. Verification — how you know it is right", level=1)
para("Three independent layers, none of which trusts the others.")
table(["Layer", "What it does", "Command"],
      [["56 unit tests", "pure functions: geometry, LCP intervals, gap splitting, "
                         "storage round-trips over a URI scheme",
        "pytest tests/ -q"],
       ["9 verifiers", "each recounts a stage's output by a DIFFERENT route than "
                       "the mining used — e.g. brute-force substring containment "
                       "vs the Aho-Corasick automaton",
        "run_pipeline --verify"],
       ["cloud verifier", "compares a cluster run against the local baseline in "
                          "tiers, demanding equality only where the algorithm is "
                          "deterministic",
        "verify_cloud_run"]],
      widths=[1.3, 3.6, 1.7])
rich(("The load-bearing check is Method D. ", {"b": True}),
     ("It has no sampling, no seeds and no hash-order dependence, so its output "
      "is a function of the input alone. If a cluster produces different "
      "corridors, the cluster read different data. That is why the cloud "
      "verifier demands EXACT equality there, CLOSE for Method C, and only "
      "sanity for Method A and the sketches — A samples trips with unseeded "
      "MinHash-LSH and sketch merge order is partition-dependent, so demanding "
      "equality there would manufacture false alarms.", {}))

doc.add_page_break()

# =================================================================== 6 =====
doc.add_heading("6. Running it on GCP", level=1)
doc.add_heading("6.1 One-time setup", level=2)
code("""
gcloud auth login
gcloud config set project finalproj-noyabayazi

# a bucket in the same region as the cluster
gsutil mb -l europe-west1 gs://taxi-project-noyabayazi

# upload the raw inputs once
gsutil -m cp train.csv gs://taxi-project-noyabayazi/taxi/raw/train.csv
gsutil -m cp test.csv  gs://taxi-project-noyabayazi/taxi/raw/test.csv

# APIs
gcloud services enable dataproc.googleapis.com storage.googleapis.com

# IAM: DataProc VMs run as the DEFAULT COMPUTE service account, which in a
# fresh project has NO storage role at all.
PROJNUM=$(gcloud projects describe finalproj-noyabayazi --format='value(projectNumber)')
gcloud projects add-iam-policy-binding finalproj-noyabayazi \\
  --member="serviceAccount:${PROJNUM}-compute@developer.gserviceaccount.com" \\
  --role="roles/storage.admin"
""")

doc.add_heading("6.2 Running", level=2)
code("""
# 1. dry run — checks every precondition, creates and bills nothing
PROJECT=finalproj-noyabayazi BUCKET=gs://taxi-project-noyabayazi PREFIX=taxi \\
  DRY_RUN=1 bash scripts/dataproc_submit.sh

# 2. cheap rehearsal on 5,000 trips
PROJECT=... BUCKET=... PREFIX=taxi SCALE=--sample WORKERS=2 \\
  bash scripts/dataproc_submit.sh

# 3. the real run (defaults: SCALE=--full, WORKERS=5)
PROJECT=... BUCKET=... PREFIX=taxi bash scripts/dataproc_submit.sh

# re-run ONLY some stages against parquet already in the bucket
ONLY="temporal_analysis.py evaluation.py" bash scripts/dataproc_submit.sh
""")
rich(("Wrap the full run in ", {}), ("caffeinate -i", {"font": "Consolas"}),
     (". It takes about an hour; if the Mac sleeps the script dies, and a killed "
      "process may not fire the cleanup trap — leaving a cluster billing.", {}))

doc.add_heading("6.3 What the submit script does, in order", level=2)
bullets([
    ("zip + upload", "packages src/ and pushes src.zip to the bucket"),
    ("ensure_input", "uses each raw file already in the bucket, uploads it only "
                     "if missing, and errors clearly if it is in neither place"),
    ("wheelhouse", "downloads platform wheels and stages them in the bucket "
                   "(skipped if already there; FORCE_WHEELS=1 to rebuild)"),
    ("arm the cleanup trap", "BEFORE creating anything"),
    ("create cluster", "1 master + N workers, with the offline init action"),
    ("submit stages", "in dependency order, deliverables before the expensive tail"),
    ("delete cluster", "on ANY exit — success, failure or Ctrl-C"),
])

doc.add_heading("6.4 Capturing the ≥5-machines evidence", level=2)
rich(("The submit script deletes the cluster on exit — that is what stops the "
      "billing — so the machine list exists ONLY while the run is in progress. "
      "From a second terminal:", {}))
code("""
bash scripts/cloud_status.sh evidence    # cluster size + every VM, named
bash scripts/cloud_status.sh clusters    # anything alive is being billed
bash scripts/cloud_status.sh outputs     # what has landed in GCS
""")
para("Output is archived in docs/cloud_evidence/ and tracked in git, unlike "
     "outputs/, precisely because it cannot be regenerated.")

doc.add_heading("6.5 Verifying the cloud run", level=2)
code("""
gsutil -m cp -r gs://taxi-project-noyabayazi/taxi/outputs/* ./cloud_outputs/
.venv/bin/python -m src.verify_cloud_run --cloud-dir ./cloud_outputs --scale full
""")
para("Result from the graded run:")
code("""
trip count matches the baseline -> cloud=1,614,508 baseline=1,614,508
Method D corridor set matches   -> cloud=420 baseline=420 shared=420
Method D supports identical     -> 420 identical
CLOUD RUN VERIFICATION PASSED
""", bg="EAF3EA")

doc.add_page_break()

# =================================================================== 7 =====
doc.add_heading("7. Every cloud failure, and what it actually was", level=1)
para("None of these was a logic error, and none was reachable from local "
     "testing. They are recorded because the diagnosis was the expensive part, "
     "not the fix.")

doc.add_heading("7.1 “Permissions are missing for the default service account”", level=3)
para("A fresh project's compute service account has no Storage role. DataProc "
     "VMs read input, write parquet and create a staging bucket as that account. "
     "Fix: grant roles/storage.admin (§6.1).")

doc.add_heading("7.2 “Initialization action timed out” — it was not a timeout", level=3)
code("""
Failed to establish a new connection: [Errno 101] Network is unreachable
ERROR: No matching distribution found for h3==3.7.7
""", bg=WARN_BG)
rich(("The cluster VMs have no route to the public internet, which is normal for "
      "a restricted org VPC. pip retried until the clock ran out and gcloud "
      "reported the clock. Raising the timeout cannot reach an unreachable host.",
      {}))
rich(("Fix: ", {"b": True}),
     ("the VMs CAN reach GCS — that is where they write their own logs. So "
      "platform wheels are staged into the bucket from a machine that has "
      "internet, and scripts/init_offline_deps.sh installs them with --no-index. "
      "Two details that matter: the wheels are built for the IMAGE interpreter "
      "(cp311 / manylinux x86_64, not your arm64 Mac), and --no-deps keeps numpy "
      "off the list — the image's pandas and pyarrow are compiled against its "
      "numpy 1.x and replacing it breaks every pandas_udf.", {}))

doc.add_heading("7.3 The driver received NO environment — the dangerous one", level=3)
code("NotADirectoryError: '/tmp/<job>/src.zip/.spark-tmp'", bg=WARN_BG)
rich(("spark.yarn.appMasterEnv", {"font": "Consolas"}),
     (" reaches the driver only in CLUSTER deploy mode, and ", {}),
     ("gcloud dataproc jobs submit", {"font": "Consolas"}),
     (" runs the driver in CLIENT mode. So the driver got no SPARK_ENV, no "
      "DATA_BASE and no OUTPUT_BASE — only the executors did. SPARK_ENV "
      "defaulted to 'local', which builds a temp dir relative to PROJECT_ROOT, "
      "and on a cluster PROJECT_ROOT resolves INSIDE src.zip.", {}))
callout("WHY THIS ONE MATTERS MOST",
        "The crash was luck. The same missing environment left OUTPUT_BASE "
        "unset, so a run that got one line further would have written every "
        "result to a disk that is deleted with the cluster — and exited 0. "
        "storage.py cannot catch that: it would be correctly writing to a "
        "correctly-resolved local path. Fix: driver env now comes from "
        "`spark-env:` CLUSTER properties, which land in /etc/spark/conf/"
        "spark-env.sh and are sourced under `set -a` in both deploy modes. And "
        "spark_session now REFUSES to start a local-mode session on a node with "
        "/etc/google-dataproc, so this class of failure can never be silent "
        "again.", WARN_BG, WARN)

doc.add_heading("7.4 A failed cluster is not rolled back", level=3)
para("DataProc parks a cluster whose init action failed in state ERROR with its "
     "VMs still RUNNING, so you can read the logs. The cleanup trap was armed on "
     "the line AFTER `clusters create`, so `set -e` aborted the script before the "
     "trap existed — exactly the case it was meant to cover. Three "
     "n2-standard-4 VMs billed unattended until deleted by hand. The trap is now "
     "armed before creation.")

doc.add_heading("7.5 Hour-of-day buckets were machine-dependent", level=3)
rich(("F.hour(F.from_unixtime(TIMESTAMP))", {"font": "Consolas"}),
     (" renders in ", {}),
     ("spark.sql.session.timeZone", {"font": "Consolas"}),
     (", which defaults to the JVM's machine timezone and was never set. The "
      "same 1,614,508 trips bucketed one way on the laptop (UTC+3) and another "
      "on DataProc (UTC) — identical totals, different assignment. Neither is "
      "Porto. Only running the same code in two places exposed it. "
      "config.DATASET_TIMEZONE now pins Europe/Lisbon.", {}))

doc.add_heading("7.6 Smaller ones worth remembering", level=3)
bullets([
    ("Image version", "2.1 pairs with debian11, not debian12. Use 2.2-debian12."),
    ("make_sample never submitted", "every scale except --full reads a pre-built "
     "sample; the cloud script never built it. --full reads RAW_TRAIN directly, "
     "so only the rehearsal could ever hit this."),
    ("Driver heap", "client-mode driver defaults to ~4g on the master; the local "
     "full run needed 10g. Set to 8g via DRIVER_MEM."),
    ("Stage gating", "the cloud script gated the O(n²) family on `!= --full`, so "
     "--mid submitted three stages that are known to OOM there. They are "
     "sample-only, matching run_pipeline.STAGES."),
])

doc.add_page_break()

# =================================================================== 8 =====
doc.add_heading("8. Cost control", level=1)
table(["Mechanism", "What it prevents"],
      [["trap armed before create", "a failed creation leaving VMs billing"],
       ["--max-idle 30m", "an abandoned cluster running overnight"],
       ["DRY_RUN=1", "spending anything to discover a typo"],
       ["--sample rehearsal", "finding a bug during the expensive run"],
       ["ONLY=\"a.py b.py\"", "re-running 13 stages to fix one report"],
       ["deliverables submitted first", "a failure in the expensive tail costing "
                                        "the actual results"]],
      widths=[2.1, 4.6])
para("Total spend for this project was about $3.15 at roughly $0.295 per "
     "node-hour (n2-standard-4 compute + DataProc premium + a 1000 GB boot "
     "disk): $1.77 for the graded run, $0.94 for the rehearsals that found the "
     "bugs above, and ~$0.44 for a targeted re-run of two corrected stages.")

# =================================================================== 9 =====
doc.add_heading("9. Rebuilding the deliverables", level=1)
code("""
# the report figures + the presentation
.venv/bin/python scripts/build_deck_charts.py
.venv/bin/python scripts/build_deck.py        # -> build/deck/*.pptx
.venv/bin/python scripts/lint_deck.py         # geometry check, no renderer needed

# this document
.venv/bin/python scripts/build_devdoc.py      # -> build/docs/*.docx
""")
para("The deck reads its numbers from the generated reports rather than "
     "hard-coding them, so re-running the pipeline changes the slides. "
     "lint_deck.py exists because there is no PowerPoint or LibreOffice on the "
     "build machine, so the deck cannot be rendered and eyeballed — it checks "
     "slide-edge overflow, text-over-chart collisions and text overset instead. "
     "It immediately found seven right-hand columns rendering at the left margin "
     "on top of their charts.")

# ================================================================== 10 =====
doc.add_heading("10. Command cheat sheet", level=1)
code("""
# --- local -----------------------------------------------------------------
.venv/bin/python -m src.validate_env                     # env gate
.venv/bin/python -m src.make_sample --sample             # build a scale
.venv/bin/python -m src.run_pipeline --sample --verify   # stages + verifiers
.venv/bin/python -m pytest tests/ -q                     # 56 tests
bash scripts/cloud_rehearsal.sh                          # URI-path dress run, free

SPARK_SHUFFLE_PARTS=200 SPARK_DRIVER_MEM=10g \\
  .venv/bin/python -m src.run_pipeline --full

# --- cloud -----------------------------------------------------------------
DRY_RUN=1 bash scripts/dataproc_submit.sh                # costs nothing
SCALE=--sample WORKERS=2 bash scripts/dataproc_submit.sh # cheap rehearsal
bash scripts/dataproc_submit.sh                          # full, 5 workers
ONLY="evaluation.py" bash scripts/dataproc_submit.sh     # one stage

bash scripts/cloud_status.sh evidence                    # while it runs!
bash scripts/cloud_status.sh clusters                    # anything billing?

.venv/bin/python -m src.verify_cloud_run \\
  --cloud-dir ./cloud_outputs --scale full

# --- if something is wrong -------------------------------------------------
gcloud dataproc clusters list --region europe-west1
gcloud dataproc jobs list --region europe-west1 --limit 20
gsutil cat gs://<staging>/google-cloud-dataproc-metainfo/<id>/<node>/\\
dataproc-initialization-script-0_output      # init action log
""")

# ================================================================== 11 =====
doc.add_heading("11. Things that are findings, not bugs", level=1)
para("Do not 'fix' these.")
bullets([
    ("≥40 km is empty at every scale", "Porto has no 40 km stretch two taxis "
     "repeat. A power law fitted on 5k–755k predicted 26–28 km at 1.71M; "
     "measured 26.25 km."),
    ("≥20 km is hollow", "it populates, but all 20 routes have ≤2 taxis and the "
     "longest is 2 trips from ONE vehicle. Length and confidence move in "
     "opposite directions — hence the support_taxis column."),
    ("HyperLogLog is ~5× slower than exact here", "group cardinality is capped by "
     "the 442-taxi fleet, so the sketch never pays. The original justification "
     "(85M pairs) was the INPUT size, which is the wrong quantity. Kept as a "
     "measured negative result."),
    ("Frequency sketches fail on long corridors, and more data makes it worse",
     "recall at ≥10 km fell 0.17 → 0.01 between 5k and 200k. Space-Saving keeps "
     "heavy hitters; a corridor is long BECAUSE few trips repeat it. Their real "
     "value is memory: 3× smaller at 5k, 105× at 200k."),
    ("The cluster was slower than the laptop", "60 min on 6 machines vs 33 min on "
     "one. At 1.71M this still fits in a single machine, so distribution buys "
     "fault tolerance and headroom, not speed."),
    ("No Bloom filter", "it is on the brief's list, but exact containment is "
     "already one Aho-Corasick pass and a Bloom pre-filter would exist only to "
     "be named. Saying why is worth more than the checkbox."),
])

doc.save(str(OUT))
print(f"{OUT}")
