"""
build_report.py  --  the technical report, as a .docx
=====================================================
Writes `build/docs/Porto_Taxi_Technical_Report.docx`: what the project is, how
each algorithm works, how it was executed on GCP, and what it found.

This is the descriptive companion to the developer guide. Where that document
answers "how do I run it", this one answers "what did you build, and why does it
work". Figures are the same PNGs the presentation uses, so the two cannot
disagree.
"""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))

from _docx_kit import (ACCENT, CODE_BG, GOOD, INK, MUTED, NOTE_BG, WARN,
                       WARN_BG, bullets, callout, code, figure, new_doc, para,
                       rich, table)

FIGS = pathlib.Path("build/deck/figs")
OUT = pathlib.Path("build/docs/Porto_Taxi_Technical_Report.docx")
OUT.parent.mkdir(parents=True, exist_ok=True)

doc = new_doc()

# ----------------------------------------------------------------- cover ---
para("BIG DATA & CLOUD COMPUTING — FINAL PROJECT", 11, True, ACCENT, space_after=2)
para("Popular Long Sub-Routes in 1.7 Million Porto Taxi Trajectories", 24, True,
     INK, space_after=4)
para("Four independent mining methods, exact and approximate structures, "
     "executed on a six-machine GCP DataProc cluster.", 12, color=MUTED,
     space_after=16)
table(["", ""],
      [["Dataset", "1,710,670 trips · 442 taxis · Porto, 2013–2014 · GPS every 15 s"],
       ["Representation", "H3 hexagons, resolution 9 (~174 m edge) — a trip is a string"],
       ["Methods", "A clustering · B maximal-frequent · C transition graph · D suffix array"],
       ["Approximate", "Space-Saving · Count-Min · HyperLogLog"],
       ["Cluster", "DataProc 2.2, 1 master + 5 workers (n2-standard-4), europe-west1"],
       ["Headline", "420 corridors reproduced bit-for-bit on the cluster; longest 26.25 km"]],
      widths=[1.4, 5.3])
doc.add_page_break()

# =================================================================== 1 =====
doc.add_heading("1. The problem", level=1)
para("A sub-route is a contiguous piece of a journey. The task is to find the "
     "sub-routes that are POPULAR — contained in many distinct trips — and LONG, "
     "at six minimum-length configurations: 1, 3, 5, 10, 20 and 40 km. The top "
     "100 must be reported at each, by four independent methods, together with "
     "activity zones, anomalous routes, an approximate-versus-exact comparison, "
     "and a map.")
para("The reason four methods are required is not redundancy. Each has a "
     "different failure mode, so where they agree the result is trustworthy in a "
     "way no single method can establish about itself.")

doc.add_heading("1.1 Why a full trip is the wrong unit", level=2)
para("A complete journey from origin to destination is nearly unique — no two "
     "passengers share an exact route across a whole city. Counting whole trips "
     "would report a popularity of 1 for almost everything. The deliverable is "
     "therefore about SHARED SEGMENTS: the stretch of road that many otherwise "
     "different journeys pass through together.")

# =================================================================== 2 =====
doc.add_heading("2. Representation: a trip is a string", level=1)
para("Every design decision below follows from one modelling choice.")
callout("THE CENTRAL IDEA",
        "Snap each GPS point to an H3 hexagon at resolution 9 and collapse "
        "consecutive repeats. A trajectory becomes a WORD over a cell alphabet. "
        "A sub-route is then a contiguous SUBSTRING, popularity is substring "
        "SUPPORT, and length is the ground distance along the cell path. The "
        "problem becomes classical string processing — and because all four "
        "methods consume the same representation, their outputs are directly "
        "comparable.")
code("trip 1372636858620000589  →  8939220f027ffff > 8939220f023ffff > 8939220f02fffff > …")

doc.add_heading("2.1 Why H3, and why resolution 9", level=2)
para("The grid was chosen by measurement, not assertion. Five candidates were "
     "encoded and compared on the sample:")
table(["Grid", "Res", "Cell (m)", "Avg cells/trip", "Distinct cells", "Bearing entropy"],
      [["H3", "8", "461", "7.8", "416", "1.176"],
       ["H3", "9", "174", "17.2", "1,853", "0.939"],
       ["H3", "10", "66", "29.6", "6,821", "0.749"],
       ["geohash", "6", "610", "9.3", "571", "1.016"],
       ["geohash", "7", "76", "29.4", "6,638", "0.730"]],
      widths=[0.9, 0.5, 0.8, 1.3, 1.3, 1.4])
rich(("Bearing entropy", {"b": True}),
     (" is the conflation measure that matters: the entropy of travel directions "
      "leaving a cell. A cell sitting on one road sees one or two directions; a "
      "cell that has swallowed two parallel roads sees several. Lower is better.",
      {}))
para("Two honest readings. First, resolution 9 is a compromise rather than an "
     "optimum — resolution 10 conflates less (0.749 vs 0.939) but costs 3.7× the "
     "alphabet and 1.7× the sequence length, and since sub-route keys are "
     "SEQUENCES of cells that multiplies the mining key space superlinearly. "
     "Second, these numbers do not by themselves justify H3 over geohash; at "
     "comparable cell sizes the two score about the same. The reason to prefer "
     "H3 is structural: a hexagon has six equidistant neighbours, so a "
     "trajectory is a walk with uniform step cost, whereas geohash rectangles "
     "have edge and corner neighbours at different distances and distort with "
     "latitude.")

doc.add_heading("2.2 Two definitions of popular", level=2)
para("Support is counted two ways, deliberately. There are only 442 vehicles in "
     "the dataset across a year, so a corridor driven two hundred times by one "
     "driver commuting to their own stand is indistinguishable from a genuinely "
     "public route if you count trips alone. Every reported corridor therefore "
     "carries both a distinct-trip support and a distinct-taxi support.")

doc.add_page_break()

# =================================================================== 3 =====
doc.add_heading("3. The processing pipeline", level=1)
para("Seventeen stages, gated by scale. Each writes parquet or CSV that the next "
     "reads, so any stage can be re-run alone.")

doc.add_heading("3.1 Phase 1 — cleaning", level=2)
para("Parses the POLYLINE column into coordinate arrays and rejects trips on "
     "endpoint-visible criteria: vendor MISSING_DATA flag, fewer than 2 GPS "
     "points, more than 4,000 points, start or end outside the Porto metro "
     "bounding box, and duplicate TRIP_IDs. Results at full scale:")
table(["Rule", "Trips", "Share"],
      [["MISSING_DATA flagged by vendor", "10", "0.00%"],
       ["fewer than 2 GPS points", "36,510", "2.13%"],
       ["more than 4,000 GPS points", "0", "0.00%"],
       ["start or end outside Porto metro box", "10,463", "0.61%"],
       ["duplicate TRIP_ID", "11", "0.00%"]],
      widths=[3.4, 1.4, 1.2])
rich(("1,710,670 read → 1,663,886 written (97.3%). ", {"b": True}),
     ("A duplicate TRIP_ID would be counted twice by every downstream support "
      "metric, which is why deduplication happens once, here.", {}))

doc.add_heading("3.2 Phase 2 — features and anomaly flags", level=2)
para("Computes per-trip geometry (haversine distance, duration, mean and maximum "
     "segment speed, sinuosity) and raises six independent anomaly flags. "
     "Endpoint checks cannot see mid-route corruption, so this is where "
     "trajectory-level problems surface.")
figure(FIGS / "anomalies.png",
       "Figure 1 — 4.28% of cleaned trips fail at least one physical detector. "
       "The worst offender records 8,940 km/h and a single 147,345 km segment.",
       width=5.9)

doc.add_heading("3.3 Phase 4 — spatial encoding", level=2)
para("Applies the H3 encoding through a pandas_udf (Arrow-vectorised) and drops "
     "every trip flagged anomalous: 1,663,886 in, 1,614,508 encoded, 49,378 "
     "excluded (2.97%).")
callout("WHY CORRUPT TRIPS ARE EXCLUDED, NOT MERELY LABELLED",
        "Sub-route length is measured between cell centres. A window spanning a "
        "GPS dropout therefore reports the WIDTH OF THE GAP as route length — "
        "and those fabricated routes are long, so they land directly in the "
        "graded lists. Measured without the guard: a worst window of 53.6 km per "
        "cell-hop, 45× the physical bound, and 563 windows of ≥10 km built "
        "across gaps, one claiming 59.8 km from 19 cells. Two layers of defence: "
        "encoding drops anomalous trips, and every miner additionally splits "
        "trajectories at any hop exceeding ~1.18 km. That bound is DERIVED — the "
        "retained-speed limit plus cell quantisation — not tuned until the "
        "output looked reasonable. After the guard: worst hop 1.10 km, zero "
        "cross-gap windows.", WARN_BG, WARN)

doc.add_page_break()

# =================================================================== 4 =====
doc.add_heading("4. The algorithms", level=1)

doc.add_heading("4.1 Method D — generalised suffix array with LCP intervals", level=2)
para("This is the method that carries the deliverable: exact, single-pass and "
     "scalable. It is described first because the others are best understood by "
     "contrast.")
rich(("The insight. ", {"b": True}),
     ("Instead of enumerating every window of every trip — which is O(n²) in "
      "trip length — enumerate every SUFFIX. Sorting the suffixes places every "
      "repeat of every substring in a contiguous block, so the number of emitted "
      "rows drops from O(n²) to O(n log n) per bucket while the answer stays "
      "exact.", {}))
para("Construction, per bucket:")
bullets([
    "Emit every suffix of every gap-free segment of every trip, truncated at 200 "
    "cells (~60 km), tagged with its trip id, taxi id and preceding cell.",
    "Bucket the suffixes by their first three cells. Every occurrence of a "
    "substring S of length ≥3 shares S's first three cells, so all occurrences "
    "of S land in the SAME bucket and a bucket can be counted with no "
    "cross-partition communication. This is what makes the method distributable "
    "without approximation.",
    "Within a bucket, sort the suffixes — that is the suffix array — and compute "
    "the LCP (longest common prefix) array between adjacent entries.",
    "Walk the LCP intervals with a stack sweep (Abouelhoda–Kurtz–Ohlebusch). An "
    "interval [l..r] at depth h means the substring of length h occurs in sorted "
    "positions l through r, i.e. with support r − l + 1.",
])
rich(("Maximality. ", {"b": True}),
     ("The assignment asks for MAXIMAL frequent sub-routes — a route that cannot "
      "be extended without losing frequency. Right-maximality is guaranteed by "
      "construction, because the LCP-interval enumeration only yields branching "
      "intervals. Left-maximality requires more than one distinct preceding cell "
      "(or a trip start). Each candidate carries the support of its best "
      "one-cell extension in each direction, which turns maximality into a pure "
      "filter:", {}))
code("maximal(min_sup)  ⟺  support ≥ min_sup  AND  max(best_right, best_left) < min_sup")
para("The consequence is important for cost: the entire support-floor grid, and "
     "therefore the per-length calibration, is answered from ONE mining pass "
     "instead of re-deriving a support table for every candidate floor. That is "
     "what lets this method carry the deliverable at a scale where window "
     "enumeration cannot.")
callout("A CONSEQUENCE WORTH UNDERSTANDING",
        "Because D reports only maximal routes, its top support is LOWER than A's "
        "or C's, and this is correct rather than a weakness. At ≥1 km, Method A's "
        "top route is a 4-cell, 1.10 km corridor contained in 79,952 trips — but "
        "its best one-cell extension is still contained in 64,936, so it is not "
        "maximal and D suppresses it as a redundant prefix of a longer frequent "
        "corridor. D instead reports 14,330 for a route that cannot be extended. "
        "Both counts were verified by brute-force containment. They answer "
        "different questions, and the comparison table says so explicitly.")

doc.add_heading("4.2 Method A — MinHash-LSH and greedy star clustering", level=2)
para("Group similar trajectories, then report the sub-route the group actually "
     "shares.")
bullets([
    "Each trip becomes a set of DIRECTED bigram shingles (cell → next cell), "
    "taken only within gap-free segments so a lost signal is never treated as a "
    "transition.",
    "HashingTF maps the shingle set to a fixed-dimension binary vector.",
    "MinHashLSH approximates Jaccard similarity: random hash permutations "
    "produce a signature, banding makes similar signatures collide, and "
    "approxSimilarityJoin returns candidate pairs below a distance threshold. "
    "This is the structure that turns an O(N²) all-pairs comparison into roughly "
    "O(N).",
    "GREEDY STAR clustering over the pruned similarity graph: repeatedly take "
    "the highest-degree unassigned trip as a seed and attach every trip directly "
    "similar to it. Membership requires direct similarity TO THE SEED, so "
    "clusters cannot chain dissimilar trips the way connected components would.",
    "Per cluster, extract the longest contiguous cell run present in at least "
    "60% of members — the corridor the cluster agrees on, with the members' "
    "individual origins and destinations dropped.",
    "Re-measure each run's support against ALL trips by containment, so support "
    "means the same thing as in the other methods.",
])
rich(("Why it is approximate. ", {"b": True}),
     ("LSH uses unseeded hash functions and the method clusters a capped 50,000-"
      "trip subset, because the self-join grows quadratically. Its corridor "
      "GEOGRAPHY is stable across samples; its exact route extents are not. That "
      "is why the cloud verifier treats Method A as informational rather than "
      "demanding equality.", {}))

doc.add_heading("4.3 Method C — transition graph, PageRank and heavy paths", level=2)
para("Model the city as a directed weighted graph and mine it. A node is an H3 "
     "cell; an edge is an observed transition; the weight is the number of "
     "DISTINCT trips making that transition, deduplicated per trip so a circling "
     "taxi cannot inflate an edge.")
bullets([
    ("Activity zones", "weighted PageRank by power iteration with DataFrame "
     "joins, including explicit redistribution of dangling mass. The highest-"
     "ranked cells are the city's structural hubs."),
    ("Popular routes", "HEAVY PATHS — greedy walks that follow the busiest "
     "outgoing transition. This is graph-guided candidate generation rather "
     "than exhaustive enumeration."),
    ("Validation", "every candidate is then checked against real trips by "
     "containment support, so the method can never emit a 'Frankenstein route' "
     "assembled from popular edges that no taxi actually drove end to end."),
])

doc.add_heading("4.4 Method B — maximal frequent from a window table", level=2)
para("The textbook formulation: enumerate every window of every trip into a "
     "support table, keep those clearing the minimum-support floor, then remove "
     "any that are extendable. It is the ground truth the others are validated "
     "against at sample scale, where it agrees with Method D on every maximal "
     "route with zero support disagreements.")
rich(("It does not scale, and that is a finding rather than an omission. ",
      {"b": True}),
     ("It shares an O(n²) window table with the exact baselines; measured at "
      "200,000 trips the exact stage exhausts memory and the maximal stage "
      "spilled 21 GB without finishing, while Method D produces the identical "
      "answer in 41 seconds. Method B is therefore demonstrated at sample scale "
      "and gated off above it, and the report states this rather than implying "
      "it scales.", {}))

doc.add_heading("4.5 Aho-Corasick — the supporting structure", level=2)
para("Methods A and C both need the same thing: given a few thousand candidate "
     "routes, how many trips contain each? The naive nested loop is roughly "
     "5×10⁹ substring scans at full scale and was the single slowest thing in "
     "the pipeline. An Aho-Corasick automaton answers ALL candidates in one pass "
     "per trip — O(len(trip) + matches) instead of O(len(trip) × candidates) — "
     "by building a trie of the patterns with failure links that jump to the "
     "longest proper suffix that is still a prefix of some pattern.")
para("It is built over CELL TOKENS rather than characters. That also removes a "
     "class of bug: a delimiter-joined string can match across a cell boundary "
     "if delimiter handling is ever wrong, whereas a token automaton cannot.")

doc.add_heading("4.6 The approximate structures", level=2)
bullets([
    ("Space-Saving (frequent items)", "a mergeable Misra–Gries-family sketch "
     "holding a bounded number of counters. On overflow it evicts the smallest "
     "counter and reuses its slot, which is what bounds memory. It returns the "
     "top-k with per-item lower and upper support bounds. This is the top-k "
     "FINDER."),
    ("Count-Min", "a mergeable frequency oracle: d independent hash functions "
     "index d rows of counters, and an estimate is the MINIMUM across rows, so "
     "it never underestimates. It stores no keys, so it cannot enumerate the "
     "top-k by itself — it is auxiliary."),
    ("HyperLogLog", "a distinct-count sketch estimating cardinality from the "
     "distribution of leading zeros in hashed values, applied to distinct taxis "
     "per activity-zone cell."),
])
para("The reason these scale is structural: the exact path shuffles hundreds of "
     "millions of window rows into a groupBy, whereas a sketch is built per "
     "partition and only the small summaries are merged across the network.")

doc.add_page_break()

# =================================================================== 5 =====
doc.add_heading("5. Execution on Google Cloud Platform", level=1)
para("The brief requires the pipeline to run on at least five machines reading "
     "from cloud storage. The same code ran unchanged on DataProc.")
table(["Component", "Configuration"],
      [["Cluster", "1 master + 5 workers = 6 machines"],
       ["Machine type", "n2-standard-4 (4 vCPU, 16 GB) each"],
       ["Image", "DataProc 2.2.84-debian12 — Spark 3.5.x, Python 3.11"],
       ["Region", "europe-west1 (same region as the bucket)"],
       ["Storage", "gs://taxi-project-noyabayazi/taxi — raw, processed, outputs"],
       ["Runtime", "60 min wall, 50.5 min of job time across 13 stages"],
       ["Cost", "$1.77 for the graded run; ~$3.15 including all rehearsals"]],
      widths=[1.5, 5.2])

doc.add_heading("5.1 How the code stays identical", level=2)
para("Two design rules make local and cloud the same code path. Every parquet "
     "filename is built by a single function, so no module can reconstruct a "
     "path independently and drift. And every output write goes through one "
     "storage module which dispatches to the Hadoop FileSystem whenever the path "
     "has a URI scheme. Locally that path is file://, on the cluster it is "
     "gs://, and nothing above that boundary knows the difference.")
callout("THE FAILURE MODE THIS PREVENTS",
        "Using open() or os.makedirs on a gs:// base does not error. It silently "
        "creates a local directory literally named 'gs:' on whichever node "
        "executed the line — and that node is deleted with the cluster. The run "
        "reports success and produces nothing.")

doc.add_heading("5.2 Dependencies without internet access", level=2)
para("The cluster VMs have no route to the public internet, which is normal for "
     "a restricted organisational VPC. The stock pip-based initialization action "
     "fails on every node with 'Network is unreachable', and because pip retries "
     "until the clock runs out, the surface error is a TIMEOUT, which points at "
     "entirely the wrong thing.")
para("The VMs can, however, reach Cloud Storage — that is where they write their "
     "own logs. So platform-correct wheels are staged into the project bucket "
     "from a machine with internet, and a custom initialization action installs "
     "them with --no-index. Two details matter: the wheels are built for the "
     "IMAGE interpreter (cp311, manylinux x86_64) rather than the submitting "
     "machine, and dependencies are deliberately not resolved, because the "
     "image's pandas and pyarrow are compiled against its own numpy and "
     "replacing it breaks every pandas_udf.")

doc.add_heading("5.3 Cost control", level=2)
bullets([
    ("Cleanup trap armed before creation", "a cluster whose initialization fails "
     "is NOT rolled back — DataProc parks it in state ERROR with its VMs still "
     "running so the logs can be read."),
    ("--max-idle 30m", "an abandoned cluster cannot run overnight."),
    ("Dry-run mode", "verifies credentials, APIs, bucket, inputs and that the "
     "code imports, without creating anything."),
    ("Sample-scale rehearsal", "the whole pipeline on 5,000 trips before "
     "committing to the full run."),
    ("Deliverable stages submitted first", "a failure in the expensive "
     "approximate tail cannot cost the actual results."),
])

doc.add_page_break()

# =================================================================== 6 =====
doc.add_heading("6. Results", level=1)

doc.add_heading("6.1 The deliverable", level=2)
figure(FIGS / "deliverable.png",
       "Figure 2 — longest corridor found at each length configuration, with the "
       "number of distinct taxis driving it. The ≥20 km band populates but is "
       "supported by a single vehicle; ≥40 km is empty at every support floor.")
table(["Min length", "Floor (trips)", "= X%", "# maximal", "Top support", "Taxis", "Longest"],
      [["≥ 1 km", "5,000", "0.3097%", "639", "14,330", "435", "4.70 km"],
       ["≥ 3 km", "2,500", "0.1548%", "292", "4,701", "435", "6.17 km"],
       ["≥ 5 km", "1,000", "0.0619%", "195", "1,945", "369", "8.72 km"],
       ["≥ 10 km", "50", "0.0031%", "217", "107", "84", "12.41 km"],
       ["≥ 20 km", "2", "0.0001%", "20", "2", "1", "26.25 km"],
       ["≥ 40 km", "2", "0.0001%", "0", "—", "—", "empty"]],
      widths=[1.0, 1.1, 0.9, 0.9, 1.0, 0.7, 0.9])
rich(("The support floor is calibrated per configuration ", {"b": True}),
     ("— the tightest floor that still fills the band, i.e. the strongest claim "
      "the data supports at that length. Floors are ABSOLUTE trip counts rather "
      "than percentages, because a percentage floor is scale-dependent the wrong "
      "way: 0.01% is 2 trips on a 5,000-trip sample but 171 trips at 1.71M, so "
      "the long bands would empty out AS THE DATASET GROWS. The equivalent X% is "
      "reported alongside.", {}))

doc.add_heading("6.2 Where the corridors are", level=2)
para("No landmark information was supplied to the pipeline; these fall out of "
     "the mining.")
table(["Band", "Trips", "Taxis", "Corridor"],
      [["≥ 1 km", "14,330", "435", "inside the centre, around São Bento"],
       ["≥ 3 km", "4,701", "435", "Boavista → Matosinhos"],
       ["≥ 5 km", "1,945", "369", "Boavista → Matosinhos"],
       ["≥ 10 km", "107", "84", "Hospital S. João → Sá Carneiro airport"],
       ["≥ 20 km", "2", "1", "Matosinhos → Hospital S. João"]],
      widths=[0.9, 1.0, 0.8, 4.0])

doc.add_heading("6.3 Is it a popular route, or one driver's habit?", level=2)
figure(FIGS / "diversity.png",
       "Figure 3 — distinct taxis driving the top corridor at each length. The "
       "short bands are driven by 98% of the fleet; at ≥20 km it collapses to a "
       "single vehicle.", width=5.9)
para("This is the strongest evidence that the short-band results are real: 435 "
     "of 442 taxis traverse the top corridors, so they are genuinely public "
     "rather than one person's routine. It is equally the clearest statement of "
     "where the answer stops being trustworthy.")

doc.add_heading("6.4 Length and confidence move in opposite directions", level=2)
para("All twenty routes at ≥20 km have at most two distinct taxis, and the "
     "longest — 26.25 km — is two trips from ONE vehicle. Nothing at ≥40 km "
     "repeats at any floor down to two trips. Both are findings about Porto and "
     "about the data volume, not filter failures, and they are reported as such "
     "rather than as a top-100 list a reader would assume is meaningful.")
figure(FIGS / "scaling.png",
       "Figure 4 — longest corridor against dataset size at three support "
       "floors. A power law fitted on 5k–755k (R² ≈ 0.98) predicted 26–28 km at "
       "1.71M; the measured value is 26.25 km. The same fit correctly predicted "
       "that ≥20 km would populate and ≥40 km would not.")

doc.add_heading("6.5 Choosing X", level=2)
figure(FIGS / "floor_sweep.png",
       "Figure 5 — the trade the brief asks you to explore. At a floor of 5,000 "
       "trips the longest corridor is 5.80 km and highly credible; at 2 trips it "
       "is 26.25 km and means very little.", width=5.9)

doc.add_heading("6.6 Activity zones", level=2)
table(["Rank", "Location", "Traversals", "Taxis", "Trips/taxi"],
      [["1", "Sá Carneiro airport", "72,048", "438", "164.5"],
       ["3", "Sá Carneiro airport", "67,733", "438", "154.6"],
       ["4", "Maia", "68,667", "438", "156.8"],
       ["5", "Sá Carneiro airport", "66,208", "438", "151.2"],
       ["6", "Campanhã station", "42", "29", "1.4"]],
      widths=[0.7, 2.4, 1.2, 0.9, 1.0])
para("Six of the top eight cells sit around the airport, each touched by 438 of "
     "442 taxis. For a taxi fleet this is exactly what should win, which makes it "
     "a strong sanity check on the PageRank implementation. The trips-per-taxi "
     "column separates a genuine public hotspot (164 trips per vehicle across the "
     "whole fleet) from incidental traffic (1.4 trips across 29 vehicles).")

doc.add_heading("6.7 Do the methods agree?", level=2)
figure(FIGS / "agreement.png",
       "Figure 6 — fraction of the row method's corridors having a cell-set "
       "Jaccard ≥ 0.5 partner in the column method's, at ≥3 km.", width=4.4)
para("Method A and Method D agree at 0.94. A randomised LSH clustering and an "
     "exact string algorithm converging on 94% of the same corridors is the "
     "strongest available evidence that those corridors are real, precisely "
     "because the two fail in unrelated ways. Method C agrees least (0.39–0.47), "
     "which is not a defect: it optimises flow through a graph rather than "
     "substring support, so it is the lens that legitimately disagrees.")

doc.add_heading("6.8 Do the corridors generalise?", level=2)
figure(FIGS / "holdout.png",
       "Figure 7 — corridors mined from training data, tested against 318 trips "
       "the pipeline never saw. Lift is measured against a null of random walks "
       "over the held-out city's own road adjacency, length-matched.", width=5.9)
para("Every other check in this project is internal — verifiers recount support "
     "against the same table the mining used. This is the only one that can "
     "distinguish a real corridor from a memorised training path, and all three "
     "methods clear their null by 3.6× to 4.6×. The null deliberately uses "
     "plausible unmined routes rather than uniform-random cells, which would be "
     "disconnected and would inflate the lift into meaninglessness.")

doc.add_heading("6.9 Does popularity depend on the hour?", level=2)
figure(FIGS / "temporal.png",
       "Figure 8 — overlap between each time bucket's top-100 and the all-time "
       "top-100. Mean 0.81.", width=5.9)
para("The corridors are structural: the same stretches dominate at 03:00 and at "
     "08:00, so it is the road network rather than time-varying demand that "
     "decides where taxis go. The all-time list is therefore a fair summary "
     "rather than an average describing no actual hour — which is worth having "
     "measured rather than assumed. Night is least typical (0.75) and midday "
     "most (0.89).")

doc.add_heading("6.10 Approximate versus exact", level=2)
figure(FIGS / "sketch_accuracy.png",
       "Figure 9 — top-100 recall of the Space-Saving sketch against the exact "
       "groupBy, at two dataset sizes.")
para("The sketches are near-perfect for short frequent corridors and useless for "
     "long rare ones — and the failure does not heal with scale. Recall at ≥10 km "
     "fell from 0.17 at 5,000 trips to 0.01 at 200,000. The cause is structural "
     "rather than statistical: Space-Saving retains HEAVY HITTERS, and a corridor "
     "is long precisely because few trips repeat it, so long corridors sit in the "
     "tail by construction. Adding data adds more short frequent corridors "
     "competing for the same bounded set of counters, so long ones are evicted "
     "harder.")
figure(FIGS / "sketch_memory.png",
       "Figure 10 — the real case for the sketches: 66 MB of fixed capacity "
       "against a 6,987 MB exact key table at 200k trips, with the advantage "
       "growing as the data grows.", width=5.4)
para("Runtime is a wash (86.5 s against 89.0 s). The sketch does not save time; "
     "it saves the key table, and that saving strengthens with scale — roughly 3× "
     "at 5,000 trips and 105× at 200,000.")

doc.add_heading("6.11 A measured negative result: HyperLogLog", level=2)
figure(FIGS / "hll.png",
       "Figure 11 — HyperLogLog is 5.19× SLOWER than exact distinct counting on "
       "this workload.", width=5.2)
para("The original justification for reaching for a sketch was that there are "
     "roughly 85 million (cell, taxi) pairs at full scale. That is the INPUT "
     "size, and it is the wrong quantity. What decides whether a distinct-count "
     "sketch pays is the CARDINALITY PER GROUP — and the fleet is 442 taxis, so "
     "no cell can ever exceed 442 distinct values. An exact set of at most 442 "
     "ids is trivial, and the sketch's fixed register array is pure overhead at "
     "every scale. The result is kept because a negative result that was measured "
     "is worth more than a positive one that was assumed. HyperLogLog would earn "
     "its place on an unbounded group — distinct passengers per cell, for "
     "instance.")

doc.add_page_break()

# =================================================================== 7 =====
doc.add_heading("7. Verification", level=1)
para("Three independent layers, none trusting the others: 56 unit tests over the "
     "pure functions; nine stage verifiers that each recount a stage's output by "
     "a DIFFERENT route than the mining used — brute-force substring containment "
     "against the Aho-Corasick automaton, for example — and a cloud verifier "
     "comparing the cluster run against the local baseline.")

doc.add_heading("7.1 Why Method D is the load-bearing check", level=2)
para("Method D contains no randomness whatsoever: no sampling, no seeds, no "
     "hash-order dependence. Its corridors and supports are a function of the "
     "input alone. If a cluster run produces different numbers, the cluster read "
     "different data. That makes it the single most informative comparison "
     "available, and it is the only one held to exact equality.")
code("""
trip count matches the baseline -> cloud=1,614,508 baseline=1,614,508
Method D corridor set matches   -> cloud=420 baseline=420 shared=420
Method D supports identical     -> 420 identical
activity zones                  -> 50/50 identical, incl. PageRank floats
km-per-cell within 1.18 km      -> 1,054 corridor rows, 0 violations
CLOUD RUN VERIFICATION PASSED
""", bg="EAF3EA")
para("420 corridors reproduced bit-for-bit across a different machine count, a "
     "different partitioning and a different filesystem. Methods A and the "
     "sketches are expected to differ slightly and are reported rather than "
     "failed — A samples trips with unseeded MinHash-LSH, and sketch merge order "
     "is partition-dependent, so demanding equality there would manufacture false "
     "alarms. Getting these tiers wrong in either direction is harmful: too "
     "strict and you chase phantom failures, too loose and a real truncation "
     "slips through.")

doc.add_heading("7.2 Distribution is not the same as speed", level=2)
figure(FIGS / "cloud_local.png",
       "Figure 12 — per-stage wall time, six machines against one laptop.")
para("The cluster took 60 minutes against the laptop's 33. This is reported "
     "rather than hidden because it is the honest result: at 1.71 million trips "
     "the problem still fits in a single machine's memory, so distribution buys "
     "fault tolerance and headroom rather than speed, and the coordination and "
     "shuffle-over-network costs are real. The exception is instructive — the "
     "suffix array costs the same on both (409.1 s against 409.4 s) because it is "
     "a single pass already partitioned by three-cell prefix, so there is nothing "
     "to shuffle. The stages that got slower are exactly the ones that do.")

# =================================================================== 8 =====
doc.add_heading("8. Limitations", level=1)
bullets([
    ("≥40 km is empty and ≥20 km is hollow", "findings about the city and the "
     "data volume, but they must be presented as such rather than as lists a "
     "reader will assume are meaningful."),
    ("Method B does not scale", "demonstrated at sample scale only. Method D "
     "reproduces its output exactly, so nothing is lost — but the report must "
     "not imply B scales."),
    ("Method A clusters a capped subset", "50,000 trips, because the LSH "
     "self-join grows quadratically. Support is measured on all trips, but "
     "cluster size is a sample statistic, and route extents shift between "
     "samples even where the corridor geography is stable."),
    ("Methods A and C collect a pruned graph to the driver", "bounded by a "
     "configured cap; going beyond it would need distributed community "
     "detection."),
    ("The suffix array truncates suffixes at 200 cells (~60 km)", "corridors "
     "longer than that are out of scope by construction."),
    ("The destination and travel-time labels are unused", "the held-out "
     "trajectories are used for generalisation testing, but the prediction task "
     "those labels support is a different problem."),
    ("No Bloom filter", "it appears on the brief's list of structures, but exact "
     "containment is already one Aho-Corasick pass and a Bloom pre-filter would "
     "exist only to be named. Explaining why is worth more than the checkbox."),
])

# =================================================================== 9 =====
doc.add_heading("9. Conclusions", level=1)
para("Porto's taxi traffic is dominated by a small number of structural "
     "corridors — the city centre, the Boavista–Matosinhos axis and the airport "
     "approach — that almost the entire 442-vehicle fleet uses at every hour of "
     "the day. Beyond roughly 12 km, popular routes stop existing in any "
     "meaningful sense; past 26.25 km nothing repeats at all.")
para("Three methodological conclusions carry beyond this dataset. First, the "
     "string representation is what makes the comparison possible: four methods "
     "that share nothing algorithmically produce directly comparable output "
     "because they consume the same alphabet. Second, exact is not always the "
     "expensive option — a suffix array with LCP intervals answers the whole "
     "support-floor grid in one pass, where window enumeration cannot finish at "
     "all. Third, approximate structures must be chosen against the right "
     "quantity: the sketches here fail on the long corridors that matter and "
     "HyperLogLog loses to exact counting outright, both because the governing "
     "parameter was group cardinality rather than input size.")

doc.save(str(OUT))
print(OUT)
