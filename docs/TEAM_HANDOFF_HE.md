# מסמך העברה לצוות — Porto Taxi Route Mining (Big Data / Spark)

מסמך זה נועד להביא חבר/ת צוות שלא עקב/ה אחרי העבודה עד כה לנקודה שבה אפשר
לשכפל את הריפו, להבין את הארכיטקטורה, ולהתחיל לתרום — בקריאה אחת.
טקסט בעברית; שמות קבצים, אלגוריתמים, פקודות ומונחים טכניים ב-English.

---

## 1. מטרת הפרויקט (Project Goal)

- **מה המרצה ביקש:** לנתח את דאטהסט מסלולי המוניות של פורטו בסביבת **Big Data
  מבוזרת (Spark)**, ולזהות **מסלולי-משנה פופולריים ארוכים (popular long
  sub-routes)** — top 100 עבור אורכי מינימום {1, 3, 5, 10, 20, 40} ק"מ.
  יש לממש **שלוש שיטות שונות**: (A) clustering, (B) suffix-array / frequent
  sub-route mining, (C) שיטה מקורית שאינה clustering ואינה suffix. בנוסף,
  אופטימיזציה עם **מבני נתונים הסתברותיים** (Bloom / LSH / Count-Min / T-Digest /
  HyperLogLog). ההרצה הסופית צריכה לרוץ על **GCP DataProc עם 5+ מכונות**.
- **הדאטהסט:** Porto Taxi (תחרות Kaggle ECML/PKDD 2015). קובץ `train.csv` בגודל
  ~1.9GB, **1,710,670 נסיעות**, 442 מוניות, דגימת GPS כל 15 שניות. השדה המרכזי
  הוא `POLYLINE` — רשימת נקודות `[longitude, latitude]` (קו-אורך קודם!).
- **מה המערכת הסופית מייצרת:** DataFrame נקי, סטטיסטיקות, top-100 מסלולים לכל סף
  אורך בשלוש שיטות, אזורי פעילות (activity zones), ניתוח מסלולים חריגים, והשוואת
  runtime / memory / accuracy בין השיטות + מפות ויזואליזציה.
- **למה Spark / עיבוד מבוזר / spatial encoding / אלגוריתמים מקורבים:** 1.71M
  נסיעות × ~50 נקודות = ~85M נקודות GPS — לא ריאלי בזיכרון של מכונה אחת. Spark
  מאפשר עיבוד מבוזר. **Spatial encoding (H3)** ממיר GPS רועש לרצף תאים בדיד וניתן
  להשוואה. **אלגוריתמים מקורבים** נחוצים כי ספירה מדויקת של כל תת-המסלולים מייצרת
  מיליוני מפתחות ו-shuffle כבד — sketches נותנים תוצאה טובה בזיכרון קבוע.

---

## 2. הארכיטקטורה המרכזית (Current Architecture)

הרעיון המרכזי — pipeline של medallion (Bronze→Silver→Gold), כל שלב קורא Parquet
וכותב Parquet:

```
GPS trajectory (POLYLINE)
  → cleaned Spark DataFrame            (Phase 1)
  → trip-level features                (Phase 2)
  → ordered H3 cell sequence           (Phase 4 / M3)
  → frequent contiguous sub-route mining (Phase 5: M5 exact / M6 maximal / M7 approx)
```

**התובנה המרכזית (עמוד השדרה של הפרויקט):** אחרי encoding, כל נסיעה הופכת ל**רצף
תאי H3 — כלומר מחרוזת (string) מעל אלפבית של cell-IDs**. אז:
- *sub-route* = תת-מחרוזת רציפה (contiguous substring) של תאים,
- *popular* = support גבוה (מספר נסיעות שמכילות את המסלול),
- *long* = אורך קרקעי ≥ L ק"מ.

כך כל הבעיה מצטמצמת ל**כריית תת-מחרוזות רציפות תכופות מעל אוסף ענק של מחרוזות**.
ייצוג זה תומך בשלוש השיטות: (A) clustering לפי דמיון בין רצפי-תאים, (B) ספירת
n-grams / maximal substrings, (C) גרף מעברים בין תאים. אלפבית אחיד (H3) הוא מה
שמאפשר להשוות מסלולים בין נסיעות שונות.

---

## 3. מה הושלם (What Has Been Completed) — לפי סדר כרונולוגי

> סביבה: **Spark 3.5.1, Java 11.0.31 (Temurin), Python 3.11.9, Windows 11**.

1. **מבנה פרויקט + configuration** — `src/config.py` הוא "מתג הענן" היחיד: כל
   הנתיבים עוברים דרכו; מעבר ל-GCS = שינוי `DATA_BASE=gs://...` בלבד.
   `src/spark_session.py` בונה SparkSession זהה מקומית ובענן (`SPARK_ENV=cloud`).
2. **סביבה מקומית (Windows) + validation** — התקנת Java 11 / Python 3.11 / venv /
   `requirements.txt`. סקריפט `src/validate_env.py` בודק שהכל מחובר. תיעוד מלא
   ב-`SETUP.md`. **באגים שנמצאו ותוקנו:** (א) **נתיב לא-ASCII** (תיקיית "שולחן
   העבודה" בעברית) שבר את ה-launcher של Spark → תוקן ב-`spark_session.py` ע"י
   המרה אוטומטית ל-Windows 8.3 short-path. (ב) **winutils** — נדרשים `winutils.exe`
   **וגם** `hadoop.dll` טעון (גם ל-read, לא רק write) עבור Hadoop 3.3.4;
   `_ensure_hadoop_home()` מוסיף את `C:\hadoop\bin` ל-PATH.
3. **Phase 1 — cleaning** (`src/load_data.py`, `src/clean_data.py`): סכימה
   מפורשת (בלי inferSchema), פענוח `POLYLINE` עם `from_json`, חישוב `n_points`,
   `duration_sec`, קואורדינטות התחלה/סוף, סימון `is_valid`, וכתיבת Parquet נקי.
4. **Phase 1 — validation** (`src/verify_phase1.py`): קריאת ה-Parquet בחזרה,
   בדיקת schema, אימות שהפענוח תקין (`size(points)==n_points`), ופירוט סיבות
   הפסילה.
5. **Phase 2 — feature engineering** (`src/feature_engineering.py`,
   `src/summarize_features.py`): `pandas_udf` וקטורי (Arrow) שמחשב בעבירה אחת
   מרחק Haversine, מהירות ממוצעת/מקסימלית, bounding box, sinuosity, ודגלי אנומליה.
   **החלטה:** `pandas_udf` במקום `explode` (שהיה מנפח 1.7M שורות ל-~85M + shuffle).
   **באג שתוקן:** המרת `array<array<double>>` מ-Arrow ל-numpy דרשה בנייה מפורשת
   `np.array([(p[0],p[1]) for p in pts])`.
6. **H3 spatial encoding** (`src/spatial_encoding.py`, `src/verify_encoding.py`):
   כל נסיעה → רצף תאי H3 (res 9), עם **collapse של תאים כפולים עוקבים**. נשמרים
   `h3_seq_raw` ו-`h3_seq_compact` + מטריקות (raw/compact cells, compression,
   encoded length). כל 5 בדיקות ה-verification עברו (row count, אין רצפים ריקים,
   0 תאים לא-תקינים, compact ≤ raw, תאים בתוך bbox של פורטו).
7. **H3 resolution comparison** — sweep על res 8/9/10 (בתוך `spatial_encoding.py`).
   **החלטה מנומקת: res 9** (edge ~174m) — איזון בין denoising לבין שמירת צורת
   המסלול (ראו §4).
8. **Exact n-gram mining (M5)** (`src/route_mining_exact.py`,
   `src/verify_route_mining.py`): ספירת כל תת-המסלולים הרציפים, `support = מספר
   נסיעות distinct` (dedup בתוך נסיעה), top-100 לכל סף. אומת ע"י ספירת containment
   עצמאית (brute-force) שהתאימה במדויק.
9. **Suffix-style maximal-route mining (M6)** (`src/route_mining_suffix.py`,
   `src/verify_suffix_mining.py`): שמירת מסלולים **maximal/closed** בלבד —
   מסלול נשמר רק אם אין הרחבה בתא בודד (שמאל/ימין) עם support זהה. זו תכונת
   "branching node" של suffix-tree, מחושבת ב-Spark דרך parent-key `groupBy`+join.
10. **Approximate top-k mining (M7)** (`src/route_mining_approx.py`,
    `src/verify_approx_mining.py`): **Space-Saving** (primary, top-k finder) +
    **Count-Min** (auxiliary frequency oracle), בנייה per-partition ומיזוג
    (mapPartitions+merge) — ללא shuffle כבד. השוואה מלאה מול M5. **באג שתוקן:**
    seed של Count-Min חייב להיות ברירת-המחדל של הספרייה כדי ש-`deserialize`+merge
    יעבדו בין partitions.

---

## 4. תוצאות מרכזיות שנמדדו (Important Results) — ערכים אמיתיים בלבד

- **גודל הדאטהסט המלא:** 1,710,670 נסיעות.
- **Sample:** 5,000 נסיעות → **4,867 valid (97.3%)**, 133 נפסלו (101 too few
  points, 32 outside Porto bbox).
- **Anomaly rate:** 2.81% סה"כ (is_teleport 2.12%, is_idle 0.16%, is_too_fast
  0.10%, is_too_short 0.62%).
- **חציונים (sample):** מרחק ~4.0 ק"מ, משך ~615s (~10.25 דק'), מהירות ~23.7
  קמ"ש, sinuosity ~1.45; p95 מרחק ~13.4 ק"מ.
- **H3 resolution comparison (sample):**

  | res | edge | avg compact cells | compression | len_ratio |
  |----|------|------|------|------|
  | 8 | 461m | 8.0 | 7.44x | 1.208 |
  | **9** | **174m** | **17.5** | **3.36x** | **1.158** |
  | 10 | 66m | 30.0 | 1.83x | 1.07 |

- **Exact mining (M5):** runtime ~52.6s; **window emissions = 1,088,976**;
  **distinct routes = 810,933**. Top support: 1km=285, 3km=85, 5km=29, 10km=3,
  20km=1, 40km=1.
- **Maximal (M6):** 810,933 → **24,323 (3.0% נשמרו, 97–99% reduction)**; top
  support נשמר (1km=285, 3km=85, 5km=29).
- **M7 exact-vs-approx:** memory 388MB→**101MB (3.8x)**, shuffle **1,088,976 מול
  ~2 sketch bundles**; precision@100: 1km=1.00, 3km=0.92, 5km=0.63, 10/20/40km=0.00
  (ties בגלל sparsity ב-sample); SS support MAE: 0.1 / 0.9 / 3.9; determinism אומת.

> **הערה חשובה:** support נמוך (1–3) בספים ≥10 ק"מ הוא תוצר של **sample קטן**
> (5k נסיעות אקראיות), לא באג. זה ייפתר על הדאטהסט המלא.

---

## 5. מבנה הריפו (Repository Structure)

```
Taxi/
  README.md          # roadmap + מעבר ל-DataProc
  SETUP.md           # התקנה + validation + טבלת שגיאות נפוצות
  requirements.txt   # תלויות עם גרסאות מוצמדות
  .gitignore         # מונע commit של data/parquet/logs/.venv
  src/               # כל קוד ה-PySpark (tracked)
  docs/              # ARCHITECTURE.md, DESIGN_REVIEW.md, TEAM_HANDOFF_HE.md
  data/              # raw/sample/processed  (IGNORED — לא ב-git)
  outputs/           # statistics/routes/maps/logs  (IGNORED — נוצרים בהרצה)
  notebooks/
```

**קבצי `src/` עיקריים:** `config.py`, `spark_session.py`, `load_data.py`,
`make_sample.py`, `clean_data.py`, `verify_phase1.py`, `feature_engineering.py`,
`summarize_features.py`, `spatial_encoding.py`, `verify_encoding.py`,
`route_mining_exact.py`, `verify_route_mining.py`, `route_mining_suffix.py`,
`verify_suffix_mining.py`, `route_mining_approx.py`, `verify_approx_mining.py`,
`validate_env.py`.

**מה tracked ומה ignored:** ב-git נמצאים **רק source + docs**. **לא** נכנסים
ל-git: הדאטה (`train.csv`, כל `*.csv`), פלטי Parquet, דוחות שנוצרים
(`outputs/statistics/*`, `outputs/routes/*`), logs, ו-`.venv`. כל דוח/פלט נוצר
מחדש ע"י הרצת הסקריפטים.

---

## 6. איך מתחילים לעבוד (How to Start)

```powershell
git clone https://github.com/noyaba1/Taxi.git
cd Taxi
git checkout Noya
```

**סביבה (פעם אחת):**
1. התקינו **Java 11** (Temurin) ו-**Python 3.11** (ראו `SETUP.md` §1–§3).
2. צרו venv והתקינו תלויות:
   ```powershell
   py -3.11 -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   ```
3. **winutils (Windows):** הורידו `winutils.exe`+`hadoop.dll` (hadoop-3.3) ל-
   `C:\hadoop\bin`, והגדירו `HADOOP_HOME=C:\hadoop` (`SETUP.md` §6).
4. **אימות סביבה:** `python -m src.validate_env` → כל השורות `[OK]`.

**איפה הדאטה:** הניחו את `train.csv` בנתיב `train.csv/train.csv` תחת שורש הפרויקט
(או עדכנו `RAW_TRAIN` ב-`config.py`). הדאטה **לא** ב-git — קבלו אותה מנויה/המרצה.

**הרצת ה-pipeline על ה-sample (מומלץ להתחיל כך תמיד):**
```powershell
python -m src.make_sample 5000        # יוצר sample קטן
python -m src.clean_data --sample     # Phase 1
python -m src.feature_engineering --sample   # Phase 2
python -m src.spatial_encoding --sample      # H3 (M3)
python -m src.route_mining_exact --sample    # M5
python -m src.route_mining_suffix --sample   # M6
python -m src.route_mining_approx --sample   # M7
```
כל שלב יש לו `verify_*.py` מקביל להרצה אחרי.

**איך לא לעשות commit לקבצים גדולים:** `.gitignore` כבר מכסה data/parquet/logs/
venv. לפני commit הריצו `git status` ו-`git add -A --dry-run` ווודאו שרק
source/docs נכנסים.

**יצירת feature branch מ-Noya:**
```powershell
git checkout Noya && git pull
git checkout -b feature/<name>
```
**מוסכמת שמות branch:** `feature/clustering`, `feature/graph-mining`,
`feature/visualization`, `docs/presentation`.

---

## 7. חלוקת עבודה מומלצת (Work Division)

> כל אחד עובד ב-branch נפרד, בונה על הקוד הקיים, ולא משכפל לוגיקה.

- **Method A — Clustering** (`feature/clustering`)
  - **Deliverables:** `src/route_mining_clustering.py` + `verify_*`.
  - **תלוי ב:** `trips_clean_encoded_r9_*.parquet` (`h3_seq_compact`).
  - **גישה (מ-DESIGN_REVIEW):** MinHash-LSH על **directed bigram shingles** של
    רצף התאים (לא על סט התאים!) → Label Propagation / Louvain (לא connected
    components — נמנע chaining). ל-MinHashLSH יש מימוש ב-Spark MLlib.
  - **לא לשכפל:** את ה-encoding או את חישוב ה-support (השתמשו בקיים).
  - **validation:** clusters לא ענקיים; consensus route לכל cluster; אורך ≥ L.

- **Method C — Transition Graph + Activity Zones** (`feature/graph-mining`)
  - **Deliverables:** `src/route_mining_graph.py` + `verify_*`.
  - **תלוי ב:** ה-encoded parquet; שקלו GraphFrames.
  - **גישה:** גרף מכוון (nodes=cells, edges=מעברים עוקבים משוקללים) → **PageRank**
    ל-activity zones + **heavy-path** למסלולים פופולריים, עם **validation מול
    נסיעות אמיתיות** (כדי למנוע "Frankenstein routes"). Bloom filter לגיזום קשתות
    נדירות.
  - **validation:** כל מסלול שמדווח קיים בפועל בנסיעות (support ≥ τ).

- **Visualization & Maps** (`feature/visualization`)
  - **Deliverables:** `src/visualization.py` (Folium/kepler.gl).
  - **תלוי ב:** פלטי ה-top routes של M5/M6/M7 והתאים.
  - **חשוב:** לצייר רק **top-N + H3 heatmaps** (Leaflet קורס מעל ~10k polylines);
    לעולם לא `toPandas` על trajectories גולמיים.

- **DataProc / GCP deployment + benchmarks** (`feature/dataproc`)
  - **Deliverables:** סקריפטי `gcloud dataproc jobs submit`, מדידות scaling.
  - **תלוי ב:** `SPARK_ENV=cloud` + `DATA_BASE=gs://...` (כבר נתמך בקוד).
  - **לא לשנות:** לוגיקה עסקית — רק config/פריסה.

- **Evaluation / Presentation / Defense** (`docs/presentation`)
  - **Deliverables:** טבלאות השוואה A/B/C, exact-vs-approx curves, מצגת.
  - **תלוי ב:** דוחות `outputs/statistics/*` מכל השיטות.

---

## 8. כללי עבודה משותפת ב-Git (Collaboration Rules)

- **אין לעבוד ישירות על `Noya`.** תמיד branch נפרד.
- **branch מ-`Noya` המעודכן** (`git checkout Noya && git pull` לפני).
- **commits קטנים ולוגיים** עם הודעות ברורות (`feat:`, `fix:`, `docs:`).
- **לעולם לא** לעשות commit ל-datasets / parquet / logs / `.venv`.
- **pull/rebase לפני PR** כדי למנוע קונפליקטים.
- **PR חזרה ל-`Noya`**, עם **תוצאות validation ב-description**.
- **אין לשנות `config.py` המשותף** בלי תיאום (זה משפיע על כולם).

---

## 9. מה עוד חסר (Remaining Roadmap)

- Method A (clustering) — טרם מומש.
- Method C (transition graph) + activity-zone analysis — טרם מומש.
- ניתוח מסלולים חריגים (anomaly routes) מעבר לדגלים ב-Phase 2.
- **הרצות מלאות מקומית** (`--full`) — טרם בוצעו (עד כה sample בלבד).
- **DataProc עם 5+ מכונות** + **GCS** — טרם.
- השוואת performance מלאה (A vs B vs C) + approximate-vs-exact curves.
- H3 sensitivity study על דאטה גדולה יותר.
- מפות/ויזואליזציה.
- מצגת סופית + הכנה ל-defense.

---

## 10. מגבלות נוכחיות (Current Limitations) — שקיפות מלאה

- **validation על sample בלבד** (5,000 נסיעות) — עוד לא הורץ full.
- **support דליל למסלולים ארוכים** ב-sample (≥10 ק"מ → support ~1); דורש full.
- **workarounds ספציפיים ל-Windows** (non-ASCII path, winutils) — לא רלוונטיים
  ב-DataProc (Linux) אבל חשוב שכל חבר צוות על Windows יגדיר אותם.
- **H3 discretization tradeoff** — res 9 הוא פשרה; highways עלולים לאבד דיוק.
- **Exact mining shuffle cost** — 1.09M window rows; יגדל לינארית ב-full (זו
  בדיוק הסיבה ל-M7).
- **מגבלות קירוב** — precision יורד ב-tail; support-1 ties לא משמעותיים ב-sample.
- **טרם נבדק על DataProc** בכלל.

---

## 11. פעולות מיידיות לצוות (Immediate Next Actions)

1. **כולם:** לשכפל, `git checkout Noya`, להקים venv, `python -m src.validate_env`,
   ולהריץ את ה-sample pipeline מקצה-לקצה כדי לוודא שהסביבה עובדת.
2. **אחראי/ת Method A:** לפתוח `feature/clustering` ולהתחיל ב-MinHash-LSH על
   bigram shingles (ראו DESIGN_REVIEW §4).
3. **אחראי/ת Method C:** לפתוח `feature/graph-mining` ולבנות את גרף המעברים +
   PageRank ל-activity zones.
4. **אחראי/ת Visualization:** לפתוח `feature/visualization` ולצייר את ה-top
   routes של M5/M6 על מפה (Folium) + heatmap של תאי H3.
5. **אחראי/ת DataProc:** להכין bucket ב-GCS, ולנסח `gcloud dataproc` submit
   לשלב אחד (למשל clean_data) כ-proof-of-concept — בזהירות עם התקציב ($50).
6. **אחראי/ת Evaluation:** להתחיל שלד מצגת + טבלת השוואת השיטות.

בהצלחה — כל השאלות/החלטות הארכיטקטוניות מתועדות ב-`docs/ARCHITECTURE.md` ו-
`docs/DESIGN_REVIEW.md`.
