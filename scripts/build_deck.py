"""
build_deck.py  --  the presentation
===================================
Builds `build/deck/Porto_Taxi_Routes.pptx` from the figures in
`build/deck/figs` (scripts/build_deck_charts.py) and the numbers this pipeline
actually produced.

DESIGN NOTES (why it does not look like a template)
---------------------------------------------------
* One dark surface, one accent, three categorical hues -- the validated set from
  the data-viz palette, checked with `validate_palette.js` on THIS surface.
* Nothing is centred. Every slide hangs off one left margin, so the eye lands in
  the same place on every slide and the content, not the layout, changes.
* Hierarchy comes from size and weight, not from boxes, shadows or icons.
* No bullet-point slides. A slide is either a statement, a number, a chart or a
  table. If a thought needs five bullets it needs five slides or a table.
* Numbers are the hero: the figure is set large, its unit small beside it, and
  the caption explains what it means rather than repeating it.
"""
import pathlib

from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.util import Emu, Inches, Pt

FIGS = pathlib.Path("build/deck/figs")
OUT = pathlib.Path("build/deck/Porto_Taxi_Routes.pptx")
OUT.parent.mkdir(parents=True, exist_ok=True)

# ---- ink -------------------------------------------------------------------
SURFACE = RGBColor(0x14, 0x16, 0x1A)
PLANE = RGBColor(0x0D, 0x0D, 0x0D)
INK = RGBColor(0xFF, 0xFF, 0xFF)
INK2 = RGBColor(0xC3, 0xC2, 0xB7)
MUTED = RGBColor(0x89, 0x87, 0x81)
RULE = RGBColor(0x2C, 0x2C, 0x2A)
S1 = RGBColor(0x39, 0x87, 0xE5)     # blue
S2 = RGBColor(0xD9, 0x59, 0x26)     # orange
S3 = RGBColor(0x19, 0x9E, 0x70)     # aqua
GOOD = RGBColor(0x0C, 0xA3, 0x0C)
CRIT = RGBColor(0xD0, 0x3B, 0x3B)

FONT = "Helvetica Neue"
MONO = "Menlo"

W, H = Inches(13.333), Inches(7.5)
ML = Inches(0.92)                    # the one left margin everything hangs off
MR = Inches(0.92)
CW = W - ML - MR                     # content width


def _txbox(slide, x, y, w, h):
    tb = slide.shapes.add_textbox(x, y, w, h)
    tf = tb.text_frame
    tf.word_wrap = True
    tf.margin_left = tf.margin_right = tf.margin_top = tf.margin_bottom = 0
    return tf


def _para(tf, text, size, color, bold=False, font=FONT, space_after=0,
          line=None, first=False, align=PP_ALIGN.LEFT):
    p = tf.paragraphs[0] if first else tf.add_paragraph()
    p.alignment = align
    if line:
        p.line_spacing = line
    p.space_after = Pt(space_after)
    r = p.add_run()
    r.text = text
    f = r.font
    f.size, f.bold, f.name = Pt(size), bold, font
    f.color.rgb = color
    return p


def _rect(slide, x, y, w, h, color):
    from pptx.enum.shapes import MSO_SHAPE
    sh = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, x, y, w, h)
    sh.fill.solid()
    sh.fill.fore_color.rgb = color
    sh.line.fill.background()
    sh.shadow.inherit = False
    return sh


def blank(prs, dark=SURFACE):
    s = prs.slides.add_slide(prs.slide_layouts[6])
    bg = s.background.fill
    bg.solid()
    bg.fore_color.rgb = dark
    return s


def chrome(slide, section, n):
    """Footer: section on the left, number on the right. Same place every slide."""
    tf = _txbox(slide, ML, H - Inches(0.62), CW, Inches(0.3))
    _para(tf, section.upper(), 10, MUTED, first=True)
    tf2 = _txbox(slide, W - MR - Inches(1.2), H - Inches(0.62), Inches(1.2), Inches(0.3))
    _para(tf2, str(n), 10, MUTED, first=True, align=PP_ALIGN.RIGHT)


def eyebrow(slide, text, y=Inches(0.72), color=S1):
    tf = _txbox(slide, ML, y, CW, Inches(0.26))
    _para(tf, text.upper(), 11, color, bold=True, first=True)
    _rect(slide, ML, y + Inches(0.34), Inches(0.62), Emu(22860), color)


def headline(slide, text, y=Inches(1.28), size=40, color=INK, w=None):
    tf = _txbox(slide, ML, y, w or CW, Inches(1.5))
    _para(tf, text, size, color, bold=True, line=1.02, first=True)


BOTTOM = H - Inches(0.86)          # keep clear of the footer chrome


def body(slide, text, y, size=17, color=INK2, w=None, line=1.34, x=None):
    # `x` matters: without it every right-hand column silently rendered at the
    # left margin, on top of the chart. Six slides were wrong before the linter
    # caught it.
    h = min(Inches(1.6), max(Inches(0.5), BOTTOM - y))   # never past the footer
    tf = _txbox(slide, x if x is not None else ML, y, w or CW, h)
    _para(tf, text, size, color, line=line, first=True)
    return tf


def picture(slide, name, x, y, w=None, h=None):
    """Place a figure, shrinking it if the requested width would run off the
    bottom. A chart that overlaps the footer looks like a mistake, because it is."""
    from PIL import Image
    path = FIGS / f"{name}.png"
    iw, ih = Image.open(path).size
    aspect = ih / iw
    if w and not h:
        if y + Emu(int(w * aspect)) > BOTTOM:
            h = BOTTOM - y
            w = Emu(int(h / aspect))
    elif h and not w:
        w = Emu(int(h / aspect))
    kw = {}
    if w: kw["width"] = w
    if h: kw["height"] = h
    return slide.shapes.add_picture(str(path), x, y, **kw)


def table(slide, headers, rows, x, y, w, col_w=None, size=13,
          head_color=MUTED, hi_rows=(), hi_color=None):
    """Hairline table: no fills, no borders except a rule under the header."""
    n = len(headers)
    col_w = col_w or [w / n] * n
    rh = Inches(0.375)
    # header
    cx = x
    for i, hcell in enumerate(headers):
        tf = _txbox(slide, cx, y, col_w[i], rh)
        _para(tf, str(hcell), size - 2, head_color, bold=True, first=True,
              align=PP_ALIGN.RIGHT if i else PP_ALIGN.LEFT)
        cx += col_w[i]
    _rect(slide, x, y + rh - Inches(0.06), w, Emu(12700), RULE)
    # rows
    for r, row in enumerate(rows):
        ry = y + rh + Inches(0.06) + r * rh
        cx = x
        colr = (hi_color or S1) if r in hi_rows else INK2
        for i, cell in enumerate(row):
            tf = _txbox(slide, cx, ry, col_w[i], rh)
            _para(tf, str(cell), size, INK if (i == 0 or r in hi_rows) else colr,
                  bold=(r in hi_rows), font=FONT, first=True,
                  align=PP_ALIGN.RIGHT if i else PP_ALIGN.LEFT)
            cx += col_w[i]
        if r < len(rows) - 1:
            _rect(slide, x, ry + rh - Inches(0.02), w, Emu(6350),
                  RGBColor(0x20, 0x22, 0x26))


def stat(slide, x, y, value, unit, caption, color=INK, vsize=54, w=Inches(3.5)):
    """A hero figure: big number, small unit, explanatory caption."""
    tf = _txbox(slide, x, y, w, Inches(0.95))
    p = tf.paragraphs[0]
    r = p.add_run(); r.text = value
    r.font.size, r.font.bold, r.font.name = Pt(vsize), True, FONT
    r.font.color.rgb = color
    if unit:
        r2 = p.add_run(); r2.text = "  " + unit
        r2.font.size, r2.font.name = Pt(16), FONT
        r2.font.color.rgb = MUTED
    cy = y + Inches(0.92)
    tf2 = _txbox(slide, x, cy, w, min(Inches(2.0), max(Inches(0.5), BOTTOM - cy)))
    _para(tf2, caption, 13, INK2, line=1.3, first=True)


# ============================================================== the slides ===
def build():
    prs = Presentation()
    prs.slide_width, prs.slide_height = W, H
    n = [0]

    def S(section, dark=SURFACE):
        n[0] += 1
        s = blank(prs, dark)
        chrome(s, section, n[0])
        return s

    # ---------------------------------------------------------------- 1 title
    s = blank(prs, PLANE)
    _rect(s, 0, 0, Inches(0.14), H, S1)
    tf = _txbox(s, ML, Inches(2.05), Inches(10.5), Inches(0.4))
    _para(tf, "BIG DATA & CLOUD COMPUTING — FINAL PROJECT", 12, S1, bold=True, first=True)
    tf = _txbox(s, ML, Inches(2.62), Inches(11), Inches(2.4))
    _para(tf, "Popular long sub-routes", 54, INK, bold=True, line=1.0, first=True)
    _para(tf, "in 1.7 million Porto taxi trajectories", 54, MUTED, bold=True, line=1.0)
    _rect(s, ML, Inches(5.16), Inches(1.5), Emu(28575), S1)
    tf = _txbox(s, ML, Inches(5.52), Inches(11), Inches(1.2))
    _para(tf, "Four independent mining methods · exact and approximate · "
              "executed on a 6-machine DataProc cluster", 15, INK2, line=1.4, first=True)
    tf = _txbox(s, ML, H - Inches(1.0), Inches(11), Inches(0.5))
    _para(tf, "1,710,670 trips   ·   442 taxis   ·   2013–2014   ·   GPS every 15 s",
          12, MUTED, font=MONO, first=True)

    # ------------------------------------------------------------- 2 the task
    s = S("The problem")
    eyebrow(s, "What was asked")
    headline(s, "Find the sub-routes that many taxis\nactually share — and the long ones.", size=34)
    body(s, "A sub-route is a contiguous piece of a journey. It is POPULAR if enough "
            "distinct trips contain it, and LONG if its ground length clears a "
            "threshold. The deliverable is the top 100 at each of six length "
            "configurations, found by four independent methods so the answers can be "
            "cross-checked.", Inches(3.0), w=Inches(7.3))
    xs = ML + Inches(7.9)
    tf = _txbox(s, xs, Inches(3.0), Inches(3.6), Inches(3.0))
    _para(tf, "LENGTH CONFIGURATIONS", 11, MUTED, bold=True, first=True, space_after=10)
    for L in ("≥ 1 km", "≥ 3 km", "≥ 5 km", "≥ 10 km", "≥ 20 km", "≥ 40 km"):
        _para(tf, L, 19, INK, bold=True, space_after=5)

    # -------------------------------------------------------- 3 THE ONE IDEA
    s = S("Representation")
    eyebrow(s, "The one idea everything rests on", color=S3)
    headline(s, "A trip is a string.", size=48)
    body(s, "Snap every GPS point to an H3 hexagon at resolution 9 (~174 m edge) and "
            "collapse repeats. A trajectory becomes a word over a cell alphabet — and "
            "the whole problem becomes classical string processing.", Inches(2.75),
         w=Inches(6.6))
    tf = _txbox(s, ML, Inches(4.35), Inches(6.6), Inches(1.6))
    _para(tf, "8939220f027 › 023 › 02f › 02b › 067 …", 15, S3, font=MONO, first=True,
          space_after=14)
    xs = ML + Inches(7.2)
    tf = _txbox(s, xs, Inches(2.75), Inches(4.3), Inches(3.4))
    for a, b in (("sub-route", "contiguous SUBSTRING"),
                 ("popular", "SUPPORT — distinct trips, and distinct taxis"),
                 ("long", "ground length ≥ L km")):
        _para(tf, a, 20, INK, bold=True, first=(a == "sub-route"), space_after=2)
        _para(tf, b, 14, INK2, space_after=16)
    body(s, "One representation feeds all four methods. That is what makes the "
            "cross-method comparison mean anything.", Inches(6.05), size=14, color=MUTED)

    # ------------------------------------------------------------ 4 the data
    s = S("The data")
    eyebrow(s, "From raw file to mineable corpus")
    headline(s, "96,162 trips are thrown away —\nand that is the point.", size=34)
    picture(s, "funnel", ML, Inches(3.05), w=Inches(7.5))
    xs = ML + Inches(8.0)
    stat(s, xs, Inches(3.0), "5.6", "% discarded",
         "Vendor-flagged, too few points, outside the Porto box, duplicate IDs — "
         "then trajectory-level corruption.", color=S2, vsize=44, w=Inches(3.5))

    # ------------------------------------------------------- 5 THE GUARD
    s = S("The guard")
    eyebrow(s, "Why corrupt trips are excluded, not flagged", color=S2)
    headline(s, "A GPS gap invents a route\nthat was never driven.", size=34)
    body(s, "Sub-route length is measured between cell centres. A window spanning a "
            "GPS dropout therefore reports the width of the GAP as route length — and "
            "those fabricated routes are long, so they land straight in the graded "
            "lists.", Inches(2.95), w=Inches(6.3))
    xs = ML + Inches(7.0)
    tf = _txbox(s, xs, Inches(2.7), Inches(4.6), Inches(0.4))
    _para(tf, "MEASURED WITHOUT THE GUARD", 11, MUTED, bold=True, first=True)
    stat(s, xs, Inches(3.15), "53.6", "km per cell-hop", "45× the physical bound.",
         color=CRIT, vsize=40, w=Inches(4.6))
    stat(s, xs, Inches(4.85), "563", "false windows ≥10 km",
         "One claimed 59.8 km from 19 cells.", color=CRIT, vsize=40, w=Inches(4.6))
    tf = _txbox(s, ML, Inches(5.3), Inches(6.3), Inches(1.4))
    _para(tf, "AFTER THE GUARD", 11, MUTED, bold=True, first=True, space_after=6)
    _para(tf, "worst hop 1.10 km   ·   0 windows across gaps", 20, GOOD, bold=True)
    _para(tf, "The 1.18 km limit is DERIVED — retained-speed bound plus cell "
              "quantisation — not tuned until the output looked nice.", 13, MUTED,
          space_after=0)

    # --------------------------------------------------------- 6 the methods
    s = S("Methods")
    eyebrow(s, "Four independent lenses on one representation")
    headline(s, "Four methods, one alphabet", size=36)
    table(s, ["", "Method", "Approach", "Scales?"],
          [["A", "Clustering", "MinHash-LSH → star clustering → longest shared run", "yes"],
           ["B", "Maximal-frequent", "n-gram support table → maximal patterns", "sample only"],
           ["C", "Transition graph", "PageRank zones → dominant-flow heavy paths", "yes"],
           ["D", "Suffix array", "generalised suffix array + LCP intervals — EXACT", "carries it"]],
          ML, Inches(2.65), CW,
          col_w=[Inches(0.5), Inches(2.5), Inches(6.0), Inches(2.5)], hi_rows=(3,))
    body(s, "B shares an O(n²) window table with the exact baselines. Measured at "
            "200k it OOMs and spills 21 GB without finishing; D produces the same "
            "maximal-frequent answer in one pass, with zero support disagreements at "
            "sample scale. So B is demonstrated, not scaled — and the report says so.",
         Inches(5.05), size=14, color=MUTED)

    # ================================================== THE DELIVERABLE ======
    s = S("Results — the deliverable", PLANE)
    eyebrow(s, "The answer")
    headline(s, "Top-100 popular sub-routes,\nat six length configurations", size=36)
    picture(s, "deliverable", ML, Inches(2.95), w=Inches(11.4))

    s = S("Results — the deliverable")
    eyebrow(s, "Every configuration, with its calibrated support floor")
    headline(s, "The support floor is calibrated\nper length — the tightest that still fills", size=30)
    table(s, ["min length", "floor (trips)", "= X%", "#maximal", "top support", "taxis", "longest"],
          [["≥ 1 km", "5,000", "0.3097%", "639", "14,330", "435", "4.70 km"],
           ["≥ 3 km", "2,500", "0.1548%", "292", "4,701", "435", "6.17 km"],
           ["≥ 5 km", "1,000", "0.0619%", "195", "1,945", "369", "8.72 km"],
           ["≥ 10 km", "50", "0.0031%", "217", "107", "84", "12.41 km"],
           ["≥ 20 km", "2", "0.0001%", "20", "2", "1", "26.25 km"],
           ["≥ 40 km", "2", "0.0001%", "0", "—", "—", "empty"]],
          ML, Inches(3.15), CW,
          col_w=[Inches(1.7), Inches(1.8), Inches(1.5), Inches(1.5), Inches(1.9),
                 Inches(1.2), Inches(1.9)], hi_rows=(4, 5), hi_color=S2)
    body(s, "Floors are ABSOLUTE, not percentages. A 0.01% floor is 2 trips on a "
            "sample and 171 at 1.71M — so a percentage makes the long bands empty out "
            "as the dataset grows, which is exactly backwards.", Inches(6.05),
         size=13, color=MUTED)

    # --------------------------------------------------------- where they are
    s = S("Results — where")
    eyebrow(s, "The corridors are recognisable places")
    headline(s, "Centre, the Matosinhos axis,\nand the airport", size=36)
    table(s, ["band", "trips", "taxis", "corridor"],
          [["≥ 1 km", "14,330", "435", "inside the centre, around São Bento"],
           ["≥ 3 km", "4,701", "435", "Boavista → Matosinhos"],
           ["≥ 5 km", "1,945", "369", "Boavista → Matosinhos"],
           ["≥ 10 km", "107", "84", "Hospital S. João → Sá Carneiro airport"],
           ["≥ 20 km", "2", "1", "Matosinhos → Hospital S. João"]],
          ML, Inches(3.1), CW,
          col_w=[Inches(1.7), Inches(1.7), Inches(1.4), Inches(6.7)], hi_rows=(4,),
          hi_color=S2)
    body(s, "No landmark was supplied to the pipeline. These fall out of the mining, "
            "which is the strongest sanity check available: for a taxi fleet, the "
            "airport and the western arterial SHOULD win.", Inches(5.8), size=14,
         color=MUTED)

    # ------------------------------------------------- taxi diversity (KEY)
    s = S("Results — is it real?")
    eyebrow(s, "Popular route, or one driver's habit?", color=S3)
    headline(s, "435 of 442 taxis drive\nthe top corridors.", size=38)
    picture(s, "diversity", ML, Inches(3.1), w=Inches(7.2))
    xs = ML + Inches(7.8)
    stat(s, xs, Inches(3.05), "98%", "of the fleet",
         "Support counts distinct TRIPS. With only 442 vehicles over a year, a "
         "corridor driven 200 times by one commuter would look identical. The "
         "distinct-taxi count is what separates them.", color=S3, vsize=48, w=Inches(3.7))

    # -------------------------------------------------- the honest degradation
    s = S("Results — the honest reading", PLANE)
    eyebrow(s, "Where the answer stops being trustworthy", color=S2)
    headline(s, "Length and confidence move\nin opposite directions.", size=38)
    xs = ML
    stat(s, xs, Inches(3.3), "20", "routes at ≥20 km",
         "…and every one has ≤ 2 distinct taxis.", color=S2, vsize=50, w=Inches(3.6))
    stat(s, xs + Inches(4.0), Inches(3.3), "1", "taxi, 2 trips",
         "The longest corridor, 26.25 km, is one driver's route driven twice — "
         "not something Porto uses.", color=CRIT, vsize=50, w=Inches(3.6))
    stat(s, xs + Inches(8.0), Inches(3.3), "0", "routes at ≥40 km",
         "Nothing that long repeats, at ANY floor down to 2 trips.", color=MUTED,
         vsize=50, w=Inches(3.6))
    body(s, "This is a finding, not a gap — and it is why the deliverable carries a "
            "distinct-taxi column instead of a single support number.", Inches(6.1),
         size=15, color=INK2)

    # ------------------------------------------------------ the scaling law
    s = S("Results — scaling")
    eyebrow(s, "Predicted before it was measured")
    headline(s, "A power law fitted on 5k–755k\ncalled the 1.71M ceiling", size=32)
    picture(s, "scaling", ML, Inches(2.95), w=Inches(7.6))
    xs = ML + Inches(8.1)
    stat(s, xs, Inches(3.1), "26.25", "km measured",
         "Predicted 26–28 km at 1.71M from four smaller scales, R² ≈ 0.98. The "
         "same fit said ≥20 km would populate and ≥40 km would not. Both correct.",
         color=S1, vsize=42, w=Inches(3.6))

    # -------------------------------------------------------- floor sweep (X)
    s = S("Results — choosing X")
    eyebrow(s, "The brief asks you to experiment with X")
    headline(s, "Strictness buys confidence\nand costs length", size=34)
    picture(s, "floor_sweep", ML, Inches(2.9), w=Inches(7.5))
    xs = ML + Inches(8.0)
    body(s, "At 5,000 trips of support the longest corridor is 5.80 km — a route you "
            "could stake money on. At 2 trips it is 26.25 km, and it means almost "
            "nothing.\n\nThere is no single right X. Reporting the curve is the honest "
            "answer; picking one number and hiding the trade is not.", Inches(3.05),
         size=14, w=Inches(3.6), x=xs)

    # ------------------------------------------------------- activity zones
    s = S("Results — activity zones")
    eyebrow(s, "PageRank over the cell-transition graph")
    headline(s, "The airport dominates —\nand nobody told the algorithm", size=34)
    table(s, ["rank", "where", "traversals", "taxis", "trips / taxi"],
          [["1", "Sá Carneiro airport", "72,048", "438", "164.5"],
           ["3", "Sá Carneiro airport", "67,733", "438", "154.6"],
           ["4", "Maia", "68,667", "438", "156.8"],
           ["5", "Sá Carneiro airport", "66,208", "438", "151.2"],
           ["6", "Campanhã station", "42", "29", "1.4"]],
          ML, Inches(3.05), CW,
          col_w=[Inches(1.1), Inches(4.6), Inches(2.4), Inches(1.6), Inches(1.8)],
          hi_rows=(0,))
    body(s, "Six of the top eight cells sit around the airport, each touched by 438 of "
            "442 taxis. `trips / taxi` separates a public hotspot (164 trips per "
            "vehicle, whole fleet) from incidental traffic (1.4 trips, 29 vehicles).",
         Inches(5.65), size=14, color=MUTED)

    # ----------------------------------------------------------- anomalies
    s = S("Results — anomalies")
    eyebrow(s, "Anomalous routes, as the brief requires")
    headline(s, "4.28% of cleaned trips are\nphysically implausible", size=34)
    picture(s, "anomalies", ML, Inches(3.0), w=Inches(7.5))
    xs = ML + Inches(8.0)
    body(s, "Six detectors, scored 0–4. The worst offender records 8,940 km/h and a "
            "single 147,345 km segment.\n\nThese are excluded before encoding, not "
            "merely labelled — a corrupt trajectory does not just produce a bad row, "
            "it manufactures long fake corridors.", Inches(3.05), size=14, w=Inches(3.6), x=xs)

    # ------------------------------------------------- cross-method agreement
    s = S("Results — do the methods agree?", PLANE)
    eyebrow(s, "The key scientific question")
    headline(s, "Two methods that fail differently\nfound the same corridors", size=34)
    picture(s, "agreement", ML, Inches(3.0), h=Inches(3.6))
    xs = ML + Inches(5.6)
    stat(s, xs, Inches(3.0), "0.94", "A → D agreement",
         "94% of clustering's corridors have a Jaccard ≥ 0.5 partner in the suffix "
         "array's. A randomised LSH method and an exact string algorithm converging "
         "is the strongest evidence the corridors are real.", color=S1, vsize=46,
         w=Inches(6.0))
    body(s, "C agrees least (0.39–0.47). Not a defect — it optimises flow through a "
            "graph rather than substring support, so it is the lens that disagrees, "
            "and that is worth knowing.", Inches(5.55), size=14, color=MUTED,
         w=Inches(6.0), x=xs)
    # keep the caveat visible: top_support is not comparable across methods
    tf = _txbox(s, xs, Inches(6.15), Inches(6.0), Inches(0.8))
    _para(tf, "Support totals are NOT comparable across methods: B and D report only "
              "MAXIMAL sub-routes, A and C do not.", 12, S2, first=True, line=1.3)

    # ---------------------------------------------------------- generalisation
    s = S("Results — generalisation")
    eyebrow(s, "The only check against unseen data", color=S3)
    headline(s, "The corridors are not memorised", size=36)
    picture(s, "holdout", ML, Inches(2.9), w=Inches(7.4))
    xs = ML + Inches(7.9)
    body(s, "Corridors mined from training data, tested against 318 trips the "
            "pipeline never saw.\n\nThe null is random walks over the HELD-OUT city's "
            "own road adjacency, length-matched — plausible routes that simply were "
            "not mined as popular. Uniform-random cells would be disconnected and "
            "would inflate the lift into meaninglessness.", Inches(3.0), size=13,
         w=Inches(3.7), x=xs)

    # -------------------------------------------------------------- temporal
    s = S("Results — time")
    eyebrow(s, "Does 'popular' depend on the hour?")
    headline(s, "Barely. The road network decides,\nnot the clock.", size=34)
    picture(s, "temporal", ML, Inches(2.95), w=Inches(7.5))
    xs = ML + Inches(8.0)
    stat(s, xs, Inches(3.0), "0.81", "mean overlap",
         "The same stretches dominate at 03:00 and at 08:00, so the all-time "
         "top-100 is a fair summary rather than an artefact of averaging. Night is "
         "least typical (0.75), midday most (0.89).", color=S1, vsize=44, w=Inches(3.6))

    # ====================================== APPROXIMATE VS EXACT (2 slides) ==
    s = S("Approximate vs exact", PLANE)
    eyebrow(s, "Space-Saving & Count-Min against the exact groupBy", color=S2)
    headline(s, "Sketches are perfect where it is easy\nand useless where it matters", size=32)
    picture(s, "sketch_accuracy", ML, Inches(3.05), w=Inches(11.4))

    s = S("Approximate vs exact")
    eyebrow(s, "So why use them at all?")
    headline(s, "The win is memory — and it\ngrows with the data", size=34)
    picture(s, "sketch_memory", ML, Inches(3.0), w=Inches(6.4))
    xs = ML + Inches(7.0)
    body(s, "At 200k trips: 66 MB of fixed sketch capacity against a 6,987 MB exact "
            "key table (14.6M keys). Runtime is a wash — 86.5 s vs 89.0 s. The "
            "sketch does not save time, it saves the table.", Inches(3.0), size=15,
         w=Inches(4.6), x=xs)
    tf = _txbox(s, xs, Inches(4.5), Inches(4.6), Inches(2.0))
    _para(tf, "WHY THE ≥10 km COLLAPSE NEVER HEALS", 11, S2, bold=True, first=True,
          space_after=8)
    _para(tf, "Space-Saving retains HEAVY HITTERS. A corridor is long because few "
              "trips repeat it, so long corridors sit in the tail by construction. "
              "More data adds more short corridors competing for the same slots — so "
              "recall fell 0.17 → 0.01 as the data grew 40×.", 13, INK2, line=1.35)

    # ------------------------------------------------------ HLL negative result
    s = S("A measured negative result")
    eyebrow(s, "HyperLogLog on distinct taxis per cell", color=S2)
    headline(s, "We were wrong about why\na sketch would pay", size=36)
    picture(s, "hll", ML, Inches(2.95), w=Inches(6.8))
    xs = ML + Inches(7.4)
    body(s, "The argument was “~85M (cell, taxi) pairs”. That is the INPUT size, and "
            "it is the wrong quantity.\n\nWhat decides a distinct-count sketch is "
            "CARDINALITY PER GROUP — and the fleet is 442 taxis, so no cell can ever "
            "exceed 442 distinct values. An exact set of ≤442 ids is trivial; the "
            "sketch's register array is pure overhead at every scale.", Inches(2.9),
         size=14, w=Inches(4.3), x=xs)
    tf = _txbox(s, xs, Inches(5.5), Inches(4.3), Inches(1.0))
    _para(tf, "Kept in the report because a negative result measured is worth more "
              "than a positive one assumed. HLL would pay on distinct PASSENGERS — "
              "an unbounded group.", 13, MUTED, line=1.35, first=True)

    # ------------------------------------------------------- grid choice
    s = S("Design decisions")
    eyebrow(s, "Choosing the spatial grid")
    headline(s, "Resolution 9 is a compromise,\nand we say so", size=34)
    table(s, ["grid", "res", "cell (m)", "avg cells/trip", "distinct cells", "bearing entropy"],
          [["H3", "8", "461", "7.8", "416", "1.176"],
           ["H3", "9", "174", "17.2", "1,853", "0.939"],
           ["H3", "10", "66", "29.6", "6,821", "0.749"],
           ["geohash", "6", "610", "9.3", "571", "1.016"],
           ["geohash", "7", "76", "29.4", "6,638", "0.730"]],
          ML, Inches(3.0), CW,
          col_w=[Inches(1.7), Inches(1.2), Inches(1.6), Inches(2.6), Inches(2.4),
                 Inches(2.0)], hi_rows=(1,))
    body(s, "Bearing entropy measures conflation — how many travel directions leave a "
            "cell. Res 10 conflates less (0.749) but costs 3.7× the alphabet and 1.7× "
            "the sequence length, and sub-route keys are SEQUENCES, so that multiplies "
            "superlinearly. H3 beats geohash structurally, not numerically: six "
            "equidistant neighbours mean a uniform step cost.", Inches(5.5), size=13,
         color=MUTED)

    # ========================================================= CLOUD =========
    s = S("Cloud execution", PLANE)
    eyebrow(s, "The brief requires ≥5 machines reading cloud storage")
    headline(s, "The same code, unchanged,\non a 6-machine DataProc cluster", size=34)
    xs = ML
    stat(s, xs, Inches(3.35), "6", "machines",
         "1 master + 5 workers, n2-standard-4, europe-west1.", color=S1, vsize=52,
         w=Inches(3.4))
    stat(s, xs + Inches(3.8), Inches(3.35), "1.71M", "trips",
         "Every input read from and every output written to gs:// cloud storage. "
         "Nothing touched a local disk.", color=S1, vsize=52, w=Inches(3.4))
    stat(s, xs + Inches(7.6), Inches(3.35), "$2.71", "total spend",
         "$1.77 for the graded run; $0.94 for the rehearsals that found the bugs.",
         color=S3, vsize=52, w=Inches(3.6))
    body(s, "`config.dataset_paths()` builds every path and `storage.py` is the only "
            "I/O boundary — so `file://` locally and `gs://` on the cluster are the "
            "same code path.", Inches(6.1), size=14, color=MUTED)

    s = S("Cloud execution")
    eyebrow(s, "How do we know the cluster got it right?", color=S3)
    headline(s, "Method D reproduced 420 corridors\nbit-for-bit", size=34)
    tf = _txbox(s, ML, Inches(3.0), Inches(7.2), Inches(2.0))
    _para(tf, "trip count      cloud 1,614,508  =  local 1,614,508", 15, INK,
          font=MONO, first=True, space_after=10)
    _para(tf, "corridor set    cloud 420        =  local 420", 15, INK, font=MONO,
          space_after=10)
    _para(tf, "supports        420 identical, 0 differing", 15, GOOD, font=MONO,
          space_after=10)
    _para(tf, "activity zones  50 / 50 identical, incl. PageRank floats", 15, GOOD,
          font=MONO)
    xs = ML + Inches(7.9)
    body(s, "The suffix array has no sampling, no seeds and no hash-order dependence, "
            "so its output is a function of the input alone.\n\nIdentical output "
            "across a different machine count, a different partitioning and a "
            "different filesystem is therefore evidence of identical INPUT — the "
            "strongest correctness claim available here.", Inches(2.95), size=13,
         w=Inches(3.7))
    body(s, "Methods A and the M7 sketches differ slightly and are REPORTED rather "
            "than failed: A samples trips with unseeded MinHash-LSH, and sketch merge "
            "order is partition-dependent. Demanding equality there would manufacture "
            "false alarms.", Inches(5.55), size=13, color=MUTED, w=Inches(7.2))

    s = S("Cloud execution")
    eyebrow(s, "Distribution is not the same as speed")
    headline(s, "The cluster was SLOWER — and that\nis the honest result", size=32)
    picture(s, "cloud_local", ML, Inches(2.85), w=Inches(7.8))
    xs = ML + Inches(8.3)
    body(s, "60 min on 6 machines against 33 min on one laptop.\n\nAt 1.71M this "
            "problem still fits in a single machine's memory, so distribution buys "
            "fault tolerance and headroom, not speed — the coordination and "
            "shuffle-over-network costs are real and the dataset is not big enough to "
            "amortise them.", Inches(2.95), size=13, w=Inches(3.4), x=xs)
    tf = _txbox(s, xs, Inches(5.5), Inches(3.4), Inches(1.4))
    _para(tf, "M12 costs the SAME on both (409 s): one pass, already partitioned by "
              "3-cell prefix, so there is nothing to shuffle. The stages that got "
              "slower are exactly the ones that do.", 13, S1, line=1.35, first=True)

    # ------------------------------------------------------- what went wrong
    s = S("Engineering")
    eyebrow(s, "Four failures that local testing could not find", color=S2)
    headline(s, "The dangerous cloud bugs\nare the ones that still exit 0", size=32)
    table(s, ["symptom", "actual cause"],
          [["“Initialization action timed out”",
            "no route to PyPI — Errno 101. More time cannot reach an unreachable host."],
           ["NotADirectoryError on a temp dir",
            "the driver received NO environment: appMasterEnv is cluster-mode only"],
           ["3 VMs billing after a failure",
            "a failed cluster is NOT rolled back; the cleanup trap was armed too late"],
           ["temporal buckets disagreed",
            "hour-of-day rendered in the JVM's timezone — never Porto's"]],
          ML, Inches(2.95), CW, col_w=[Inches(4.6), Inches(6.9)], hi_rows=(1,),
          hi_color=S2)
    body(s, "The second one is the one that matters. It crashed on a temp path — which "
            "was luck. The same missing environment left OUTPUT_BASE unset, so a run "
            "that got one line further would have written every result to a disk that "
            "is deleted with the cluster, and exited 0. `storage.py` cannot catch "
            "that: it would be correctly writing to a correctly-resolved local path.",
         Inches(5.35), size=13, color=INK2)

    # -------------------------------------------------------- limitations
    s = S("Limitations")
    eyebrow(s, "What we would not claim")
    headline(s, "Honest limitations", size=38)
    tf = _txbox(s, ML, Inches(2.75), Inches(11.4), Inches(4.0))
    items = [
        ("≥40 km is empty and ≥20 km is hollow",
         "Findings about Porto and the data volume — but they must not be presented "
         "as a top-100 list a reader will assume is meaningful."),
        ("Method B does not scale",
         "Demonstrated at sample scale only. D reproduces its output exactly, so "
         "nothing is lost — but the report must not imply B scales."),
        ("Method A clusters a capped 50,000-trip subset",
         "Support is measured on all trips, but cluster_size is a sample statistic, "
         "and route extents shift between samples even though the geography is stable."),
        ("The suffix array truncates at 200 cells (~60 km)",
         "Corridors longer than that are out of scope by construction."),
    ]
    first = True
    for a, b in items:
        _para(tf, a, 17, INK, bold=True, first=first, space_after=3)
        _para(tf, b, 13, INK2, line=1.3, space_after=15)
        first = False

    # ------------------------------------------------------------ conclusion
    s = blank(prs, PLANE)
    n[0] += 1
    chrome(s, "Conclusion", n[0])
    _rect(s, 0, 0, Inches(0.14), H, S3)
    eyebrow(s, "In one sentence", color=S3)
    tf = _txbox(s, ML, Inches(1.9), Inches(11.2), Inches(3.4))
    _para(tf, "Porto's taxi traffic is dominated by a handful of structural "
              "corridors — the centre, the Boavista–Matosinhos axis and the airport — "
              "that almost the whole fleet uses at every hour of the day.", 30, INK,
          bold=True, line=1.18, first=True, space_after=22)
    _para(tf, "Beyond about 12 km, “popular routes” stop existing. Past 26 km, "
              "nothing repeats at all.", 30, S3, bold=True, line=1.18)
    tf = _txbox(s, ML, Inches(5.7), Inches(11.2), Inches(1.0))
    _para(tf, "1,710,670 trips  ·  4 methods  ·  420 corridors reproduced exactly on "
              "a 6-machine cluster  ·  $2.71", 13, MUTED, font=MONO, first=True)

    prs.save(str(OUT))
    print(f"{OUT}  ({n[0]} slides)")


if __name__ == "__main__":
    build()
