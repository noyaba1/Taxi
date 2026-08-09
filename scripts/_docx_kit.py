"""
_docx_kit.py  --  shared .docx formatting primitives
====================================================
Used by build_devdoc.py (the operational guide) and build_report.py (the
technical report), so the two documents cannot drift apart typographically.

`doc` is created by the caller and passed to new_doc() -- these helpers write
into whichever Document is current.
"""
import pathlib

from docx import Document
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Inches, Pt, RGBColor


INK = RGBColor(0x1A, 0x1A, 0x1A)
MUTED = RGBColor(0x5A, 0x5A, 0x5A)
ACCENT = RGBColor(0x1C, 0x5C, 0xAB)
WARN = RGBColor(0xB0, 0x3A, 0x1A)
GOOD = RGBColor(0x0A, 0x6B, 0x2A)
CODE_BG = "F2F3F5"
NOTE_BG = "EEF4FC"
WARN_BG = "FDF0EA"

doc = None


def new_doc():
    global doc
    doc = Document()
    _setup()
    return doc


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




def figure(path, caption, width=6.4):
    """A chart plus its caption. The caption states the finding, not the axes."""
    doc.add_picture(str(path), width=Inches(width))
    doc.paragraphs[-1].alignment = WD_ALIGN_PARAGRAPH.CENTER
    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p.paragraph_format.space_before = Pt(3)
    p.paragraph_format.space_after = Pt(12)
    r = p.add_run(caption)
    r.font.size, r.font.italic, r.font.color.rgb = Pt(9), True, MUTED
    return p
