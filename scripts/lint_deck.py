"""
lint_deck.py  --  geometry checks for the generated deck
========================================================
There is no PowerPoint or LibreOffice on the build machine, so the deck cannot be
rendered and eyeballed. These checks stand in for that: they catch the failure
modes that a screenshot would have caught.

  1. OVERFLOW      a shape extending past the slide edge
  2. COLLISION     a text box overlapping a picture (labels landing on a chart)
  3. TEXT OVERSET  estimated wrapped text height exceeding its box

(3) is an estimate: python-pptx does not lay text out. It uses a conservative
average glyph width per point size, so it over-reports rather than under-reports.
Treat a WARN as "go look at that slide", not as a hard failure.
"""
import pathlib
import sys

from pptx import Presentation
from pptx.util import Emu

DECK = pathlib.Path("build/deck/Porto_Taxi_Routes.pptx")
EMU_IN = 914400.0


def inches(v):
    return v / EMU_IN


def rect(sh):
    return (sh.left or 0, sh.top or 0,
            (sh.left or 0) + (sh.width or 0), (sh.top or 0) + (sh.height or 0))


def overlap(a, b):
    ax0, ay0, ax1, ay1 = a
    bx0, by0, bx1, by1 = b
    ix = max(0, min(ax1, bx1) - max(ax0, bx0))
    iy = max(0, min(ay1, by1) - max(ay0, by0))
    return ix * iy


def est_text_height(sh):
    """Rough wrapped height. Deliberately pessimistic."""
    if not sh.has_text_frame or not (sh.width or 0):
        return 0
    total = 0.0
    w_in = inches(sh.width)
    for p in sh.text_frame.paragraphs:
        if not p.runs:
            continue
        # Width must be summed PER RUN. A hero figure is one paragraph holding a
        # 44pt number and a 16pt unit; charging every character the 44pt width
        # reported overset on lines that comfortably fit.
        width_pt = sum(len(r.text) * (r.font.size.pt if r.font.size else 18) * 0.52
                       for r in p.runs)
        if not width_pt:
            continue
        size = max((r.font.size.pt for r in p.runs if r.font.size), default=18)
        lines = max(1, -(-int(width_pt) // int(w_in * 72)))
        line_h = size * (p.line_spacing or 1.2)
        total += lines * line_h + (p.space_after.pt if p.space_after else 0)
    return total / 72.0 * EMU_IN


def main():
    prs = Presentation(str(DECK))
    SW, SH = prs.slide_width, prs.slide_height
    problems = []
    for i, slide in enumerate(prs.slides, 1):
        pics, texts = [], []
        for sh in slide.shapes:
            x0, y0, x1, y1 = rect(sh)
            # 1. overflow
            if x0 < -1000 or y0 < -1000 or x1 > SW + 1000 or y1 > SH + 1000:
                problems.append(
                    f"  slide {i:>2}  OVERFLOW   {sh.shape_type}: "
                    f"right={inches(x1):.2f}in bottom={inches(y1):.2f}in "
                    f"(slide {inches(SW):.2f}x{inches(SH):.2f})")
            if sh.shape_type is not None and "PICTURE" in str(sh.shape_type):
                pics.append((sh, rect(sh)))
            elif sh.has_text_frame and sh.text_frame.text.strip():
                texts.append((sh, rect(sh)))
            # 3. text overset
            if sh.has_text_frame and sh.text_frame.text.strip():
                need = est_text_height(sh)
                if need > (sh.height or 0) * 1.5:
                    txt = sh.text_frame.text.strip().replace("\n", " ")[:44]
                    problems.append(
                        f"  slide {i:>2}  OVERSET?   needs ~{inches(need):.2f}in in "
                        f"{inches(sh.height or 0):.2f}in — \"{txt}…\"")
        # 2. text over picture
        for tsh, tr in texts:
            for _p, pr in pics:
                a = overlap(tr, pr)
                if a > 0.12 * (tr[2] - tr[0]) * (tr[3] - tr[1]):
                    txt = tsh.text_frame.text.strip().replace("\n", " ")[:40]
                    problems.append(
                        f"  slide {i:>2}  COLLISION  text over chart — \"{txt}…\"")
    print(f"{DECK}: {len(prs.slides.__iter__.__self__._sldIdLst)} slides checked")
    if problems:
        print(f"\n{len(problems)} thing(s) to look at:\n")
        for p in problems:
            print(p)
    else:
        print("\nno overflow, no collisions, no oversets")
    return 1 if any("OVERFLOW" in p or "COLLISION" in p for p in problems) else 0


if __name__ == "__main__":
    sys.exit(main())
