"""Render site/assets/og-card.svg to site/assets/og-card.png at 1200x630.

The canonical SVG is the editable source. The PNG is what social-media
platforms reliably render in link previews (Facebook, LinkedIn, and some
Twitter card configs don't reliably handle SVG previews).

Two backends are tried in order:

1. ``cairosvg`` — pure-Python (well, libcairo-bound) SVG → PNG renderer.
   Best fidelity for SVGs that use only the subset CairoSVG supports.
2. ``Pillow`` + ``svglib`` — fallback that renders SVG via reportlab to PIL.

If neither is available, the script prints clear instructions and exits 1
without generating a partial output. The committed PNG remains valid until
explicitly regenerated.

Usage::

    python scripts/render_og_card.py
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SVG_PATH = REPO_ROOT / "site" / "assets" / "og-card.svg"
PNG_PATH = REPO_ROOT / "site" / "assets" / "og-card.png"


def render_with_cairosvg() -> bool:
    try:
        import cairosvg  # type: ignore
    except ImportError:
        return False
    cairosvg.svg2png(
        url=str(SVG_PATH),
        write_to=str(PNG_PATH),
        output_width=1200,
        output_height=630,
    )
    return True


def render_with_svglib() -> bool:
    try:
        from svglib.svglib import svg2rlg  # type: ignore
        from reportlab.graphics import renderPM  # type: ignore
    except ImportError:
        return False
    drawing = svg2rlg(str(SVG_PATH))
    # svglib reads viewBox; renderPM emits at native dimensions. We want
    # 1200x630 explicitly, so scale if needed.
    if drawing.width and drawing.height:
        scale_x = 1200 / drawing.width
        scale_y = 630 / drawing.height
        drawing.scale(scale_x, scale_y)
        drawing.width = 1200
        drawing.height = 630
    renderPM.drawToFile(drawing, str(PNG_PATH), fmt="PNG")
    return True


def main() -> int:
    if not SVG_PATH.exists():
        print(f"ERROR: SVG source missing: {SVG_PATH}")
        return 1

    for name, fn in (("cairosvg", render_with_cairosvg), ("svglib", render_with_svglib)):
        if fn():
            print(f"render_og_card: wrote {PNG_PATH} via {name}")
            return 0

    print(
        "ERROR: no SVG-to-PNG renderer available. Install one of:\n"
        "    pip install cairosvg        # recommended\n"
        "    pip install svglib reportlab  # fallback\n"
        "Or edit site/assets/og-card.svg in any vector tool (Inkscape, Affinity,\n"
        "Figma) and export the result manually as site/assets/og-card.png."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
