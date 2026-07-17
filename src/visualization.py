"""
visualization.py  --  Milestone M15  (map of results)
=====================================================
Render the discovered routes, activity zones and anomalies onto an interactive
Folium (Leaflet) map. We plot only the TOP-N routes + activity zones (never raw
trajectories) so the browser stays responsive (DESIGN_REVIEW viz note).

Layers (toggleable):
  * Method B (maximal-frequent) top routes   -- blue
  * Method C (transition-graph) top routes   -- red
  * Method A (clustering) representatives     -- green
  * Activity zones (PageRank)                 -- orange circles, sized by rank
  * Anomalous routes (start points)           -- purple markers

Output: outputs/maps/porto_map_<suffix>.html  (git-ignored, self-contained).

Run:
    python -m src.visualization --sample
"""
import argparse
import csv
import os

import folium

from src import config

DELIM = ">"
PORTO_CENTER = (41.157, -8.629)


def _h3_line(cells):
    import h3
    return [list(h3.h3_to_geo(c)) for c in cells if c]


def _read(path):
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _add_routes(m, rows, route_col, pop_col, color, name, top_n=25, min_len=3):
    fg = folium.FeatureGroup(name=name, show=(name.startswith("B")))
    seen = 0
    for r in rows:
        if int(r.get("min_len_km", 0)) != min_len:
            continue
        pts = _h3_line(str(r[route_col]).split(DELIM))
        if len(pts) < 2:
            continue
        folium.PolyLine(pts, color=color, weight=3, opacity=0.7,
                        tooltip=f"{name} pop={r[pop_col]} len={r.get('length_km', r.get('rep_len_km',''))}km"
                        ).add_to(fg)
        seen += 1
        if seen >= top_n:
            break
    fg.add_to(m)


def main(use_sample: bool) -> None:
    suffix = "sample" if use_sample else "full"
    rd = os.path.join(config.OUTPUT_BASE, "routes")
    m = folium.Map(location=PORTO_CENTER, zoom_start=13, tiles="cartodbpositron")

    _add_routes(m, _read(os.path.join(rd, f"maximal_frequent_top100_{suffix}.csv")),
                "subroute", "support", "#1f5fbf", "B maximal-frequent")
    _add_routes(m, _read(os.path.join(rd, f"graph_heavy_paths_top100_{suffix}.csv")),
                "route", "support", "#d1341c", "C transition-graph")
    _add_routes(m, _read(os.path.join(rd, f"clustering_top100_{suffix}.csv")),
                "rep_route", "cluster_size", "#2e8b3d", "A clustering", min_len=5)

    # activity zones -> orange circles sized by rank (bigger = more important)
    zfg = folium.FeatureGroup(name="Activity zones (PageRank)", show=True)
    zones = _read(os.path.join(rd, f"activity_zones_{suffix}.csv"))
    for z in zones:
        folium.CircleMarker(
            location=(float(z["lat"]), float(z["lon"])),
            radius=max(3, 14 - int(z["rank"]) // 4), color="#e8850c",
            fill=True, fill_opacity=0.5,
            tooltip=f"zone #{z['rank']} pagerank={z['pagerank']}").add_to(zfg)
    zfg.add_to(m)

    # anomalies -> purple markers at start points
    afg = folium.FeatureGroup(name="Anomalous routes", show=False)
    for a in _read(os.path.join(rd, f"anomalies_top50_{suffix}.csv")):
        try:
            folium.CircleMarker(
                location=(float(a["start_lat"]), float(a["start_lon"])),
                radius=5, color="#7b2fbf", fill=True, fill_opacity=0.7,
                tooltip=f"anomaly score={a['anomaly_score']} dist={a['dist_km']}km"
                ).add_to(afg)
        except (KeyError, ValueError):
            continue
    afg.add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)
    title = ("<h4 style='position:fixed;top:8px;left:50px;z-index:9999;"
             "background:white;padding:4px 8px;border-radius:4px'>"
             f"Porto Taxi — popular routes / zones / anomalies ({suffix})</h4>")
    m.get_root().html.add_child(folium.Element(title))

    out_dir = os.path.join(config.OUTPUT_BASE, "maps")
    os.makedirs(out_dir, exist_ok=True)
    out = os.path.join(out_dir, f"porto_map_{suffix}.html")
    m.save(out)
    print(f"[m15] wrote interactive map -> {out}")
    print("M15 VISUALIZATION COMPLETE.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--sample", action="store_true")
    g.add_argument("--full", action="store_true")
    args = ap.parse_args()
    main(use_sample=args.sample)
