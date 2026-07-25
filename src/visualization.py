"""
visualization.py  --  map of results
====================================
Render the discovered routes, activity zones and anomalies onto an interactive
Folium (Leaflet) map. We plot only the TOP-N routes + activity zones (never raw
trajectories) so the browser stays responsive.

ALL SIX LENGTH CONFIGURATIONS ARE RENDERED, each as its own toggleable layer,
because the assignment asks for the top-100 at >=1/3/5/10/20/40 km and a map
showing only one of them does not demonstrate that. Open the layer control and
switch between them; the >=3 km layers start visible because that is the band
where every method has results.

Layers (toggleable), one per method per length:
  * A clustering       -- green
  * B maximal-frequent -- blue
  * C transition-graph -- red
  * D suffix-array     -- purple
  * Activity zones (PageRank)   -- orange circles, sized by rank
  * Anomalous routes (starts)   -- grey markers

Output: outputs/maps/porto_map_<scale>.html  (self-contained).

Run:
    python -m src.visualization --sample
"""
import folium

from src import cli, config, storage

DELIM = ">"
PORTO_CENTER = (41.157, -8.629)
DEFAULT_VISIBLE_L = 3        # the band where all methods have output

# method -> (file stem, route column, colour, label)
LAYERS = [
    ("B", "maximal_frequent_top100", "subroute", "#1f5fbf", "B maximal-frequent"),
    ("D", "suffix_array_top100", "subroute", "#7b2fbf", "D suffix-array"),
    ("C", "graph_heavy_paths_top100", "route", "#d1341c", "C transition-graph"),
    ("A", "clustering_top100", "subroute", "#2e8b3d", "A clustering"),
]

log = cli.setup_logging("m15")


def _h3_line(cells):
    import h3

    return [list(h3.h3_to_geo(c)) for c in cells if c]


def _add_routes(m, rows, route_col, colour, name, min_len, top_n=25):
    """One feature group per (method, length config)."""
    subset = [r for r in rows if int(r.get("min_len_km", 0)) == min_len][:top_n]
    if not subset:
        return 0
    fg = folium.FeatureGroup(name=f"{name} (>={min_len} km)",
                             show=(min_len == DEFAULT_VISIBLE_L))
    drawn = 0
    for r in subset:
        pts = _h3_line(str(r[route_col]).split(DELIM))
        if len(pts) < 2:
            continue
        folium.PolyLine(
            pts, color=colour, weight=3, opacity=0.7,
            tooltip=(f"{name} >={min_len}km | support={r.get('support', '?')} "
                     f"| {float(r.get('length_km', 0)):.2f} km")).add_to(fg)
        drawn += 1
    fg.add_to(m)
    return drawn


def main(scale: str) -> None:
    with cli.session_if_remote("visualization"):
        _main(scale)


def _main(scale: str) -> None:
    m = folium.Map(location=PORTO_CENTER, zoom_start=12, tiles="cartodbpositron")
    total = 0

    for _key, stem, rcol, colour, label in LAYERS:
        rows = storage.read_csv_rows(storage.out_path("routes", f"{stem}_{scale}.csv"))
        if not rows:
            log.warning("no routes for %s", label)
            continue
        for L in config.ROUTE_LENGTH_THRESHOLDS_KM:
            total += _add_routes(m, rows, rcol, colour, label, L)

    # activity zones -> orange circles sized by rank (bigger = more important)
    zones = storage.read_csv_rows(storage.out_path("routes", f"activity_zones_{scale}.csv"))
    if zones:
        zfg = folium.FeatureGroup(name="Activity zones (PageRank)", show=True)
        for z in zones:
            folium.CircleMarker(
                location=(float(z["lat"]), float(z["lon"])),
                radius=max(3, 14 - int(z["rank"]) // 4), color="#e8850c",
                fill=True, fill_opacity=0.5,
                tooltip=f"zone #{z['rank']} pagerank={z['pagerank']}").add_to(zfg)
        zfg.add_to(m)

    # anomalies -> grey markers at start points
    anomalies = storage.read_csv_rows(storage.out_path("routes", f"anomalies_top50_{scale}.csv"))
    if anomalies:
        afg = folium.FeatureGroup(name="Anomalous routes", show=False)
        for a in anomalies:
            try:
                folium.CircleMarker(
                    location=(float(a["start_lat"]), float(a["start_lon"])),
                    radius=5, color="#555555", fill=True, fill_opacity=0.7,
                    tooltip=(f"anomaly score={a['anomaly_score']} "
                             f"dist={a['dist_km']}km")).add_to(afg)
            except (KeyError, ValueError):
                continue
        afg.add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)
    title = ("<h4 style='position:fixed;top:8px;left:50px;z-index:9999;"
             "background:white;padding:4px 8px;border-radius:4px'>"
             f"Porto Taxi &mdash; popular sub-routes / zones / anomalies ({scale})"
             "<br><span style='font-weight:normal;font-size:11px'>"
             "toggle length configs in the layer control &rarr;</span></h4>")
    m.get_root().html.add_child(folium.Element(title))

    out = storage.out_path("maps", f"porto_map_{scale}.html")
    if storage.is_remote(out):
        storage.write_text(out, m.get_root().render())
    else:
        storage.makedirs(storage.out_path("maps"))
        m.save(out)
    log.info("wrote interactive map (%d route polylines) -> %s", total, out)
    log.info("VISUALIZATION COMPLETE.")


if __name__ == "__main__":
    args = cli.scale_parser(__doc__).parse_args()
    main(cli.scale_of(args))
