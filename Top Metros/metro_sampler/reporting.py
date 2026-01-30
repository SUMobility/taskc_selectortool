"""
Output: CSV, summary report, and optional map visualization.
"""
import logging
from pathlib import Path
import pandas as pd

from metro_sampler.config import OUTPUT_DIR

log = logging.getLogger(__name__)


def save_sample_csv(sample: pd.DataFrame, filename: str = "msa_sample.csv") -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUTPUT_DIR / filename

    cols_order = [
        "cbsa_code", "msa_name", "population", "state_abbr", "census_region",
        "pop_stratum", "rail_stratum", "sm_stratum", "stratum",
        "has_rail", "has_shared_mobility",
        "n_agencies", "agency_list",
        "n_shared_mobility", "shared_mobility_list",
        "selection_method", "sample_weight",
    ]
    cols = [c for c in cols_order if c in sample.columns]
    sample[cols].to_csv(out, index=False)
    log.info("Wrote %s", out)
    return out


def summary_report(sample: pd.DataFrame, universe: pd.DataFrame) -> str:
    """Return a text summary report."""
    total_pop = universe["population"].sum()
    sample_pop = sample["population"].sum()
    lines = [
        "=" * 65,
        "  MSA SAMPLE – SUMMARY REPORT",
        "=" * 65,
        f"Universe MSAs:        {len(universe)}",
        f"Sample size:          {len(sample)}",
        f"Population coverage:  {sample_pop:,} / {total_pop:,} "
        f"({sample_pop / total_pop:.1%})",
        "",
        "── Selection method breakdown ──",
        sample["selection_method"].value_counts().to_string(),
        "",
        "── Population stratum ──",
        sample["pop_stratum"].value_counts().to_string(),
        "",
        "── Census region ──",
        sample["census_region"].value_counts().to_string(),
        "",
        "── Rail presence ──",
        sample["has_rail"].value_counts().to_string(),
        "",
        "── Shared mobility presence ──",
        sample["has_shared_mobility"].value_counts().to_string(),
        "",
        "── Sample weight summary ──",
        sample["sample_weight"].describe().to_string(),
        "",
        "── Selected MSAs ──",
    ]
    for _, r in sample.iterrows():
        lines.append(
            f"  {r['cbsa_code']}  {r['msa_name']:<55s} "
            f"pop={r['population']:>12,}  method={r['selection_method']}"
        )
    lines.append("=" * 65)
    return "\n".join(lines)


def save_report(report_text: str, filename: str = "sample_report.txt") -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUTPUT_DIR / filename
    out.write_text(report_text)
    log.info("Wrote %s", out)
    return out


def plot_map(sample: pd.DataFrame, filename: str = "sample_map.png") -> Path | None:
    """Optional: plot selected MSAs on a US map using approximate coords."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        log.warning("matplotlib not available – skipping map")
        return None

    # Approximate lat/lon for selected MSAs (good enough for a dot map)
    COORDS = {
        "35620": (40.7, -74.0), "31080": (34.0, -118.2), "16980": (41.9, -87.6),
        "19100": (32.8, -96.8), "26420": (29.8, -95.4), "47900": (38.9, -77.0),
        "33100": (25.8, -80.2), "37980": (40.0, -75.2), "12060": (33.7, -84.4),
        "14460": (42.4, -71.1), "38060": (33.4, -112.1), "41860": (37.8, -122.4),
        "40140": (33.9, -117.4), "19820": (42.3, -83.0), "42660": (47.6, -122.3),
        "33460": (44.97, -93.27), "41740": (32.7, -117.2), "45300": (28.0, -82.5),
        "19740": (39.7, -105.0), "41180": (38.6, -90.2), "12580": (39.3, -76.6),
        "36740": (28.5, -81.4), "16740": (35.2, -80.8), "41700": (29.4, -98.5),
        "38900": (45.5, -122.7), "40900": (38.6, -121.5), "38300": (40.4, -80.0),
        "12420": (30.3, -97.7), "28140": (39.1, -94.6), "17460": (41.5, -81.7),
        "18140": (40.0, -83.0), "26900": (39.8, -86.1), "29820": (36.2, -115.1),
        "34980": (36.2, -86.8), "47260": (36.8, -76.3), "39300": (41.8, -71.4),
        "27260": (30.3, -81.7), "33340": (43.0, -87.9), "36420": (35.5, -97.5),
        "39580": (35.8, -78.6), "41620": (40.8, -111.9), "46060": (32.2, -110.9),
        "46140": (36.2, -96.0), "10740": (35.1, -106.6), "36540": (41.3, -96.0),
        "30700": (40.8, -96.7), "22020": (46.9, -96.8), "14260": (43.6, -116.2),
        "21340": (31.8, -106.4), "44060": (47.7, -117.4), "30020": (34.6, -98.4),
        "24860": (34.9, -82.4), "13820": (33.5, -86.8),
    }

    fig, ax = plt.subplots(figsize=(12, 7))
    ax.set_xlim(-130, -65)
    ax.set_ylim(24, 50)
    ax.set_aspect("equal")
    ax.set_facecolor("#f0f4f8")
    ax.set_title("Selected MSA Sample", fontsize=14, fontweight="bold")

    for _, row in sample.iterrows():
        coords = COORDS.get(row["cbsa_code"])
        if not coords:
            continue
        lat, lon = coords
        size = max(20, row["population"] / 200_000)
        color = {"mandatory_top10": "#e63946", "stratified_random": "#457b9d",
                 "coverage_boost": "#2a9d8f"}.get(row["selection_method"], "#999")
        ax.scatter(lon, lat, s=size, c=color, alpha=0.75, edgecolors="white", linewidths=0.5)
        if row["population"] > 3_000_000:
            ax.annotate(row["msa_name"].split(",")[0].split("-")[0],
                        (lon, lat), fontsize=6, ha="center", va="bottom")

    # Legend
    for label, color in [("Mandatory top-10", "#e63946"),
                          ("Stratified random", "#457b9d"),
                          ("Coverage boost", "#2a9d8f")]:
        ax.scatter([], [], c=color, s=40, label=label)
    ax.legend(loc="lower left", fontsize=8)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUTPUT_DIR / filename
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info("Wrote %s", out)
    return out
