"""
Fetch shared-mobility (bikeshare / scooter) operator presence from two
complementary sources:

  1. **BTS** (Bureau of Transportation Statistics) – comprehensive,
     government-maintained dataset of every US city with bikeshare
     and/or e-scooter service (primary source).
  2. **MobilityData GBFS catalog** – voluntary industry registry,
     used as a supplement for any CBSAs not already covered by BTS.

Uses dynamic Census-based city/state matching (same approach as NTD module).
"""
import logging
import re
import pandas as pd
import requests

from metro_sampler.config import BTS_SYSTEMS_URL, BTS_YEAR, GBFS_CATALOG_URL

log = logging.getLogger(__name__)

# ── Census-based city/state index (shared with data_ntd.py) ─────────────────
_CITY_CBSA_CACHE: dict | None = None


def _build_city_cbsa_index() -> dict:
    """Build a mapping from (city, state) -> CBSA code using Census MSA names.
    Also builds city-only index for unambiguous matches."""
    global _CITY_CBSA_CACHE
    if _CITY_CBSA_CACHE is not None:
        return _CITY_CBSA_CACHE

    from metro_sampler.config import CENSUS_BASE, CENSUS_YEAR

    try:
        url = f"{CENSUS_BASE}/{CENSUS_YEAR}/acs/acs5"
        params = {
            "get": "NAME,B01003_001E",
            "for": "metropolitan statistical area/micropolitan statistical area:*",
        }
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        header, *data = resp.json()
        census = pd.DataFrame(data, columns=header)
        census = census[census["NAME"].str.contains("Metro", case=False, na=False)]
        census.columns = ["msa_name", "population", "cbsa_code"]
    except Exception as exc:
        log.warning("Could not fetch Census MSAs for shared-mobility matching: %s", exc)
        _CITY_CBSA_CACHE = {"city_state": {}, "city_only": {}}
        return _CITY_CBSA_CACHE

    def _parse_msa(msa_name):
        clean = re.sub(r"\s*Metro(politan)?\s*Area$", "", msa_name)
        parts = clean.split(",")
        city_part = parts[0].strip()
        state_part = parts[1].strip() if len(parts) > 1 else ""
        cities = [c.strip().lower().replace(".", "") for c in re.split(r"[-/]", city_part)]
        states = [s.strip().upper() for s in re.split(r"[-/]", state_part)]
        return cities, states

    city_state_idx: dict[tuple[str, str], str] = {}
    city_only: dict[str, str] = {}
    city_ambig: set[str] = set()

    for _, row in census.iterrows():
        cities, states = _parse_msa(row["msa_name"])
        cbsa = row["cbsa_code"]
        for city in cities:
            for state in states:
                city_state_idx.setdefault((city, state), cbsa)
            if city in city_ambig:
                continue
            if city in city_only and city_only[city] != cbsa:
                city_ambig.add(city)
                del city_only[city]
            else:
                city_only[city] = cbsa

    _CITY_CBSA_CACHE = {
        "city_state": city_state_idx,
        "city_only": city_only,
    }
    log.info("  Built city->CBSA index: %d city+state keys, %d unambiguous city keys",
             len(city_state_idx), len(city_only))
    return _CITY_CBSA_CACHE


def _match_city_state_to_cbsa(city: str, state: str) -> str:
    """Match a city/state pair to a CBSA code using the Census-derived index.
    Tries exact match first, then substring containment as fallback."""
    if not city:
        return ""

    idx = _build_city_cbsa_index()
    if not idx or not idx.get("city_state"):
        return ""

    city_n = city.strip().lower().replace(".", "")
    state_n = state.strip().upper() if state else ""

    # Try city+state first (exact)
    if city_n and state_n:
        cbsa = idx["city_state"].get((city_n, state_n))
        if cbsa:
            return cbsa

    # Fallback: unambiguous city match (exact)
    if city_n:
        cbsa = idx["city_only"].get(city_n)
        if cbsa:
            return cbsa

    # Fallback: substring match on city+state
    if city_n and state_n:
        for (ic, ist), cbsa in idx["city_state"].items():
            if ist == state_n and (city_n in ic or ic in city_n):
                return cbsa

    return ""


def _match_location_to_cbsa(location: str) -> str:
    """Parse a GBFS location string ('City, ST' or 'City, ST, US')
    and match to CBSA code."""
    if not location or pd.isna(location):
        return ""

    parts = [p.strip() for p in location.split(",")]
    city = parts[0] if parts else ""
    state = ""
    for p in parts[1:]:
        p_clean = p.strip().upper()
        if len(p_clean) == 2 and p_clean.isalpha():
            state = p_clean
            break

    return _match_city_state_to_cbsa(city, state)


# ── BTS (primary source) ────────────────────────────────────────────────────

def _parse_bts_operators(row: pd.Series) -> list[str]:
    """Extract unique operator names from a BTS row's docknm / docklessnm /
    scooternm columns (comma-separated, may contain parenthetical notes)."""
    ops: set[str] = set()
    for col in ("docknm", "docklessnm", "scooternm"):
        val = row.get(col)
        if pd.isna(val) or not val:
            continue
        for raw in str(val).split(","):
            name = raw.strip()
            # Drop parenthetical suffixes like "(e-bike)" or "(acquired …)"
            name = re.sub(r"\s*\(.*$", "", name).strip()
            if name and name.lower() not in ("nan", ""):
                ops.add(name)
    return sorted(ops)


def _fetch_bts_systems() -> pd.DataFrame | None:
    """Fetch the BTS Bikeshare & E-scooter Systems dataset for the
    configured year.  Returns DataFrame with system_id, name, location,
    cbsa_code – or None on failure."""
    try:
        params = {"$where": f"year='{BTS_YEAR}'", "$limit": 5000}
        resp = requests.get(BTS_SYSTEMS_URL, params=params, timeout=30)
        resp.raise_for_status()
        from io import StringIO
        bts = pd.read_csv(StringIO(resp.text))
    except Exception as exc:
        log.warning("Could not fetch BTS shared-mobility data: %s", exc)
        return None

    if len(bts) == 0:
        log.warning("BTS returned 0 rows for year %s", BTS_YEAR)
        return None

    log.info("  BTS: %d cities with shared mobility in %s", len(bts), BTS_YEAR)

    # Match each BTS city to a CBSA
    records: list[dict] = []
    for _, row in bts.iterrows():
        city = str(row.get("city", "")).strip()
        state = str(row.get("state", "")).strip()
        cbsa = _match_city_state_to_cbsa(city, state)
        if not cbsa:
            continue

        operators = _parse_bts_operators(row)
        if not operators:
            continue

        location = f"{city}, {state}"
        for op in operators:
            sid = re.sub(r"\W+", "_", f"bts_{op}_{city}_{state}").lower()
            records.append({
                "system_id": sid,
                "name": op,
                "location": location,
                "cbsa_code": cbsa,
            })

    df = pd.DataFrame(records)
    n_cbsa = df["cbsa_code"].nunique() if len(df) else 0
    log.info("  BTS: matched %d operator-entries across %d CBSAs", len(df), n_cbsa)
    return df


# ── GBFS (supplement) ───────────────────────────────────────────────────────

def _fetch_gbfs_systems() -> pd.DataFrame:
    """Fetch the MobilityData GBFS catalog (voluntary registry).
    Returns DataFrame with system_id, name, location, cbsa_code."""
    try:
        resp = requests.get(GBFS_CATALOG_URL, timeout=30)
        resp.raise_for_status()
        from io import StringIO
        df = pd.read_csv(StringIO(resp.text))
    except Exception as exc:
        log.warning("Could not fetch GBFS catalog: %s", exc)
        return pd.DataFrame(columns=["system_id", "name", "location", "cbsa_code"])

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Keep US-only
    if "country_code" in df.columns:
        df = df[df["country_code"].str.upper() == "US"].copy()
    elif "location" in df.columns:
        df = df[df["location"].str.contains("US|United States", case=False, na=False)].copy()

    if "location" in df.columns:
        df["cbsa_code"] = df["location"].apply(_match_location_to_cbsa)
    else:
        df["cbsa_code"] = ""

    df = df[df["cbsa_code"] != ""].copy()

    keep = ["system_id", "name", "location", "cbsa_code"]
    for c in keep:
        if c not in df.columns:
            df[c] = ""

    return df[keep]


# ── Public API ──────────────────────────────────────────────────────────────

def fetch_gbfs_systems() -> pd.DataFrame:
    """Return a combined DataFrame of shared-mobility operators mapped to
    CBSA codes.  Uses BTS as primary source, supplements with GBFS catalog
    entries for any CBSAs not already covered.

    Returns DataFrame: system_id, name, location, cbsa_code
    """
    # 1. Try BTS (comprehensive, government-maintained)
    bts_df = _fetch_bts_systems()

    # 2. Fetch GBFS catalog (voluntary, used as supplement)
    gbfs_df = _fetch_gbfs_systems()

    if bts_df is not None and len(bts_df):
        # Supplement BTS with GBFS entries for uncovered CBSAs
        bts_cbsas = set(bts_df["cbsa_code"].unique())
        gbfs_new = gbfs_df[~gbfs_df["cbsa_code"].isin(bts_cbsas)]
        if len(gbfs_new):
            log.info("  GBFS supplement: %d entries for %d CBSAs not in BTS",
                     len(gbfs_new), gbfs_new["cbsa_code"].nunique())
            combined = pd.concat([bts_df, gbfs_new], ignore_index=True)
        else:
            combined = bts_df
    elif len(gbfs_df):
        log.info("  BTS unavailable – using GBFS catalog only")
        combined = gbfs_df
    else:
        log.warning("  Both BTS and GBFS unavailable – using built-in fallback")
        return _builtin_gbfs()

    log.info("  Total: %d operator-entries across %d CBSAs",
             len(combined), combined["cbsa_code"].nunique())
    return combined


def gbfs_by_cbsa(systems: pd.DataFrame) -> pd.DataFrame:
    """Aggregate: count and list of operators per CBSA."""
    if len(systems) == 0:
        return pd.DataFrame(columns=["cbsa_code", "n_shared_mobility",
                                      "shared_mobility_list", "has_shared_mobility"])
    grouped = systems.groupby("cbsa_code").agg(
        n_shared_mobility=("name", "nunique"),
        shared_mobility_list=("name", lambda s: "; ".join(sorted(s.unique()))),
    ).reset_index()
    grouped["has_shared_mobility"] = True
    return grouped


# ── Fallback (only used if both BTS and GBFS fetches fail) ───────────────
def _builtin_gbfs() -> pd.DataFrame:
    """Minimal fallback - prefer live data."""
    rows = [
        ("citi_bike_nyc", "Citi Bike", "New York, NY", "35620"),
        ("divvy_chicago", "Divvy", "Chicago, IL", "16980"),
        ("capital_bikeshare", "Capital Bikeshare", "Washington, DC", "47900"),
        ("bluebikes", "Bluebikes", "Boston, MA", "14460"),
        ("bay_wheels", "Bay Wheels", "San Francisco, CA", "41860"),
    ]
    return pd.DataFrame(rows, columns=["system_id", "name", "location", "cbsa_code"])
