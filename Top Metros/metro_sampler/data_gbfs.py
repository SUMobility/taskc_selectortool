"""
Fetch shared-mobility (bikeshare / scooter) operator presence from
MobilityData's GBFS systems catalog.

Uses dynamic Census-based city/state matching (same approach as NTD module).
"""
import logging
import re
import pandas as pd
import requests

from metro_sampler.config import GBFS_CATALOG_URL

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
        log.warning("Could not fetch Census MSAs for GBFS matching: %s", exc)
        _CITY_CBSA_CACHE = {"city_state": {}, "city_only": {}}
        return _CITY_CBSA_CACHE

    def _parse_msa(msa_name):
        clean = re.sub(r"\s*Metro(politan)?\s*Area$", "", msa_name)
        parts = clean.split(",")
        city_part = parts[0].strip()
        state_part = parts[1].strip() if len(parts) > 1 else ""
        cities = [c.strip().lower() for c in re.split(r"[-/]", city_part)]
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
    log.info("  Built GBFS city->CBSA index: %d city+state keys, %d unambiguous city keys",
             len(city_state_idx), len(city_only))
    return _CITY_CBSA_CACHE


def _match_location_to_cbsa(location: str) -> str:
    """Parse a GBFS location string and match to CBSA code.

    Location formats vary:
      - "Miami, FL"
      - "Austin, TX, USA"
      - "Washington, DC, US"
      - "New York, NY"
    """
    if not location or pd.isna(location):
        return ""

    idx = _build_city_cbsa_index()
    if not idx or not idx.get("city_state"):
        return ""

    # Normalize and parse
    loc = location.strip()
    parts = [p.strip() for p in loc.split(",")]

    # Extract city (first part) and state (second part, if 2-letter code)
    city = parts[0].lower() if parts else ""
    state = ""
    for p in parts[1:]:
        p_clean = p.strip().upper()
        if len(p_clean) == 2 and p_clean.isalpha():
            state = p_clean
            break

    # Try city+state first
    if city and state:
        cbsa = idx["city_state"].get((city, state))
        if cbsa:
            return cbsa

    # Fallback: unambiguous city match
    if city:
        cbsa = idx["city_only"].get(city)
        if cbsa:
            return cbsa

    return ""


def fetch_gbfs_systems() -> pd.DataFrame:
    """Download the MobilityData GBFS systems catalog CSV.
    Returns DataFrame: system_id, name, location, cbsa_code
    """
    try:
        # Use requests to handle SSL more gracefully than pandas.read_csv
        resp = requests.get(GBFS_CATALOG_URL, timeout=30)
        resp.raise_for_status()
        from io import StringIO
        df = pd.read_csv(StringIO(resp.text))
    except Exception as exc:
        log.warning("Could not fetch GBFS catalog: %s – using fallback", exc)
        return _builtin_gbfs()

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Keep US-only
    if "country_code" in df.columns:
        df = df[df["country_code"].str.upper() == "US"].copy()
    elif "location" in df.columns:
        df = df[df["location"].str.contains("US|United States", case=False, na=False)].copy()

    # Match locations to CBSA codes using Census-based index
    if "location" in df.columns:
        df["cbsa_code"] = df["location"].apply(_match_location_to_cbsa)
    else:
        df["cbsa_code"] = ""

    # Count matches before filtering
    n_total = len(df)
    n_matched = (df["cbsa_code"] != "").sum()

    df = df[df["cbsa_code"] != ""].copy()

    keep = ["system_id", "name", "location", "cbsa_code"]
    for c in keep:
        if c not in df.columns:
            df[c] = ""

    log.info("  Matched %d of %d US GBFS systems to a CBSA", n_matched, n_total)
    return df[keep]


def gbfs_by_cbsa(systems: pd.DataFrame) -> pd.DataFrame:
    """Aggregate: count and list of operators per CBSA."""
    if len(systems) == 0:
        return pd.DataFrame(columns=["cbsa_code", "n_shared_mobility",
                                      "shared_mobility_list", "has_shared_mobility"])
    grouped = systems.groupby("cbsa_code").agg(
        n_shared_mobility=("name", "count"),
        shared_mobility_list=("name", lambda s: "; ".join(s.unique())),
    ).reset_index()
    grouped["has_shared_mobility"] = True
    return grouped


# ── Fallback (only used if GBFS catalog fetch fails) ────────────────────────
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
