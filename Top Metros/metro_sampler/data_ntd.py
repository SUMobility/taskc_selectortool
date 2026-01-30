"""
Load transit agency information from NTD (National Transit Database) files.

Expects downloaded NTD data in DATA_DIR/ntd/. If not present, falls back to
a curated list of major transit agencies with mode info.
"""
import logging
from pathlib import Path
import pandas as pd

from metro_sampler.config import NTD_DIR

log = logging.getLogger(__name__)


def load_ntd_agencies() -> pd.DataFrame:
    """Return DataFrame with columns:
        ntd_id, agency_name, cbsa_code, city, state, modes, has_rail
    """
    agency_file = _find_ntd_file("Agency_Information" , "agency", "2023_Agency")
    if agency_file:
        return _parse_ntd_file(agency_file)
    log.warning("NTD files not found in %s – using built-in agency list", NTD_DIR)
    return _builtin_agencies()


def _find_ntd_file(*patterns: str) -> Path | None:
    if not NTD_DIR.exists():
        return None
    for p in NTD_DIR.iterdir():
        for pat in patterns:
            if pat.lower() in p.name.lower() and p.suffix in (".xlsx", ".csv", ".xls"):
                return p
    return None


def _parse_ntd_file(path: Path) -> pd.DataFrame:
    """Best-effort parse of an NTD agency spreadsheet."""
    log.info("Reading NTD file: %s", path)
    if path.suffix == ".csv":
        raw = pd.read_csv(path, dtype=str)
    else:
        raw = pd.read_excel(path, dtype=str)

    # Normalise column names
    raw.columns = [c.strip().lower().replace(" ", "_") for c in raw.columns]

    # Rename known columns
    rename = {}
    for c in raw.columns:
        if c == "ntd_id":
            rename[c] = "ntd_id"
        elif c == "agency_name":
            rename[c] = "agency_name"
        elif c == "uza_name":
            rename[c] = "uza_name"
        elif c == "primary_uza_uace_code":
            rename[c] = "uza_code"
        elif c == "city":
            rename[c] = "city"
        elif c == "state":
            rename[c] = "state"
    raw = raw.rename(columns=rename)

    if "agency_name" not in raw.columns:
        log.error("NTD file missing agency_name column")
        return _builtin_agencies()

    for col in ["ntd_id", "uza_name", "uza_code", "city", "state"]:
        if col not in raw.columns:
            raw[col] = ""

    # Drop rows without an agency name
    df = raw[raw["agency_name"].notna() & (raw["agency_name"] != "")].copy()

    # Map UZA names to CBSA codes
    df["cbsa_code"] = df["uza_name"].apply(_uza_to_cbsa)

    # This file has no modes column; use builtin rail data to enrich later
    df["modes"] = ""
    df["has_rail"] = False

    result = df[["ntd_id", "agency_name", "cbsa_code", "city", "state", "modes", "has_rail"]].copy()

    # Enrich rail info from builtin data
    builtin = _builtin_agencies()
    rail_by_cbsa = set(builtin.loc[builtin["has_rail"], "cbsa_code"].unique())
    result["has_rail"] = result["cbsa_code"].isin(rail_by_cbsa)

    log.info("  Parsed %d agencies, matched %d to a CBSA",
             len(result), (result["cbsa_code"] != "").sum())
    return result


# ── Dynamic UZA-to-CBSA mapping built from Census MSA names ─────────────────
import re
import requests

_UZA_CBSA_CACHE: dict[str, str] | None = None


def _build_uza_cbsa_map() -> dict[str, str]:
    """Build a mapping from UZA name -> CBSA code by matching city/state
    text from Census MSA names against NTD UZA names."""
    global _UZA_CBSA_CACHE
    if _UZA_CBSA_CACHE is not None:
        return _UZA_CBSA_CACHE

    from metro_sampler.config import CENSUS_BASE, CENSUS_YEAR

    # Fetch Census MSA list
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
        log.warning("Could not fetch Census MSAs for UZA mapping: %s", exc)
        _UZA_CBSA_CACHE = {}
        return _UZA_CBSA_CACHE

    def _parse_msa(msa_name):
        clean = re.sub(r"\s*Metro(politan)?\s*Area$", "", msa_name)
        parts = clean.split(",")
        city_part = parts[0].strip()
        state_part = parts[1].strip() if len(parts) > 1 else ""
        cities = [c.strip().lower() for c in re.split(r"[-/]", city_part)]
        states = [s.strip().upper() for s in re.split(r"[-/]", state_part)]
        return cities, states

    # Index: (city_lower, state_upper) -> cbsa_code
    city_state_idx: dict[tuple[str, str], str] = {}
    # Also track city-only for unambiguous matches
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

    _UZA_CBSA_CACHE = {
        "city_state": city_state_idx,
        "city_only": city_only,
    }
    log.info("  Built UZA->CBSA index: %d city+state keys, %d unambiguous city keys",
             len(city_state_idx), len(city_only))
    return _UZA_CBSA_CACHE


def _uza_to_cbsa(uza_name: str) -> str:
    """Match a UZA name string to a CBSA code using Census-derived index."""
    if not uza_name or pd.isna(uza_name):
        return ""
    if "non-uza" in uza_name.lower():
        return ""

    idx = _build_uza_cbsa_map()
    if not idx:
        return ""

    # Parse UZA: "City1--City2, ST" or "City1--City2, ST1-ST2"
    parts = uza_name.split(",")
    city_part = parts[0].strip()
    state_part = parts[1].strip() if len(parts) > 1 else ""
    cities = [c.strip().lower() for c in re.split(r"--|/", city_part)]
    states = [s.strip().upper() for s in re.split(r"[-/]", state_part) if s.strip()]

    # Try city+state first
    for city in cities:
        for state in states:
            cbsa = idx["city_state"].get((city, state))
            if cbsa:
                return cbsa

    # Fallback: unambiguous city match
    for city in cities:
        cbsa = idx["city_only"].get(city)
        if cbsa:
            return cbsa

    return ""


def _builtin_agencies() -> pd.DataFrame:
    """Curated list covering the top ~70 MSAs."""
    # (cbsa_code, agency, city, state, modes, has_rail)
    rows = [
        ("35620", "MTA New York City Transit", "New York", "NY", "HR,Bus", True),
        ("35620", "MTA Bus Company", "New York", "NY", "Bus", False),
        ("35620", "NJ Transit", "Newark", "NJ", "CR,Bus,LR", True),
        ("35620", "Port Authority Trans-Hudson", "New York", "NY", "HR", True),
        ("31080", "LA Metro", "Los Angeles", "CA", "HR,LR,Bus", True),
        ("31080", "OCTA", "Orange", "CA", "Bus", False),
        ("31080", "Metrolink", "Los Angeles", "CA", "CR", True),
        ("16980", "CTA", "Chicago", "IL", "HR,Bus", True),
        ("16980", "Metra", "Chicago", "IL", "CR", True),
        ("16980", "Pace", "Arlington Heights", "IL", "Bus", False),
        ("19100", "DART", "Dallas", "TX", "LR,Bus", True),
        ("19100", "Trinity Metro", "Fort Worth", "TX", "Bus,CR", True),
        ("26420", "METRO Houston", "Houston", "TX", "LR,Bus", True),
        ("47900", "WMATA", "Washington", "DC", "HR,Bus", True),
        ("33100", "Miami-Dade Transit", "Miami", "FL", "HR,Bus", True),
        ("33100", "Broward County Transit", "Fort Lauderdale", "FL", "Bus", False),
        ("37980", "SEPTA", "Philadelphia", "PA", "HR,CR,LR,Bus", True),
        ("12060", "MARTA", "Atlanta", "GA", "HR,Bus", True),
        ("14460", "MBTA", "Boston", "MA", "HR,CR,LR,Bus", True),
        ("38060", "Valley Metro", "Phoenix", "AZ", "LR,Bus", True),
        ("41860", "BART", "San Francisco", "CA", "HR", True),
        ("41860", "SF Muni", "San Francisco", "CA", "LR,Bus", True),
        ("40140", "Omnitrans", "San Bernardino", "CA", "Bus", False),
        ("19820", "DDOT", "Detroit", "MI", "Bus", False),
        ("19820", "SMART", "Detroit", "MI", "Bus", False),
        ("42660", "Sound Transit", "Seattle", "WA", "LR,CR,Bus", True),
        ("42660", "King County Metro", "Seattle", "WA", "Bus", False),
        ("33460", "Metro Transit", "Minneapolis", "MN", "LR,Bus", True),
        ("41740", "MTS San Diego", "San Diego", "CA", "LR,Bus", True),
        ("45300", "HART", "Tampa", "FL", "Bus", False),
        ("19740", "RTD Denver", "Denver", "CO", "LR,CR,Bus", True),
        ("41180", "Metro St. Louis", "St. Louis", "MO", "LR,Bus", True),
        ("12580", "MTA Maryland", "Baltimore", "MD", "HR,LR,Bus", True),
        ("36740", "LYNX Orlando", "Orlando", "FL", "Bus", False),
        ("16740", "CATS Charlotte", "Charlotte", "NC", "LR,Bus", True),
        ("41700", "VIA Metropolitan Transit", "San Antonio", "TX", "Bus", False),
        ("38900", "TriMet", "Portland", "OR", "LR,CR,Bus", True),
        ("40900", "SacRT", "Sacramento", "CA", "LR,Bus", True),
        ("38300", "Pittsburgh Regional Transit", "Pittsburgh", "PA", "LR,Bus", True),
        ("12420", "Cap Metro", "Austin", "TX", "Bus,CR", True),
        ("28140", "KCATA", "Kansas City", "MO", "Bus", False),
        ("17460", "GCRTA", "Cleveland", "OH", "HR,Bus", True),
        ("18140", "COTA", "Columbus", "OH", "Bus", False),
        ("26900", "IndyGo", "Indianapolis", "IN", "Bus", False),
        ("29820", "RTC Southern Nevada", "Las Vegas", "NV", "Bus", False),
        ("34980", "WeGo Nashville", "Nashville", "TN", "Bus", False),
        ("47260", "Hampton Roads Transit", "Norfolk", "VA", "LR,Bus", True),
        ("39300", "RIPTA", "Providence", "RI", "Bus", False),
        ("27260", "JTA", "Jacksonville", "FL", "Bus", False),
        ("33340", "MCTS Milwaukee", "Milwaukee", "WI", "Bus", False),
        ("36420", "EMBARK OKC", "Oklahoma City", "OK", "Bus", False),
        ("41620", "UTA", "Salt Lake City", "UT", "LR,CR,Bus", True),
        ("46060", "Sun Tran Tucson", "Tucson", "AZ", "Bus", False),
        ("46140", "Tulsa Transit", "Tulsa", "OK", "Bus", False),
        ("21340", "Sun Metro El Paso", "El Paso", "TX", "Bus", False),
        ("10740", "ABQ Ride", "Albuquerque", "NM", "Bus", False),
        ("36540", "Metro Transit Omaha", "Omaha", "NE", "Bus", False),
        ("30700", "StarTran", "Lincoln", "NE", "Bus", False),
        ("22020", "MATBUS", "Fargo", "ND", "Bus", False),
        ("14260", "Valley Regional Transit", "Boise", "ID", "Bus", False),
    ]
    df = pd.DataFrame(rows, columns=[
        "cbsa_code", "agency_name", "city", "state", "modes", "has_rail",
    ])
    df["ntd_id"] = ""
    return df[["ntd_id", "agency_name", "cbsa_code", "city", "state", "modes", "has_rail"]]


def agencies_by_cbsa(agencies: pd.DataFrame) -> pd.DataFrame:
    """Aggregate to one row per CBSA: agency count, agency list, rail flag."""
    grouped = agencies.groupby("cbsa_code").agg(
        n_agencies=("agency_name", "count"),
        agency_list=("agency_name", lambda s: "; ".join(s)),
        has_rail=("has_rail", "any"),
    ).reset_index()
    return grouped
