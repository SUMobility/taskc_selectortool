"""
Fetch shared-mobility (bikeshare / scooter) operator presence from
MobilityData's GBFS systems catalog.
"""
import logging
import pandas as pd
import requests

from metro_sampler.config import GBFS_CATALOG_URL

log = logging.getLogger(__name__)

# Manual mapping: GBFS system location string fragments -> CBSA codes
# Extend as needed.
_CITY_CBSA = {
    "new york": "35620", "nyc": "35620", "jersey city": "35620",
    "los angeles": "31080", "la ": "31080", "santa monica": "31080",
    "chicago": "16980",
    "dallas": "19100", "fort worth": "19100",
    "houston": "26420",
    "washington": "47900", "arlington, va": "47900", "dc": "47900",
    "miami": "33100", "fort lauderdale": "33100",
    "philadelphia": "37980",
    "atlanta": "12060",
    "boston": "14460", "cambridge": "14460",
    "phoenix": "38060", "tempe": "38060", "mesa": "38060",
    "san francisco": "41860", "oakland": "41860", "berkeley": "41860",
    "riverside": "40140", "san bernardino": "40140",
    "detroit": "19820",
    "seattle": "42660",
    "minneapolis": "33460", "st. paul": "33460",
    "san diego": "41740",
    "tampa": "45300", "st. petersburg": "45300",
    "denver": "19740",
    "st. louis": "41180",
    "baltimore": "12580",
    "orlando": "36740",
    "charlotte": "16740",
    "san antonio": "41700",
    "portland": "38900",
    "sacramento": "40900",
    "pittsburgh": "38300",
    "austin": "12420",
    "kansas city": "28140",
    "cleveland": "17460",
    "columbus": "18140",
    "indianapolis": "26900",
    "las vegas": "29820",
    "nashville": "34980",
    "norfolk": "47260",
    "providence": "39300",
    "milwaukee": "33340",
    "salt lake": "41620",
    "tucson": "46060",
    "omaha": "36540",
    "raleigh": "39580",
    "boise": "14260",
    "albuquerque": "10740",
    "el paso": "21340",
    "spokane": "44060",
    "fargo": "22020",
    "lincoln, ne": "30700",
    "oklahoma city": "36420",
}


def fetch_gbfs_systems() -> pd.DataFrame:
    """Download the MobilityData GBFS systems catalog CSV.
    Returns DataFrame: system_id, name, location, cbsa_code
    """
    try:
        df = pd.read_csv(GBFS_CATALOG_URL)
    except Exception as exc:
        log.warning("Could not fetch GBFS catalog: %s – using fallback", exc)
        return _builtin_gbfs()

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Keep US-only
    if "country_code" in df.columns:
        df = df[df["country_code"].str.upper() == "US"].copy()
    elif "location" in df.columns:
        df = df[df["location"].str.contains("US|United States", case=False, na=False)].copy()

    df["cbsa_code"] = df.apply(_match_cbsa, axis=1)
    df = df[df["cbsa_code"] != ""].copy()

    keep = ["system_id", "name", "location", "cbsa_code"]
    for c in keep:
        if c not in df.columns:
            df[c] = ""
    return df[keep]


def _match_cbsa(row) -> str:
    """Try to match a GBFS system to a CBSA code via location/name text."""
    text = " ".join(str(row.get(c, "")) for c in ["location", "name"]).lower()
    for fragment, cbsa in _CITY_CBSA.items():
        if fragment in text:
            return cbsa
    return ""


def gbfs_by_cbsa(systems: pd.DataFrame) -> pd.DataFrame:
    """Aggregate: count and list of operators per CBSA."""
    grouped = systems.groupby("cbsa_code").agg(
        n_shared_mobility=("name", "count"),
        shared_mobility_list=("name", lambda s: "; ".join(s.unique())),
    ).reset_index()
    grouped["has_shared_mobility"] = True
    return grouped


# ── Fallback ────────────────────────────────────────────────────────────────
def _builtin_gbfs() -> pd.DataFrame:
    rows = [
        ("citi_bike_nyc", "Citi Bike", "New York, US", "35620"),
        ("metro_bike_la", "Metro Bike Share", "Los Angeles, US", "31080"),
        ("divvy_chicago", "Divvy", "Chicago, US", "16980"),
        ("capital_bikeshare", "Capital Bikeshare", "Washington DC, US", "47900"),
        ("bluebikes", "Bluebikes", "Boston, US", "14460"),
        ("bay_wheels", "Bay Wheels", "San Francisco, US", "41860"),
        ("nice_ride", "Nice Ride", "Minneapolis, US", "33460"),
        ("bcycle_denver", "Denver B-cycle", "Denver, US", "19740"),
        ("bcycle_austin", "Austin B-cycle", "Austin, US", "12420"),
        ("indego", "Indego", "Philadelphia, US", "37980"),
        ("cogo", "CoGo", "Columbus, US", "18140"),
        ("relay_atlanta", "Relay", "Atlanta, US", "12060"),
        ("healthy_ride", "Healthy Ride", "Pittsburgh, US", "38300"),
        ("biketown", "BIKETOWN", "Portland, US", "38900"),
        ("bcycle_charlotte", "Charlotte B-cycle", "Charlotte, US", "16740"),
        ("lime_seattle", "Lime", "Seattle, US", "42660"),
        ("bird_nashville", "Bird", "Nashville, US", "34980"),
        ("lime_san_diego", "Lime", "San Diego, US", "41740"),
        ("lime_salt_lake", "Lime", "Salt Lake City, US", "41620"),
        ("pacers_indianapolis", "Pacers Bikeshare", "Indianapolis, US", "26900"),
        ("bublr_milwaukee", "Bublr Bikes", "Milwaukee, US", "33340"),
        ("greenbike_slc", "GREENbike", "Salt Lake City, US", "41620"),
    ]
    return pd.DataFrame(rows, columns=["system_id", "name", "location", "cbsa_code"])
