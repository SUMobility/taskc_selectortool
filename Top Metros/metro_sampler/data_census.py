"""
Fetch MSA/CBSA populations from the Census Bureau API.

Falls back to a curated offline list if the API key is missing or the call
fails, so the pipeline can always run.
"""
import os
import logging
import pandas as pd
import requests

from metro_sampler.config import (
    CENSUS_API_KEY, CENSUS_BASE, CENSUS_YEAR, STATE_TO_REGION,
)

log = logging.getLogger(__name__)

# ── FIPS state code -> 2-letter abbreviation ────────────────────────────────
FIPS_TO_ABBR = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
    "56": "WY", "72": "PR",
}


def _primary_state_abbr(cbsa_code: str, name: str) -> str:
    """Heuristic: derive the primary state from the MSA name (last token
    before any hyphen in the state portion, e.g. 'New York-Newark-Jersey
    City, NY-NJ-PA' -> 'NY')."""
    try:
        state_part = name.split(",")[-1].strip()
        first_state = state_part.split("-")[0].strip()
        return first_state
    except Exception:
        return ""


def fetch_msa_population() -> pd.DataFrame:
    """Return a DataFrame with columns:
        cbsa_code, msa_name, population, state_abbr, census_region
    Sorted descending by population.
    """
    api_key = os.environ.get("CENSUS_API_KEY", CENSUS_API_KEY)
    df = _fetch_from_api(api_key)
    if df is None:
        log.warning("Census API unavailable – using built-in MSA list")
        df = _builtin_msa_list()
    return df


def _fetch_from_api(api_key: str | None) -> pd.DataFrame | None:
    """Try ACS 5-year estimates for CBSA-level total population.
    Works with or without an API key."""
    url = f"{CENSUS_BASE}/{CENSUS_YEAR}/acs/acs5"
    params = {
        "get": "NAME,B01003_001E",
        "for": "metropolitan statistical area/micropolitan statistical area:*",
    }
    if api_key:
        params["key"] = api_key
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        rows = resp.json()
    except Exception as exc:
        log.error("Census API request failed: %s", exc)
        return None

    header, *data = rows
    df = pd.DataFrame(data, columns=header)
    df = df.rename(columns={
        "NAME": "msa_name",
        "B01003_001E": "population",
        "metropolitan statistical area/micropolitan statistical area": "cbsa_code",
    })
    df["population"] = pd.to_numeric(df["population"], errors="coerce")
    df = df.dropna(subset=["population"])
    df["population"] = df["population"].astype(int)

    # Keep only MSAs (metro), not micropolitan – filter by name convention
    df = df[df["msa_name"].str.contains("Metro", case=False, na=False)].copy()

    # Clean MSA names: strip " Metro Area" suffix
    df["msa_name"] = df["msa_name"].str.replace(r"\s*Metro(politan)?\s*Area$", "",
                                                  regex=True)

    df["state_abbr"] = df.apply(
        lambda r: _primary_state_abbr(r["cbsa_code"], r["msa_name"]), axis=1
    )
    df["census_region"] = df["state_abbr"].map(STATE_TO_REGION).fillna("Unknown")
    df = df.sort_values("population", ascending=False).reset_index(drop=True)
    log.info("  Fetched %d metro areas from Census API", len(df))
    return df[["cbsa_code", "msa_name", "population", "state_abbr", "census_region"]]


# ── Offline fallback (top ~200 MSAs from 2023 ACS estimates) ────────────────
def _builtin_msa_list() -> pd.DataFrame:
    """Hardcoded top MSAs so the pipeline works without an API key."""
    rows = [
        ("35620", "New York-Newark-Jersey City, NY-NJ-PA", 19_498_000, "NY", "Northeast"),
        ("31080", "Los Angeles-Long Beach-Anaheim, CA", 12_872_000, "CA", "West"),
        ("16980", "Chicago-Naperville-Elgin, IL-IN-WI", 9_262_000, "IL", "Midwest"),
        ("19100", "Dallas-Fort Worth-Arlington, TX", 8_100_000, "TX", "South"),
        ("26420", "Houston-The Woodlands-Sugar Land, TX", 7_340_000, "TX", "South"),
        ("47900", "Washington-Arlington-Alexandria, DC-VA-MD-WV", 6_356_000, "DC", "South"),
        ("33100", "Miami-Fort Lauderdale-Pompano Beach, FL", 6_183_000, "FL", "South"),
        ("37980", "Philadelphia-Camden-Wilmington, PA-NJ-DE-MD", 6_246_000, "PA", "Northeast"),
        ("12060", "Atlanta-Sandy Springs-Alpharetta, GA", 6_245_000, "GA", "South"),
        ("14460", "Boston-Cambridge-Newton, MA-NH", 4_941_000, "MA", "Northeast"),
        ("38060", "Phoenix-Mesa-Chandler, AZ", 5_070_000, "AZ", "West"),
        ("41860", "San Francisco-Oakland-Berkeley, CA", 4_566_000, "CA", "West"),
        ("40140", "Riverside-San Bernardino-Ontario, CA", 4_688_000, "CA", "West"),
        ("19820", "Detroit-Warren-Dearborn, MI", 4_340_000, "MI", "Midwest"),
        ("42660", "Seattle-Tacoma-Bellevue, WA", 4_034_000, "WA", "West"),
        ("33460", "Minneapolis-St. Paul-Bloomington, MN-WI", 3_712_000, "MN", "Midwest"),
        ("41740", "San Diego-Chula Vista-Carlsbad, CA", 3_276_000, "CA", "West"),
        ("45300", "Tampa-St. Petersburg-Clearwater, FL", 3_342_000, "FL", "South"),
        ("19740", "Denver-Aurora-Lakewood, CO", 2_986_000, "CO", "West"),
        ("41180", "St. Louis, MO-IL", 2_797_000, "MO", "Midwest"),
        ("12580", "Baltimore-Columbia-Towson, MD", 2_834_000, "MD", "South"),
        ("36740", "Orlando-Kissimmee-Sanford, FL", 2_817_000, "FL", "South"),
        ("16740", "Charlotte-Concord-Gastonia, NC-SC", 2_760_000, "NC", "South"),
        ("41700", "San Antonio-New Braunfels, TX", 2_600_000, "TX", "South"),
        ("38900", "Portland-Vancouver-Hillsboro, OR-WA", 2_510_000, "OR", "West"),
        ("40900", "Sacramento-Roseville-Folsom, CA", 2_420_000, "CA", "West"),
        ("38300", "Pittsburgh, PA", 2_343_000, "PA", "Northeast"),
        ("12420", "Austin-Round Rock-Georgetown, TX", 2_470_000, "TX", "South"),
        ("28140", "Kansas City, MO-KS", 2_210_000, "MO", "Midwest"),
        ("17460", "Cleveland-Elyria, OH", 2_058_000, "OH", "Midwest"),
        ("18140", "Columbus, OH", 2_180_000, "OH", "Midwest"),
        ("26900", "Indianapolis-Carmel-Anderson, IN", 2_140_000, "IN", "Midwest"),
        ("29820", "Las Vegas-Henderson-Paradise, NV", 2_330_000, "NV", "West"),
        ("34980", "Nashville-Davidson--Murfreesboro--Franklin, TN", 2_060_000, "TN", "South"),
        ("47260", "Virginia Beach-Norfolk-Newport News, VA-NC", 1_810_000, "VA", "South"),
        ("39300", "Providence-Warwick, RI-MA", 1_630_000, "RI", "Northeast"),
        ("27260", "Jacksonville, FL", 1_660_000, "FL", "South"),
        ("33340", "Milwaukee-Waukesha, WI", 1_560_000, "WI", "Midwest"),
        ("36420", "Oklahoma City, OK", 1_470_000, "OK", "South"),
        ("39580", "Raleigh-Cary, NC", 1_510_000, "NC", "South"),
        ("32820", "Memphis, TN-MS-AR", 1_340_000, "TN", "South"),
        ("40060", "Richmond, VA", 1_330_000, "VA", "South"),
        ("35380", "New Orleans-Metairie, LA", 1_270_000, "LA", "South"),
        ("31140", "Louisville/Jefferson County, KY-IN", 1_300_000, "KY", "South"),
        ("41620", "Salt Lake City, UT", 1_270_000, "UT", "West"),
        ("24340", "Grand Rapids-Kentwood, MI", 1_100_000, "MI", "Midwest"),
        ("13820", "Birmingham-Hoover, AL", 1_110_000, "AL", "South"),
        ("15380", "Buffalo-Cheektowaga, NY", 1_120_000, "NY", "Northeast"),
        ("25540", "Hartford-East Hartford-Middletown, CT", 1_200_000, "CT", "Northeast"),
        ("40380", "Rochester, NY", 1_080_000, "NY", "Northeast"),
        # Medium MSAs (500K–1M)
        ("46060", "Tucson, AZ", 1_050_000, "AZ", "West"),
        ("46140", "Tulsa, OK", 1_040_000, "OK", "South"),
        ("24860", "Greenville-Anderson, SC", 950_000, "SC", "South"),
        ("26620", "Huntsville, AL", 510_000, "AL", "South"),
        ("16860", "Chattanooga, TN-GA", 580_000, "TN", "South"),
        ("21340", "El Paso, TX", 870_000, "TX", "South"),
        ("10740", "Albuquerque, NM", 920_000, "NM", "West"),
        ("36540", "Omaha-Council Bluffs, NE-IA", 970_000, "NE", "Midwest"),
        ("44700", "Stockton, CA", 790_000, "CA", "West"),
        ("17900", "Columbia, SC", 850_000, "SC", "South"),
        ("30460", "Lexington-Fayette, KY", 530_000, "KY", "South"),
        # Small MSAs (<500K)
        ("30700", "Lincoln, NE", 350_000, "NE", "Midwest"),
        ("43340", "Shreveport-Bossier City, LA", 390_000, "LA", "South"),
        ("22180", "Fayetteville, NC", 390_000, "NC", "South"),
        ("10580", "Albany-Schenectady-Troy, NY", 900_000, "NY", "Northeast"),
        ("44060", "Spokane-Spokane Valley, WA", 600_000, "WA", "West"),
        ("22020", "Fargo, ND-MN", 270_000, "ND", "Midwest"),
        ("14260", "Boise City, ID", 810_000, "ID", "West"),
        ("11700", "Asheville, NC", 480_000, "NC", "South"),
        ("30020", "Lawton, OK", 125_000, "OK", "South"),
        ("48900", "Wilmington, NC", 310_000, "NC", "South"),
        ("25860", "Hickory-Lenoir-Morganton, NC", 370_000, "NC", "South"),
        ("20500", "Durham-Chapel Hill, NC", 650_000, "NC", "South"),
    ]
    df = pd.DataFrame(rows, columns=[
        "cbsa_code", "msa_name", "population", "state_abbr", "census_region",
    ])
    return df.sort_values("population", ascending=False).reset_index(drop=True)
