"""
Configuration for MSA sampling pipeline.
Edit parameters here to adjust stratification, sample size, or data sources.
"""
from pathlib import Path

# ── Paths ───────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
NTD_DIR = DATA_DIR / "ntd"
OUTPUT_DIR = PROJECT_ROOT / "output"

# ── API keys / endpoints ────────────────────────────────────────────────────
CENSUS_API_KEY = "10d61a19b45bb3df4028dd0f74f39384c5b6d90e"  # set via env var CENSUS_API_KEY or paste here
CENSUS_BASE = "https://api.census.gov/data"
CENSUS_YEAR = 2023  # ACS year

GBFS_CATALOG_URL = "https://github.com/MobilityData/gbfs/raw/master/systems.csv"

TRANSITLAND_API_URL = "https://transit.land/api/v2/rest"
TRANSITLAND_API_KEY = ""  # optional

# ── Sampling parameters ─────────────────────────────────────────────────────
RANDOM_SEED = 42
TARGET_SAMPLE_SIZE = 50          # aim for 25-30
MIN_SAMPLE_SIZE = 45
MAX_SAMPLE_SIZE = 52
TOP_N_MANDATORY = 10             # include top N MSAs by population
MIN_POPULATION_COVERAGE = 0.5   # 45 % of total US metro pop

# ── Stratification bins ─────────────────────────────────────────────────────
POP_STRATA = {
    "Mega":   (5_000_000, float("inf")),
    "Large":  (1_000_000, 5_000_000),
    "Medium": (500_000,   1_000_000),
    "Small":  (0,         500_000),
}

CENSUS_REGIONS = {
    "Northeast": [
        "CT", "ME", "MA", "NH", "RI", "VT",  # New England
        "NJ", "NY", "PA",                      # Mid-Atlantic
    ],
    "Midwest": [
        "IL", "IN", "MI", "OH", "WI",          # East North Central
        "IA", "KS", "MN", "MO", "NE", "ND", "SD",
    ],
    "South": [
        "DE", "FL", "GA", "MD", "NC", "SC", "VA", "DC", "WV",
        "AL", "KY", "MS", "TN",
        "AR", "LA", "OK", "TX",
    ],
    "West": [
        "AZ", "CO", "ID", "MT", "NV", "NM", "UT", "WY",
        "AK", "CA", "HI", "OR", "WA",
    ],
}

# Invert for lookup: state_abbr -> region
STATE_TO_REGION = {}
for region, states in CENSUS_REGIONS.items():
    for st in states:
        STATE_TO_REGION[st] = region
