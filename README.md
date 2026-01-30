# MSA Sampling Pipeline for Low-Income Fare Subsidy Research

Generates a stratified random sample of US metropolitan statistical areas (MSAs) for analyzing the national prevalence of low-income fare subsidy programs across transit agencies and shared mobility operators.

## Quick Start

```bash
python3 run_sampling.py
```

Outputs are written to `./output/`:
- `msa_sample.csv` — selected MSAs with agency lists, operator lists, strata, and sample weights
- `sample_report.txt` — summary statistics and coverage diagnostics
- `sample_map.png` — geographic visualization of selected metros

## How It Works

1. **Fetch the universe** — pulls all ~393 US metro areas and populations from the Census Bureau ACS API
2. **Enrich with transit data** — matches NTD-reported transit agencies to metros by cross-referencing UZA names against Census MSA names
3. **Enrich with shared mobility data** — fetches the MobilityData GBFS catalog to identify metros with active bikeshare/scooter systems
4. **Stratify** — assigns each MSA to a composite stratum based on population size, rail presence, shared mobility presence, and Census region
5. **Sample** — force-includes the top N metros by population, then proportionally allocates remaining sample slots across strata via random selection
6. **Validate** — checks population coverage against a minimum threshold, adding metros if needed
7. **Weight** — computes inverse-probability sample weights so each metro represents its stratum proportionally

## Configuration

All parameters are in `metro_sampler/config.py`:

| Parameter | Default | Description |
|---|---|---|
| `TARGET_SAMPLE_SIZE` | 50 | Target number of MSAs in the sample |
| `MIN_SAMPLE_SIZE` | 45 | Minimum acceptable sample size |
| `MAX_SAMPLE_SIZE` | 52 | Hard cap on sample size |
| `TOP_N_MANDATORY` | 10 | Number of largest metros always included |
| `MIN_POPULATION_COVERAGE` | 0.50 | Minimum share of total US metro population |
| `RANDOM_SEED` | 42 | Seed for reproducible random sampling |
| `CENSUS_YEAR` | 2023 | ACS vintage year |

### Population strata

| Stratum | Range |
|---|---|
| Mega | 5,000,000+ |
| Large | 1,000,000–5,000,000 |
| Medium | 500,000–1,000,000 |
| Small | <500,000 |

### Census regions

Northeast, Midwest, South, West (standard Census Bureau definitions).

## Data Sources

| Source | Method | What it provides |
|---|---|---|
| **Census Bureau ACS** | Live API (no key required) | MSA definitions, CBSA codes, populations |
| **NTD (National Transit Database)** | Local Excel file or Socrata API | Transit agency names, UZA assignments |
| **MobilityData GBFS catalog** | Live CSV download | Bikeshare and scooter operator presence |

### NTD Setup

Place the NTD Agency Information file in `./data/ntd/`:

```
data/ntd/2023 Agency Information.xlsx
```

Download from the [FTA NTD Data page](https://www.transit.dot.gov/ntd/ntd-data). The pipeline also works without this file (falls back to a curated list of ~60 major agencies).

### Census API Key (Optional)

The Census API works without a key for moderate usage. To set one:

```bash
export CENSUS_API_KEY=your_key_here
```

Or edit `CENSUS_API_KEY` in `config.py`.

## Project Structure

```
├── run_sampling.py              # Main entry point
├── metro_sampler/
│   ├── config.py                # All tunable parameters
│   ├── data_census.py           # Census API: MSA populations
│   ├── data_ntd.py              # NTD: transit agencies and modes
│   ├── data_gbfs.py             # GBFS: shared mobility operators
│   ├── sampler.py               # Stratification and sampling engine
│   └── reporting.py             # CSV, text report, and map output
├── data/ntd/                    # Place NTD files here
└── output/                      # Generated outputs
```

## Output CSV Columns

| Column | Description |
|---|---|
| `cbsa_code` | Federal CBSA/MSA identifier |
| `msa_name` | Metro area name |
| `population` | ACS population estimate |
| `state_abbr` | Primary state |
| `census_region` | Northeast / Midwest / South / West |
| `pop_stratum` | Mega / Large / Medium / Small |
| `rail_stratum` | Rail / NoRail |
| `sm_stratum` | SM / NoSM (shared mobility) |
| `stratum` | Composite stratum key |
| `has_rail` | Boolean: metro has rail transit |
| `has_shared_mobility` | Boolean: metro has GBFS systems |
| `n_agencies` | Count of NTD-reported transit agencies |
| `agency_list` | Semicolon-separated agency names |
| `n_shared_mobility` | Count of GBFS operators |
| `shared_mobility_list` | Semicolon-separated operator names |
| `selection_method` | `mandatory_top10`, `stratified_random`, or `coverage_boost` |
| `sample_weight` | Inverse-probability weight for analysis |

## Sample Weights

- **Mandatory metros** (top N by population): weight = 1.0 (certainty selections)
- **Stratified random**: weight = N_stratum / n_stratum (number of MSAs in stratum divided by number sampled from that stratum)
- **Coverage boost**: weight = 1.0

Use these weights when estimating national prevalence rates from the sample.

## Requirements

- Python 3.10+
- pandas
- numpy
- requests
- matplotlib (optional, for map)
- openpyxl (if using NTD Excel files)
