# MSA Sample Data Dictionary

This document describes the columns in `msa_sample.csv`.

## Geographic Identifiers

| Column | Description |
|--------|-------------|
| `cbsa_code` | Core Based Statistical Area code — the federal 5-digit identifier for the metro area (e.g., "35620" for New York) |
| `msa_name` | Full metro area name from Census Bureau (e.g., "New York-Newark-Jersey City, NY-NJ") |
| `state_abbr` | Primary state abbreviation, derived from the first state listed in the MSA name |
| `census_region` | Census Bureau region: Northeast, Midwest, South, or West. "Unknown" for territories like Puerto Rico |

## Population

| Column | Description |
|--------|-------------|
| `population` | Total population from American Community Survey (ACS) 5-year estimates. This is the metro area population, not city population |

## Stratification Variables

These columns show how each metro was categorized for sampling purposes.

| Column | Description |
|--------|-------------|
| `pop_stratum` | Population size category: **Mega** (5M+), **Large** (1-5M), **Medium** (500K-1M), **Small** (<500K) |
| `rail_stratum` | Whether the metro has rail transit: **Rail** or **NoRail** |
| `sm_stratum` | Whether the metro has shared mobility (bikeshare/scooter): **SM** or **NoSM** |
| `stratum` | Composite stratum key combining all four dimensions (e.g., "Large_Rail_SM_South") — used internally for proportional allocation |

## Transit Characteristics

| Column | Description |
|--------|-------------|
| `has_rail` | Boolean (`True`/`False`). True if metro has heavy rail, light rail, commuter rail, or streetcar service. Derived from NTD mode data |
| `n_agencies` | Count of transit agencies in this metro that report to the National Transit Database |
| `agency_list` | Semicolon-separated list of transit agency names (e.g., "MTA New York City Transit; NJ Transit; PATH") |

## Shared Mobility Characteristics

| Column | Description |
|--------|-------------|
| `has_shared_mobility` | Boolean (`True`/`False`). True if metro has at least one bikeshare or scooter system registered in the GBFS catalog |
| `n_shared_mobility` | Count of shared mobility operators in this metro |
| `shared_mobility_list` | Semicolon-separated list of operator names (e.g., "Citi Bike; Lime; Bird") |

## Sampling Metadata

| Column | Description |
|--------|-------------|
| `selection_method` | How this metro entered the sample: |
| | • **mandatory_top10** — automatically included as one of the top 10 metros by population |
| | • **stratified_random** — randomly selected from its stratum with probability proportional to stratum size |
| | • **coverage_boost** — added after random selection to meet the minimum population coverage threshold |
| `sample_weight` | Inverse-probability weight for statistical analysis. Indicates how many similar metros this one represents in its stratum |

## Using Sample Weights

When estimating national prevalence (e.g., "what share of US metros have a fare subsidy program?"), use `sample_weight` to account for the sampling design:

- **Mandatory metros** have weight = 1.0 (they represent only themselves)
- **Stratified random metros** have weight = N/n where N is the total metros in that stratum and n is how many were sampled
- **Coverage boost metros** have weight = 1.0

**Example:** If a stratum has 50 metros and we sampled 5, each sampled metro has weight = 10. If 3 of those 5 have a subsidy program, the weighted estimate is 30 metros with programs in that stratum (not 3).

## Data Sources

| Data | Source | Update Frequency |
|------|--------|------------------|
| MSA definitions & population | Census Bureau ACS 5-year estimates | Annual (each December) |
| Transit agencies | National Transit Database (NTD) | Annual |
| Rail presence | NTD mode data + curated fallback list | Annual |
| Shared mobility operators | MobilityData GBFS catalog | Continuously updated |
