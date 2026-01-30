#!/usr/bin/env python3
"""
MSA Sampling Pipeline – main entry point.

Usage:
    python run_sampling.py                   # uses fallback data
    CENSUS_API_KEY=xxx python run_sampling.py # uses live Census API

Outputs written to ./output/
"""
import logging
import sys
import pandas as pd

from metro_sampler.config import OUTPUT_DIR
from metro_sampler.data_census import fetch_msa_population
from metro_sampler.data_ntd import load_ntd_agencies, agencies_by_cbsa
from metro_sampler.data_gbfs import fetch_gbfs_systems, gbfs_by_cbsa
from metro_sampler.sampler import select_sample
from metro_sampler.reporting import save_sample_csv, summary_report, save_report, plot_map

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
log = logging.getLogger("pipeline")


def main():
    # ── 1. Acquire data ─────────────────────────────────────────────────
    log.info("Step 1: Fetching MSA populations")
    msa = fetch_msa_population()
    log.info("  %d MSAs loaded, total pop = %s", len(msa), f"{msa['population'].sum():,}")

    log.info("Step 2: Loading transit agency data (NTD)")
    agencies = load_ntd_agencies()
    agency_agg = agencies_by_cbsa(agencies)
    log.info("  %d agencies across %d CBSAs", len(agencies), len(agency_agg))

    log.info("Step 3: Fetching shared-mobility systems (GBFS)")
    gbfs = fetch_gbfs_systems()
    gbfs_agg = gbfs_by_cbsa(gbfs)
    log.info("  %d systems matched to %d CBSAs", len(gbfs), len(gbfs_agg))

    # ── 2. Merge into unified MSA frame ─────────────────────────────────
    log.info("Step 4: Merging datasets")
    merged = msa.merge(agency_agg, on="cbsa_code", how="left")
    merged = merged.merge(gbfs_agg, on="cbsa_code", how="left")

    # Fill NAs from merge
    merged["n_agencies"] = merged["n_agencies"].fillna(0).astype(int)
    merged["agency_list"] = merged["agency_list"].fillna("")
    merged["has_rail"] = merged["has_rail"].fillna(False).infer_objects(copy=False)
    merged["n_shared_mobility"] = merged["n_shared_mobility"].fillna(0).astype(int)
    merged["shared_mobility_list"] = merged["shared_mobility_list"].fillna("")
    merged["has_shared_mobility"] = merged["has_shared_mobility"].fillna(False).infer_objects(copy=False)

    # ── 3. Data quality checks ──────────────────────────────────────────
    log.info("Step 5: Data quality checks")
    dq_issues = []
    if merged["population"].isna().any():
        dq_issues.append("Some MSAs have missing population")
    if (merged["n_agencies"] == 0).sum() > len(merged) * 0.5:
        dq_issues.append("More than 50%% of MSAs have no matched transit agencies")
    dupes = merged["cbsa_code"].duplicated().sum()
    if dupes:
        dq_issues.append(f"{dupes} duplicate CBSA codes")
    if dq_issues:
        for issue in dq_issues:
            log.warning("  DQ: %s", issue)
    else:
        log.info("  All checks passed")

    # ── 4. Sample ───────────────────────────────────────────────────────
    log.info("Step 6: Running stratified sample selection")
    sample = select_sample(merged)

    # ── 5. Output ───────────────────────────────────────────────────────
    log.info("Step 7: Writing outputs")
    csv_path = save_sample_csv(sample)
    report_text = summary_report(sample, merged)
    report_path = save_report(report_text)
    print("\n" + report_text + "\n")

    map_path = plot_map(sample)

    log.info("Done. Outputs in %s", OUTPUT_DIR)
    log.info("  CSV:    %s", csv_path)
    log.info("  Report: %s", report_path)
    if map_path:
        log.info("  Map:    %s", map_path)

    return sample


if __name__ == "__main__":
    main()
