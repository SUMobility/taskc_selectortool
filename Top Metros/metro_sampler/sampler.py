"""
Stratified sampling engine.

Steps:
1. Assign each MSA to population-size and transit-characteristic strata.
2. Force-include the top N MSAs by population.
3. Proportionally allocate remaining sample slots across strata.
4. Random-sample within each stratum (reproducible seed).
5. Compute sample weights for later analysis.
"""
import logging
import numpy as np
import pandas as pd

from metro_sampler.config import (
    POP_STRATA,
    RANDOM_SEED,
    TARGET_SAMPLE_SIZE,
    MIN_SAMPLE_SIZE,
    MAX_SAMPLE_SIZE,
    TOP_N_MANDATORY,
    MIN_POPULATION_COVERAGE,
)

log = logging.getLogger(__name__)


def assign_strata(msa_df: pd.DataFrame) -> pd.DataFrame:
    """Add stratum columns to the MSA frame."""
    df = msa_df.copy()

    # Population stratum
    def _pop_stratum(pop):
        for label, (lo, hi) in POP_STRATA.items():
            if lo <= pop < hi:
                return label
        return "Small"
    df["pop_stratum"] = df["population"].apply(_pop_stratum)

    # Rail stratum (should already be merged)
    if "has_rail" not in df.columns:
        df["has_rail"] = False
    df["rail_stratum"] = df["has_rail"].map({True: "Rail", False: "NoRail"})

    # Shared mobility
    if "has_shared_mobility" not in df.columns:
        df["has_shared_mobility"] = False
    df["sm_stratum"] = df["has_shared_mobility"].map({True: "SM", False: "NoSM"})

    # Composite stratum key
    df["stratum"] = (
        df["pop_stratum"] + "_" + df["rail_stratum"] + "_" +
        df["sm_stratum"] + "_" + df["census_region"]
    )
    return df


def select_sample(msa_df: pd.DataFrame) -> pd.DataFrame:
    """Return the final sample DataFrame with selection_method and weight."""
    df = assign_strata(msa_df)
    total_pop = df["population"].sum()

    # 1. Mandatory: top N by population
    mandatory = df.head(TOP_N_MANDATORY).copy()
    mandatory["selection_method"] = "mandatory_top10"

    # 2. Remaining MSAs available for random sampling
    remaining_pool = df.iloc[TOP_N_MANDATORY:].copy()
    slots_left = TARGET_SAMPLE_SIZE - len(mandatory)

    # Proportional allocation across composite strata
    rng = np.random.default_rng(RANDOM_SEED)
    stratum_counts = remaining_pool["stratum"].value_counts().to_dict()
    n_remaining = len(remaining_pool)

    # Initial proportional allocation, capped at stratum size
    n_strata = len(stratum_counts)
    allocation = {}
    for stratum, count in stratum_counts.items():
        raw = round(slots_left * count / n_remaining)
        # Only guarantee min-1 if we have enough slots for all strata
        if n_strata <= slots_left:
            raw = max(1, raw)
        allocation[stratum] = min(count, max(0, raw))

    # Iteratively redistribute until allocation == slots_left
    for _ in range(100):
        total = sum(allocation.values())
        if total == slots_left:
            break
        if total > slots_left:
            # Remove from strata with largest allocation first
            candidates = sorted(allocation, key=lambda s: allocation[s], reverse=True)
            for s in candidates:
                if allocation[s] > 0 and sum(allocation.values()) > slots_left:
                    allocation[s] -= 1
        else:
            # Add to strata that have room (available > allocated), largest first
            candidates = sorted(
                ((s, stratum_counts[s] - allocation[s]) for s in allocation
                 if allocation[s] < stratum_counts[s]),
                key=lambda x: x[1], reverse=True,
            )
            if not candidates:
                break  # all strata fully exhausted
            for s, room in candidates:
                if sum(allocation.values()) >= slots_left:
                    break
                allocation[s] += 1

    sampled_parts = []
    for stratum, n_pick in allocation.items():
        pool = remaining_pool[remaining_pool["stratum"] == stratum]
        n_pick = min(n_pick, len(pool))
        if n_pick > 0:
            picked = pool.sample(n=n_pick, random_state=int(rng.integers(1e9)))
            sampled_parts.append(picked)

    if sampled_parts:
        sampled = pd.concat(sampled_parts)
    else:
        sampled = pd.DataFrame(columns=df.columns)
    sampled["selection_method"] = "stratified_random"

    # 3. Combine
    sample = pd.concat([mandatory, sampled], ignore_index=True)

    # 4. Check coverage; add more if needed
    pop_coverage = sample["population"].sum() / total_pop
    if pop_coverage < MIN_POPULATION_COVERAGE:
        log.info("Coverage %.1f%% < target %.0f%% – adding metros",
                 pop_coverage * 100, MIN_POPULATION_COVERAGE * 100)
        needed = df[~df["cbsa_code"].isin(sample["cbsa_code"])].sort_values(
            "population", ascending=False)
        for _, row in needed.iterrows():
            if len(sample) >= MAX_SAMPLE_SIZE:
                break
            sample = pd.concat([sample, row.to_frame().T], ignore_index=True)
            sample.iloc[-1, sample.columns.get_loc("selection_method")] = "coverage_boost"
            pop_coverage = sample["population"].sum() / total_pop
            if pop_coverage >= MIN_POPULATION_COVERAGE:
                break

    # 5. Compute sample weights
    # Weight = (N_stratum / n_stratum) so each sampled MSA represents
    # N_stratum/n_stratum MSAs in its stratum.
    stratum_N = df["stratum"].value_counts().to_dict()
    stratum_n = sample["stratum"].value_counts().to_dict()
    sample["sample_weight"] = sample["stratum"].apply(
        lambda s: stratum_N.get(s, 1) / stratum_n.get(s, 1)
    )
    # Mandatory metros get weight = 1 (certainty selections)
    sample.loc[sample["selection_method"] == "mandatory_top10", "sample_weight"] = 1.0

    sample = sample.sort_values("population", ascending=False).reset_index(drop=True)
    log.info("Final sample: %d MSAs covering %.1f%% of metro population",
             len(sample), sample["population"].sum() / total_pop * 100)
    return sample
