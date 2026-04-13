"""Statistical comparison utilities for real vs synthetic data."""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy import stats


def compare_distributions(
    real_col: pd.Series,
    synthetic_col: pd.Series,
    column_name: str,
) -> dict:
    """Compare distribution of a single column between real and synthetic data."""
    result = {
        "column": column_name,
        "test": None,
        "statistic": None,
        "p_value": None,
        "passed": True,
        "message": "",
    }

    # Drop nulls for comparison
    real_clean = real_col.dropna()
    synthetic_clean = synthetic_col.dropna()

    if len(real_clean) == 0 or len(synthetic_clean) == 0:
        result["message"] = "Insufficient data for comparison"
        return result

    if pd.api.types.is_numeric_dtype(real_col):
        # Kolmogorov-Smirnov test for continuous data
        stat, p_value = stats.ks_2samp(real_clean.astype(float), synthetic_clean.astype(float))
        result["test"] = "ks_2samp"
        result["statistic"] = float(stat)
        result["p_value"] = float(p_value)
        result["passed"] = p_value > 0.05
        result["message"] = (
            f"KS test: statistic={stat:.4f}, p-value={p_value:.4f}. "
            f"{'Distributions are similar' if p_value > 0.05 else 'Distributions differ significantly'}"
        )
    else:
        # Chi-square test for categorical data
        real_counts = real_clean.value_counts()
        synthetic_counts = synthetic_clean.value_counts()

        # Align categories
        all_categories = set(real_counts.index) | set(synthetic_counts.index)
        real_freq = [real_counts.get(c, 0) for c in all_categories]
        synthetic_freq = [synthetic_counts.get(c, 0) for c in all_categories]

        # Normalize to expected frequencies
        total_real = sum(real_freq)
        total_synth = sum(synthetic_freq)
        if total_real > 0 and total_synth > 0:
            expected = [f * total_synth / total_real for f in real_freq]
            # Avoid zero expected frequencies
            expected = [max(e, 0.001) for e in expected]

            try:
                stat, p_value = stats.chisquare(synthetic_freq, f_exp=expected)
                result["test"] = "chi_square"
                result["statistic"] = float(stat)
                result["p_value"] = float(p_value)
                result["passed"] = p_value > 0.05
                result["message"] = (
                    f"Chi-square test: statistic={stat:.4f}, p-value={p_value:.4f}. "
                    f"{'Category distributions are similar' if p_value > 0.05 else 'Category distributions differ'}"
                )
            except Exception as e:
                result["message"] = f"Chi-square test failed: {str(e)}"

    return result


def check_privacy_leakage(
    real_df: pd.DataFrame,
    synthetic_df: pd.DataFrame,
    table_name: str,
) -> list[dict]:
    """Check for potential privacy leakage between real and synthetic data."""
    issues = []

    # Check for exact row matches
    common_cols = list(set(real_df.columns) & set(synthetic_df.columns))
    if not common_cols:
        return issues

    # Convert to string for comparison
    real_str = real_df[common_cols].astype(str).apply(lambda row: "|".join(row), axis=1)
    synth_str = synthetic_df[common_cols].astype(str).apply(lambda row: "|".join(row), axis=1)

    exact_matches = set(real_str) & set(synth_str)
    if exact_matches:
        issues.append({
            "check_name": "exact_row_match",
            "passed": False,
            "severity": "semantic",
            "message": f"Table {table_name}: {len(exact_matches)} exact row matches found between real and synthetic data",
            "details": {"match_count": len(exact_matches)},
        })

    return issues
