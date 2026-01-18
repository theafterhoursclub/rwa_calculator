"""
CRR Credit Conversion Factors (CRR Art. 111).

Provides CCF lookup tables as Polars DataFrames for efficient joins
in the RWA calculation pipeline.

Reference:
    CRR Art. 111: Off-balance sheet items
"""

from decimal import Decimal

import polars as pl

from rwa_calc.domain.enums import CommitmentType


# =============================================================================
# CCF BY CATEGORY (CRR Art. 111)
# =============================================================================

# CRR uses four CCF categories: 0%, 20%, 50%, 100%
CCF_TABLE: dict[str, Decimal] = {
    # 0% CCF - Low risk
    "low_risk": Decimal("0.00"),
    "unconditionally_cancellable": Decimal("0.00"),

    # 20% CCF - Medium-low risk
    "medium_low_risk": Decimal("0.20"),
    "documentary_credit": Decimal("0.20"),
    "short_term_lc": Decimal("0.20"),
    "undrawn_short_term": Decimal("0.20"),     # <= 1 year original maturity

    # 50% CCF - Medium risk
    "medium_risk": Decimal("0.50"),
    "undrawn_long_term": Decimal("0.50"),      # > 1 year original maturity
    "performance_guarantee": Decimal("0.50"),
    "bid_bond": Decimal("0.50"),
    "nif_ruf": Decimal("0.50"),
    "standby_lc": Decimal("0.50"),

    # 100% CCF - Full risk
    "full_risk": Decimal("1.00"),
    "guarantee_given": Decimal("1.00"),
    "acceptance": Decimal("1.00"),
    "credit_derivative": Decimal("1.00"),
    "forward_purchase": Decimal("1.00"),
}


# Mapping of common commitment types to CCF categories
CCF_TYPE_MAPPING: dict[str, str] = {
    # 0% CCF types
    "unconditionally_cancellable": "low_risk",
    "revocable": "low_risk",

    # 20% CCF types
    "documentary_lc": "documentary_credit",
    "trade_lc": "documentary_credit",
    "short_term_lc": "short_term_lc",
    "undrawn_revolver_short": "undrawn_short_term",

    # 50% CCF types
    "undrawn_revolver_long": "undrawn_long_term",
    "committed_undrawn": "undrawn_long_term",
    "rcf_undrawn": "undrawn_long_term",
    "performance_bond": "performance_guarantee",
    "warranty": "performance_guarantee",
    "nif": "nif_ruf",
    "ruf": "nif_ruf",
    "standby_letter_of_credit": "standby_lc",

    # 100% CCF types
    "guarantee": "guarantee_given",
    "financial_guarantee": "guarantee_given",
    "acceptance": "acceptance",
    "credit_derivative": "credit_derivative",
    "forward_asset_purchase": "forward_purchase",
}


def _create_ccf_df() -> pl.DataFrame:
    """Create CCF lookup DataFrame."""
    rows = []

    # Add all CCF categories with their values
    ccf_categories = [
        # Category, CCF value, Description
        ("low_risk", 0.00, "Low risk - unconditionally cancellable"),
        ("unconditionally_cancellable", 0.00, "Unconditionally cancellable commitments"),
        ("medium_low_risk", 0.20, "Medium-low risk"),
        ("documentary_credit", 0.20, "Documentary letters of credit"),
        ("short_term_lc", 0.20, "Short-term letters of credit"),
        ("undrawn_short_term", 0.20, "Undrawn facilities <= 1 year"),
        ("medium_risk", 0.50, "Medium risk"),
        ("undrawn_long_term", 0.50, "Undrawn facilities > 1 year"),
        ("performance_guarantee", 0.50, "Performance guarantees"),
        ("bid_bond", 0.50, "Bid bonds"),
        ("nif_ruf", 0.50, "Note issuance / revolving underwriting facilities"),
        ("standby_lc", 0.50, "Standby letters of credit"),
        ("full_risk", 1.00, "Full risk"),
        ("guarantee_given", 1.00, "Guarantees given"),
        ("acceptance", 1.00, "Acceptances"),
        ("credit_derivative", 1.00, "Credit derivatives"),
        ("forward_purchase", 1.00, "Forward purchases"),
    ]

    for category, ccf, description in ccf_categories:
        rows.append({
            "ccf_category": category,
            "ccf": ccf,
            "description": description,
        })

    return pl.DataFrame(rows).with_columns([
        pl.col("ccf").cast(pl.Float64),
    ])


def get_ccf_table() -> pl.DataFrame:
    """
    Get CCF lookup table as DataFrame.

    Returns:
        DataFrame with columns: ccf_category, ccf, description
    """
    return _create_ccf_df()


def get_ccf_by_maturity_table() -> pl.DataFrame:
    """
    Get simplified CCF table based on original maturity.

    This is useful for joining when only maturity information is available.

    Returns:
        DataFrame with columns: maturity_bucket, ccf
    """
    return pl.DataFrame({
        "maturity_bucket": ["short_term", "long_term", "unconditionally_cancellable"],
        "ccf": [0.20, 0.50, 0.00],
        "maturity_threshold_years": [1.0, None, None],
        "description": [
            "Original maturity <= 1 year",
            "Original maturity > 1 year",
            "Can be cancelled without conditions",
        ],
    }).with_columns([
        pl.col("ccf").cast(pl.Float64),
        pl.col("maturity_threshold_years").cast(pl.Float64),
    ])


def lookup_ccf(
    commitment_type: str,
    original_maturity_years: float | None = None,
) -> Decimal:
    """
    Look up CCF for commitment type.

    This is a convenience function for single lookups. For bulk processing,
    use the DataFrame tables with joins.

    Args:
        commitment_type: Type of off-balance sheet commitment
        original_maturity_years: Original maturity in years (for undrawn facilities)

    Returns:
        CCF as Decimal
    """
    # Direct lookup in CCF table
    if commitment_type.lower() in CCF_TABLE:
        return CCF_TABLE[commitment_type.lower()]

    # Try mapping to a category
    mapped = CCF_TYPE_MAPPING.get(commitment_type.lower())
    if mapped and mapped in CCF_TABLE:
        return CCF_TABLE[mapped]

    # Use maturity to determine CCF for undrawn facilities
    if original_maturity_years is not None:
        if original_maturity_years <= 1.0:
            return CCF_TABLE["undrawn_short_term"]
        else:
            return CCF_TABLE["undrawn_long_term"]

    # Default to medium risk (50%)
    return CCF_TABLE["medium_risk"]


def calculate_ead_off_balance_sheet(
    nominal_amount: Decimal,
    commitment_type: str,
    original_maturity_years: float | None = None,
) -> tuple[Decimal, Decimal, str]:
    """
    Calculate EAD for off-balance sheet item.

    Args:
        nominal_amount: Nominal/notional amount
        commitment_type: Type of commitment
        original_maturity_years: Original maturity in years

    Returns:
        Tuple of (ead, ccf, description)
    """
    ccf = lookup_ccf(commitment_type, original_maturity_years)
    ead = nominal_amount * ccf
    description = f"EAD = {nominal_amount:,.0f} x {ccf:.0%} CCF = {ead:,.0f}"
    return ead, ccf, description


def create_ccf_type_mapping_df() -> pl.DataFrame:
    """
    Create DataFrame mapping commitment types to CCF categories.

    Useful for joining source data with CCF lookup table.

    Returns:
        DataFrame with columns: commitment_type, ccf_category
    """
    rows = []
    for commit_type, category in CCF_TYPE_MAPPING.items():
        rows.append({
            "commitment_type": commit_type,
            "ccf_category": category,
        })

    return pl.DataFrame(rows)
