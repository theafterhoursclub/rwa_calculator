"""
Credit Conversion Factor (CCF) calculator for off-balance sheet items.

Calculates EAD for contingent exposures using regulatory CCFs:
- SA: CRR Article 111 (0%, 20%, 50%, 100%)
- F-IRB: CRR Article 166(8) (75% for undrawn commitments)

CCF is part of exposure measurement, not credit risk mitigation.
It converts nominal/notional amounts to credit-equivalent EAD.

Classes:
    CCFCalculator: Calculator for credit conversion factors

Usage:
    from rwa_calc.engine.ccf import CCFCalculator

    calculator = CCFCalculator()
    exposures_with_ead = calculator.apply_ccf(exposures, config)
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

import polars as pl

from rwa_calc.data.tables.crr_ccf import (
    CCF_TABLE,
    CCF_TYPE_MAPPING,
    FIRB_CCF_TABLE,
    get_ccf_table,
    get_firb_ccf_table,
    lookup_ccf,
    lookup_firb_ccf,
)
from rwa_calc.domain.enums import ApproachType

if TYPE_CHECKING:
    from rwa_calc.contracts.config import CalculationConfig


@dataclass
class CCFResult:
    """Result of CCF calculation for an exposure."""

    ccf: Decimal
    ccf_category: str
    ead_from_undrawn: Decimal
    description: str


class CCFCalculator:
    """
    Calculate credit conversion factors for off-balance sheet items.

    Implements CRR CCF rules:
    - SA (Art. 111): 0%, 20%, 50%, 100% by commitment type
    - F-IRB (Art. 166(8)): 75% for undrawn commitments (except 0% for cancellable)

    The approach determines which CCF table to use:
    - SA exposures use standard CCFs (0%, 20%, 50%, 100%)
    - F-IRB exposures use 75% for most undrawn commitments
    - A-IRB exposures use own estimates (passed through as-is)
    """

    def __init__(self) -> None:
        """Initialize CCF calculator with lookup tables."""
        self._sa_ccf_table = get_ccf_table()
        self._firb_ccf_table = get_firb_ccf_table()

    def apply_ccf(
        self,
        exposures: pl.LazyFrame,
        config: CalculationConfig,
    ) -> pl.LazyFrame:
        """
        Apply CCF to calculate EAD for off-balance sheet exposures.

        Uses approach-specific CCFs:
        - SA: Standard CCFs per CRR Art. 111 (0%, 20%, 50%, 100%)
        - F-IRB: 75% for undrawn per CRR Art. 166(8)
        - A-IRB: Own CCF estimates (not modified here)

        Args:
            exposures: Exposures with nominal_amount, ccf_category, and approach columns
            config: Calculation configuration

        Returns:
            LazyFrame with ead_from_ccf and ccf columns added
        """
        # Normalize ccf_category
        exposures = exposures.with_columns([
            pl.col("ccf_category").fill_null("").str.to_lowercase().alias("ccf_category_normalized"),
        ])

        # Join with SA CCF table
        sa_ccf_lookup = self._sa_ccf_table.lazy()
        exposures = exposures.join(
            sa_ccf_lookup.select([
                pl.col("ccf_category").str.to_lowercase().alias("lookup_category"),
                pl.col("ccf").alias("sa_ccf_rate"),
            ]),
            left_on="ccf_category_normalized",
            right_on="lookup_category",
            how="left",
        )

        # Join with F-IRB CCF table
        firb_ccf_lookup = self._firb_ccf_table.lazy()
        exposures = exposures.join(
            firb_ccf_lookup.select([
                pl.col("ccf_category").str.to_lowercase().alias("firb_lookup_category"),
                pl.col("ccf").alias("firb_ccf_rate"),
            ]),
            left_on="ccf_category_normalized",
            right_on="firb_lookup_category",
            how="left",
        )

        # Check if approach column exists, default to SA if not
        has_approach = "approach" in exposures.collect_schema().names()

        if has_approach:
            # Apply approach-specific CCF
            exposures = exposures.with_columns([
                pl.when(pl.col("nominal_amount") == 0)
                .then(pl.lit(0.0))  # Loans with no contingent - no CCF
                .when(pl.col("approach") == ApproachType.FIRB.value)
                .then(pl.col("firb_ccf_rate").fill_null(0.75))  # F-IRB: default 75%
                .when(pl.col("approach") == ApproachType.AIRB.value)
                .then(pl.col("sa_ccf_rate").fill_null(0.50))  # A-IRB: use SA as fallback
                .otherwise(pl.col("sa_ccf_rate").fill_null(0.50))  # SA: default 50%
                .alias("ccf"),
            ])
        else:
            # Default to SA CCF when approach not specified
            exposures = exposures.with_columns([
                pl.when(pl.col("nominal_amount") == 0)
                .then(pl.lit(0.0))  # Loans with no contingent - no CCF
                .otherwise(pl.col("sa_ccf_rate").fill_null(0.50))  # SA: default 50%
                .alias("ccf"),
            ])

        # Calculate EAD from undrawn/nominal amount
        exposures = exposures.with_columns([
            # EAD from CCF = nominal_amount * CCF
            (pl.col("nominal_amount") * pl.col("ccf")).alias("ead_from_ccf"),
        ])

        # Calculate total EAD (drawn + CCF-adjusted undrawn)
        exposures = exposures.with_columns([
            (pl.col("drawn_amount") + pl.col("ead_from_ccf")).alias("ead_pre_crm"),
        ])

        # Add CCF audit trail
        exposures = exposures.with_columns([
            pl.concat_str([
                pl.lit("CCF="),
                (pl.col("ccf") * 100).round(0).cast(pl.String),
                pl.lit("%; nominal="),
                pl.col("nominal_amount").round(0).cast(pl.String),
                pl.lit("; ead_ccf="),
                pl.col("ead_from_ccf").round(0).cast(pl.String),
            ]).alias("ccf_calculation"),
        ])

        # Clean up temporary columns
        exposures = exposures.drop([
            "ccf_category_normalized",
            "sa_ccf_rate",
            "firb_ccf_rate",
        ])

        return exposures

    def calculate_single_ccf(
        self,
        commitment_type: str,
        original_maturity_years: float | None = None,
    ) -> CCFResult:
        """
        Calculate CCF for a single exposure (convenience method).

        Args:
            commitment_type: Type of commitment/contingent
            original_maturity_years: Original maturity in years

        Returns:
            CCFResult with CCF details
        """
        ccf = lookup_ccf(commitment_type, original_maturity_years)

        # Determine category
        category = CCF_TYPE_MAPPING.get(
            commitment_type.lower(),
            "medium_risk" if original_maturity_years and original_maturity_years > 1 else "medium_low_risk"
        )

        description = f"CCF {ccf:.0%} for {commitment_type}"
        if original_maturity_years:
            description += f" (maturity: {original_maturity_years:.1f}y)"

        return CCFResult(
            ccf=ccf,
            ccf_category=category,
            ead_from_undrawn=Decimal("0"),  # Calculated separately
            description=description,
        )


def create_ccf_calculator() -> CCFCalculator:
    """
    Create a CCF calculator instance.

    Returns:
        CCFCalculator ready for use
    """
    return CCFCalculator()
