"""
CRR Supporting Factors for SA calculator (CRR2 Art. 501).

Applies SME and infrastructure supporting factors to RWA calculations.
These factors are CRR-specific and NOT available under Basel 3.1.

SME Supporting Factor - Tiered Approach (CRR2 Art. 501):
- Exposures up to EUR 2.5m (GBP 2.2m): factor of 0.7619
- Exposures above EUR 2.5m (GBP 2.2m): factor of 0.85

Formula:
    factor = [min(E, threshold) × 0.7619 + max(E - threshold, 0) × 0.85] / E

Infrastructure Supporting Factor (CRR Art. 501a):
- Qualifying infrastructure: factor of 0.75

References:
- CRR2 Art. 501 (EU 2019/876 amending EU 575/2013)
- CRR Art. 501a: Infrastructure supporting factor
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from rwa_calc.contracts.config import CalculationConfig


@dataclass
class SupportingFactorResult:
    """Result of supporting factor calculation."""

    factor: Decimal
    was_applied: bool
    description: str


class SupportingFactorCalculator:
    """
    Calculate SME and infrastructure supporting factors for CRR.

    The supporting factors reduce RWA for qualifying exposures:
    - SME: Tiered factor (0.7619 up to threshold, 0.85 above)
    - Infrastructure: Flat 0.75 factor

    Under Basel 3.1, these factors are not available (returns 1.0).
    """

    def calculate_sme_factor(
        self,
        total_exposure: Decimal,
        config: CalculationConfig,
    ) -> Decimal:
        """
        Calculate SME supporting factor based on total exposure.

        Args:
            total_exposure: Total exposure amount to the SME
            config: Calculation configuration

        Returns:
            Effective supporting factor (0.7619 to 0.85)
        """
        if not config.supporting_factors.enabled:
            return Decimal("1.0")

        if total_exposure <= 0:
            return Decimal("1.0")

        # Get thresholds and factors from config
        threshold_eur = config.supporting_factors.sme_exposure_threshold_eur
        threshold_gbp = threshold_eur * config.eur_gbp_rate

        factor_tier1 = config.supporting_factors.sme_factor_under_threshold
        factor_tier2 = config.supporting_factors.sme_factor_above_threshold

        # Use GBP threshold for GBP currency (default)
        threshold = threshold_gbp

        # Calculate tiered factor
        tier1_amount = min(total_exposure, threshold)
        tier2_amount = max(total_exposure - threshold, Decimal("0"))

        weighted_factor = (
            tier1_amount * factor_tier1 +
            tier2_amount * factor_tier2
        )

        return weighted_factor / total_exposure

    def calculate_infrastructure_factor(
        self,
        config: CalculationConfig,
    ) -> Decimal:
        """
        Get infrastructure supporting factor.

        Args:
            config: Calculation configuration

        Returns:
            Infrastructure factor (0.75 for CRR, 1.0 for Basel 3.1)
        """
        if not config.supporting_factors.enabled:
            return Decimal("1.0")

        return config.supporting_factors.infrastructure_factor

    def get_effective_factor(
        self,
        is_sme: bool,
        is_infrastructure: bool,
        total_exposure: Decimal,
        config: CalculationConfig,
    ) -> Decimal:
        """
        Get the most beneficial supporting factor.

        If both SME and infrastructure apply, returns the lower factor
        (more beneficial to the bank).

        Args:
            is_sme: Whether exposure qualifies for SME factor
            is_infrastructure: Whether exposure qualifies for infrastructure
            total_exposure: Total exposure amount (for SME tier calculation)
            config: Calculation configuration

        Returns:
            Most beneficial factor (lowest value)
        """
        if not config.supporting_factors.enabled:
            return Decimal("1.0")

        factors = [Decimal("1.0")]

        if is_sme:
            factors.append(self.calculate_sme_factor(total_exposure, config))

        if is_infrastructure:
            factors.append(self.calculate_infrastructure_factor(config))

        # Return lowest factor (most beneficial)
        return min(factors)

    def apply_factors(
        self,
        exposures: pl.LazyFrame,
        config: CalculationConfig,
    ) -> pl.LazyFrame:
        """
        Apply supporting factors to exposures LazyFrame.

        Expects columns:
        - is_sme: bool
        - is_infrastructure: bool
        - ead_final: float (exposure amount for tier calculation)
        - rwa_pre_factor: float (RWA before supporting factor)

        Adds columns:
        - supporting_factor: float
        - rwa_post_factor: float (RWA after supporting factor)
        - supporting_factor_applied: bool

        Args:
            exposures: Exposures with RWA calculated
            config: Calculation configuration

        Returns:
            Exposures with supporting factors applied
        """
        if not config.supporting_factors.enabled:
            # Basel 3.1: No supporting factors
            return exposures.with_columns([
                pl.lit(1.0).alias("supporting_factor"),
                pl.col("rwa_pre_factor").alias("rwa_post_factor"),
                pl.lit(False).alias("supporting_factor_applied"),
            ])

        # Get threshold in GBP
        threshold_eur = config.supporting_factors.sme_exposure_threshold_eur
        threshold_gbp = float(threshold_eur * config.eur_gbp_rate)

        factor_tier1 = float(config.supporting_factors.sme_factor_under_threshold)
        factor_tier2 = float(config.supporting_factors.sme_factor_above_threshold)
        infra_factor = float(config.supporting_factors.infrastructure_factor)

        # Check for required columns
        schema = exposures.collect_schema()
        has_sme = "is_sme" in schema.names()
        has_infra = "is_infrastructure" in schema.names()

        # Calculate SME tiered factor
        if has_sme:
            exposures = exposures.with_columns([
                # Tier 1 amount: min(exposure, threshold)
                pl.when(pl.col("ead_final") <= threshold_gbp)
                .then(pl.col("ead_final"))
                .otherwise(pl.lit(threshold_gbp))
                .alias("_tier1_amount"),

                # Tier 2 amount: max(exposure - threshold, 0)
                pl.when(pl.col("ead_final") > threshold_gbp)
                .then(pl.col("ead_final") - threshold_gbp)
                .otherwise(pl.lit(0.0))
                .alias("_tier2_amount"),
            ])

            # Calculate SME factor
            exposures = exposures.with_columns([
                pl.when(pl.col("is_sme") & (pl.col("ead_final") > 0))
                .then(
                    (pl.col("_tier1_amount") * factor_tier1 +
                     pl.col("_tier2_amount") * factor_tier2) /
                    pl.col("ead_final")
                )
                .otherwise(pl.lit(1.0))
                .alias("_sme_factor"),
            ])
        else:
            exposures = exposures.with_columns([
                pl.lit(1.0).alias("_sme_factor"),
            ])

        # Calculate infrastructure factor
        if has_infra:
            exposures = exposures.with_columns([
                pl.when(pl.col("is_infrastructure"))
                .then(pl.lit(infra_factor))
                .otherwise(pl.lit(1.0))
                .alias("_infra_factor"),
            ])
        else:
            exposures = exposures.with_columns([
                pl.lit(1.0).alias("_infra_factor"),
            ])

        # Get minimum (most beneficial) factor
        exposures = exposures.with_columns([
            pl.min_horizontal(
                pl.col("_sme_factor"),
                pl.col("_infra_factor"),
            ).alias("supporting_factor"),
        ])

        # Apply factor to RWA
        exposures = exposures.with_columns([
            (pl.col("rwa_pre_factor") * pl.col("supporting_factor"))
            .alias("rwa_post_factor"),

            (pl.col("supporting_factor") < 1.0).alias("supporting_factor_applied"),
        ])

        # Drop temporary columns
        exposures = exposures.drop([
            col for col in ["_tier1_amount", "_tier2_amount", "_sme_factor", "_infra_factor"]
            if col in exposures.collect_schema().names()
        ])

        return exposures


def create_supporting_factor_calculator() -> SupportingFactorCalculator:
    """Create a SupportingFactorCalculator instance."""
    return SupportingFactorCalculator()
