"""
Credit Risk Mitigation (CRM) processor for RWA calculator.

Applies all CRM techniques to exposures:
- CCF for off-balance sheet items
- Collateral haircuts and allocation
- Guarantee substitution
- Provision deduction

Classes:
    CRMProcessor: Main processor implementing CRMProcessorProtocol

Usage:
    from rwa_calc.engine.crm.processor import CRMProcessor

    processor = CRMProcessor()
    adjusted = processor.apply_crm(classified_data, config)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING

import polars as pl

from rwa_calc.contracts.bundles import (
    ClassifiedExposuresBundle,
    CRMAdjustedBundle,
)
from rwa_calc.contracts.errors import LazyFrameResult
from rwa_calc.domain.enums import ApproachType, ExposureClass
from rwa_calc.engine.ccf import CCFCalculator
from rwa_calc.engine.crm.haircuts import HaircutCalculator

if TYPE_CHECKING:
    from rwa_calc.contracts.config import CalculationConfig


@dataclass
class CRMError:
    """Error encountered during CRM processing."""

    error_type: str
    message: str
    exposure_reference: str | None = None
    context: dict = field(default_factory=dict)


class CRMProcessor:
    """
    Apply credit risk mitigation to exposures.

    Implements CRMProcessorProtocol for:
    - CCF application for off-balance sheet items (CRR Art. 111)
    - Collateral haircuts and allocation (CRR Art. 223-224)
    - Guarantee substitution (CRR Art. 213-215)
    - Provision deduction (CRR Art. 110)

    The CRM process follows this order:
    1. Apply CCF to calculate base EAD for contingents
    2. Apply collateral (reduce EAD for SA, reduce LGD for IRB)
    3. Apply guarantees (substitution approach)
    4. Deduct provisions from EAD
    """

    def __init__(self) -> None:
        """Initialize CRM processor with sub-calculators."""
        self._ccf_calculator = CCFCalculator()
        self._haircut_calculator = HaircutCalculator()

    def apply_crm(
        self,
        data: ClassifiedExposuresBundle,
        config: CalculationConfig,
    ) -> LazyFrameResult:
        """
        Apply credit risk mitigation to exposures.

        Args:
            data: Classified exposures from classifier
            config: Calculation configuration

        Returns:
            LazyFrameResult with CRM-adjusted exposures and any errors
        """
        bundle = self.get_crm_adjusted_bundle(data, config)

        # Convert to LazyFrameResult format
        return LazyFrameResult(
            frame=bundle.exposures,
            errors=[],  # CRMError objects would need conversion to CalculationError
        )

    def get_crm_adjusted_bundle(
        self,
        data: ClassifiedExposuresBundle,
        config: CalculationConfig,
    ) -> CRMAdjustedBundle:
        """
        Apply CRM and return as a bundle.

        Args:
            data: Classified exposures from classifier
            config: Calculation configuration

        Returns:
            CRMAdjustedBundle with adjusted exposures
        """
        errors: list[CRMError] = []

        # Start with all exposures
        exposures = data.all_exposures

        # Step 1: Apply CCF to calculate EAD for contingents
        exposures = self._apply_ccf(exposures, config)

        # Step 2: Initialize EAD columns
        exposures = self._initialize_ead(exposures)

        # Note: Collateral, guarantees, and provisions would be applied here
        # but require the actual collateral/guarantee data from the bundle
        # For now, we pass through with placeholder columns

        # Step 3: Add CRM audit trail
        exposures = self._add_crm_audit(exposures)

        # Split by approach for output
        sa_exposures = exposures.filter(pl.col("approach") == ApproachType.SA.value)
        irb_exposures = exposures.filter(
            (pl.col("approach") == ApproachType.FIRB.value) |
            (pl.col("approach") == ApproachType.AIRB.value)
        )

        return CRMAdjustedBundle(
            exposures=exposures,
            sa_exposures=sa_exposures,
            irb_exposures=irb_exposures,
            crm_audit=self._build_crm_audit(exposures),
            collateral_allocation=None,  # Would be populated from collateral processing
            crm_errors=errors,
        )

    def _apply_ccf(
        self,
        exposures: pl.LazyFrame,
        config: CalculationConfig,
    ) -> pl.LazyFrame:
        """
        Apply CCF to off-balance sheet exposures.

        Args:
            exposures: All exposures
            config: Calculation configuration

        Returns:
            Exposures with CCF and ead_from_ccf columns
        """
        return self._ccf_calculator.apply_ccf(exposures, config)

    def _initialize_ead(
        self,
        exposures: pl.LazyFrame,
    ) -> pl.LazyFrame:
        """
        Initialize EAD columns.

        Sets up the EAD waterfall:
        - ead_gross: drawn + CCF-adjusted undrawn
        - ead_after_collateral: EAD after collateral
        - ead_after_guarantee: EAD after guarantee substitution
        - ead_after_provision: Final EAD after provision deduction
        """
        return exposures.with_columns([
            # Gross EAD = drawn + CCF-adjusted contingent
            pl.col("ead_pre_crm").alias("ead_gross"),

            # Initialize subsequent EAD columns (will be adjusted by CRM)
            pl.col("ead_pre_crm").alias("ead_after_collateral"),
            pl.col("ead_pre_crm").alias("ead_after_guarantee"),
            pl.col("ead_pre_crm").alias("ead_final"),

            # Initialize collateral-related columns
            pl.lit(0.0).alias("collateral_allocated"),
            pl.lit(0.0).alias("collateral_adjusted_value"),

            # Initialize guarantee-related columns
            pl.lit(0.0).alias("guarantee_amount"),
            pl.lit(None).cast(pl.String).alias("guarantor_reference"),
            pl.lit(None).cast(pl.Float64).alias("substitute_rw"),

            # Initialize provision-related columns
            pl.lit(0.0).alias("provision_allocated"),
            pl.lit(0.0).alias("provision_deducted"),

            # LGD for IRB (may be adjusted by collateral)
            pl.col("lgd").fill_null(0.45).alias("lgd_pre_crm"),
            pl.col("lgd").fill_null(0.45).alias("lgd_post_crm"),
        ])

    def apply_collateral(
        self,
        exposures: pl.LazyFrame,
        collateral: pl.LazyFrame,
        config: CalculationConfig,
    ) -> pl.LazyFrame:
        """
        Apply collateral to reduce EAD (SA) or LGD (IRB).

        Args:
            exposures: Exposures with ead_gross
            collateral: Collateral data
            config: Calculation configuration

        Returns:
            Exposures with collateral effects applied
        """
        # Apply haircuts to collateral
        adjusted_collateral = self._haircut_calculator.apply_haircuts(
            collateral, exposures, config
        )

        # Apply maturity mismatch
        adjusted_collateral = self._haircut_calculator.apply_maturity_mismatch(
            adjusted_collateral, exposures
        )

        # Aggregate collateral by beneficiary
        collateral_by_exposure = adjusted_collateral.group_by(
            "beneficiary_reference"
        ).agg([
            pl.col("value_after_haircut").sum().alias("total_collateral_adjusted"),
            pl.col("market_value").sum().alias("total_collateral_market"),
            pl.len().alias("collateral_count"),
        ])

        # Join collateral to exposures
        exposures = exposures.join(
            collateral_by_exposure,
            left_on="exposure_reference",
            right_on="beneficiary_reference",
            how="left",
        )

        # Fill nulls for exposures without collateral
        exposures = exposures.with_columns([
            pl.col("total_collateral_adjusted").fill_null(0.0).alias("collateral_adjusted_value"),
            pl.col("total_collateral_market").fill_null(0.0).alias("collateral_market_value"),
        ])

        # Apply collateral effect based on approach
        exposures = exposures.with_columns([
            # For SA: Reduce EAD by collateral (simple substitution)
            pl.when(pl.col("approach") == ApproachType.SA.value)
            .then(
                (pl.col("ead_gross") - pl.col("collateral_adjusted_value")).clip(lower_bound=0)
            )
            # For IRB: Keep EAD, but collateral affects LGD (handled elsewhere)
            .otherwise(pl.col("ead_gross"))
            .alias("ead_after_collateral"),
        ])

        return exposures

    def apply_guarantees(
        self,
        exposures: pl.LazyFrame,
        guarantees: pl.LazyFrame,
        counterparty_lookup: pl.LazyFrame,
        config: CalculationConfig,
    ) -> pl.LazyFrame:
        """
        Apply guarantee substitution.

        For guaranteed portion, substitute borrower RW with guarantor RW.

        Args:
            exposures: Exposures with EAD
            guarantees: Guarantee data
            counterparty_lookup: For guarantor risk weights
            config: Calculation configuration

        Returns:
            Exposures with guarantee effects applied
        """
        # Aggregate guarantees by beneficiary
        guarantees_by_exposure = guarantees.group_by(
            "beneficiary_reference"
        ).agg([
            pl.col("amount_covered").sum().alias("total_guarantee_amount"),
            pl.col("guarantor").first().alias("primary_guarantor"),
            pl.len().alias("guarantee_count"),
        ])

        # Join guarantees to exposures
        exposures = exposures.join(
            guarantees_by_exposure,
            left_on="exposure_reference",
            right_on="beneficiary_reference",
            how="left",
        )

        # Fill nulls
        exposures = exposures.with_columns([
            pl.col("total_guarantee_amount").fill_null(0.0).alias("guarantee_amount"),
            pl.col("primary_guarantor").alias("guarantor_reference"),
        ])

        # Calculate guaranteed vs unguaranteed portions
        exposures = exposures.with_columns([
            # Guaranteed amount (capped at EAD)
            pl.min_horizontal(
                pl.col("guarantee_amount"),
                pl.col("ead_after_collateral")
            ).alias("guaranteed_portion"),
        ])

        exposures = exposures.with_columns([
            # Unguaranteed portion
            (
                pl.col("ead_after_collateral") - pl.col("guaranteed_portion")
            ).alias("unguaranteed_portion"),
        ])

        return exposures

    def apply_provisions(
        self,
        exposures: pl.LazyFrame,
        provisions: pl.LazyFrame,
        config: CalculationConfig,
    ) -> pl.LazyFrame:
        """
        Apply provision deduction from EAD.

        For SA, specific provisions are deducted from EAD.
        For IRB, provisions are compared to EL for shortfall/excess.

        Args:
            exposures: Exposures with EAD
            provisions: Provision data
            config: Calculation configuration

        Returns:
            Exposures with provision effects applied
        """
        # Check if provisions frame has data
        # Aggregate provisions by exposure
        provisions_by_exposure = provisions.group_by(
            "exposure_reference"
        ).agg([
            pl.col("provision_amount").sum().alias("total_provision"),
            pl.col("provision_type").first().alias("primary_provision_type"),
        ])

        # Join provisions to exposures
        exposures = exposures.join(
            provisions_by_exposure,
            on="exposure_reference",
            how="left",
        )

        # Fill nulls
        exposures = exposures.with_columns([
            pl.col("total_provision").fill_null(0.0).alias("provision_allocated"),
        ])

        # Apply provision deduction for SA
        exposures = exposures.with_columns([
            pl.when(pl.col("approach") == ApproachType.SA.value)
            .then(
                (pl.col("ead_after_guarantee") - pl.col("provision_allocated")).clip(lower_bound=0)
            )
            .otherwise(pl.col("ead_after_guarantee"))
            .alias("ead_final"),
        ])

        return exposures

    def _add_crm_audit(
        self,
        exposures: pl.LazyFrame,
    ) -> pl.LazyFrame:
        """Add CRM processing audit trail."""
        return exposures.with_columns([
            pl.concat_str([
                pl.lit("EAD: gross="),
                pl.col("ead_gross").round(0).cast(pl.String),
                pl.lit("; coll="),
                pl.col("collateral_adjusted_value").round(0).cast(pl.String),
                pl.lit("; guar="),
                pl.col("guarantee_amount").round(0).cast(pl.String),
                pl.lit("; prov="),
                pl.col("provision_allocated").round(0).cast(pl.String),
                pl.lit("; final="),
                pl.col("ead_final").round(0).cast(pl.String),
            ]).alias("crm_calculation"),
        ])

    def _build_crm_audit(
        self,
        exposures: pl.LazyFrame,
    ) -> pl.LazyFrame:
        """Build CRM audit trail for reporting."""
        return exposures.select([
            pl.col("exposure_reference"),
            pl.col("counterparty_reference"),
            pl.col("approach"),
            pl.col("ead_gross"),
            pl.col("ead_from_ccf"),
            pl.col("ccf"),
            pl.col("collateral_adjusted_value"),
            pl.col("guarantee_amount"),
            pl.col("provision_allocated"),
            pl.col("ead_final"),
            pl.col("lgd_pre_crm"),
            pl.col("lgd_post_crm"),
            pl.col("crm_calculation"),
        ])


def create_crm_processor() -> CRMProcessor:
    """
    Create a CRM processor instance.

    Returns:
        CRMProcessor ready for use
    """
    return CRMProcessor()
