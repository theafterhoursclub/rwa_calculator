"""
Slotting Calculator for Specialised Lending RWA.

Implements CRR Art. 153(5) for supervisory slotting approach.
Supports both CRR and Basel 3.1 frameworks with appropriate risk weights.

Pipeline position:
    CRMProcessor -> SlottingCalculator -> OutputAggregator

Key responsibilities:
- Map slotting categories to risk weights
- Handle HVCRE (High Volatility Commercial Real Estate) distinction
- Calculate RWA = EAD × RW
- Build audit trail of calculations

Specialised Lending Types:
- Project Finance (PF)
- Object Finance (OF)
- Commodities Finance (CF)
- Income-Producing Real Estate (IPRE)
- High Volatility Commercial Real Estate (HVCRE)

References:
- CRR Art. 153(5): Supervisory slotting approach
- CRR Art. 147(8): Specialised lending definition
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

import polars as pl

from rwa_calc.contracts.bundles import CRMAdjustedBundle, SlottingResultBundle
from rwa_calc.contracts.errors import (
    CalculationError,
    ErrorCategory,
    ErrorSeverity,
    LazyFrameResult,
)
from rwa_calc.data.tables.crr_slotting import (
    get_slotting_table,
    lookup_slotting_rw,
    SLOTTING_RISK_WEIGHTS,
    SLOTTING_RISK_WEIGHTS_HVCRE,
)
from rwa_calc.domain.enums import SlottingCategory

if TYPE_CHECKING:
    from rwa_calc.contracts.config import CalculationConfig


@dataclass
class SlottingCalculationError:
    """Error during slotting calculation."""

    error_type: str
    message: str
    exposure_reference: str | None = None


class SlottingCalculator:
    """
    Calculate RWA using supervisory slotting approach for specialised lending.

    Implements SlottingCalculatorProtocol for CRR Art. 153(5).

    The slotting approach maps exposures to five categories:
    - Strong: 70% RW (CRR) / 50% RW (Basel 3.1 non-HVCRE)
    - Good: 70% RW (CRR) / 70% RW (Basel 3.1)
    - Satisfactory: 115% RW (CRR) / 100% RW (Basel 3.1)
    - Weak: 250% RW (CRR) / 150% RW (Basel 3.1)
    - Default: 0% RW (100% provisioned) / 350% RW (Basel 3.1)

    CRR Note: Same weights for HVCRE and non-HVCRE.
    Basel 3.1: Higher weights for HVCRE.

    Usage:
        calculator = SlottingCalculator()
        result = calculator.calculate(crm_bundle, config)
    """

    def __init__(self) -> None:
        """Initialize slotting calculator."""
        self._slotting_table: pl.DataFrame | None = None

    def calculate(
        self,
        data: CRMAdjustedBundle,
        config: CalculationConfig,
    ) -> LazyFrameResult:
        """
        Calculate RWA using supervisory slotting approach.

        Args:
            data: CRM-adjusted exposures (uses slotting_exposures)
            config: Calculation configuration

        Returns:
            LazyFrameResult with slotting RWA calculations
        """
        bundle = self.get_slotting_result_bundle(data, config)

        # Convert bundle errors to CalculationErrors
        calc_errors = [
            CalculationError(
                code="SLOTTING001",
                message=str(err),
                severity=ErrorSeverity.ERROR,
                category=ErrorCategory.CALCULATION,
            )
            for err in bundle.errors
        ]

        return LazyFrameResult(
            frame=bundle.results,
            errors=calc_errors,
        )

    def get_slotting_result_bundle(
        self,
        data: CRMAdjustedBundle,
        config: CalculationConfig,
    ) -> SlottingResultBundle:
        """
        Calculate slotting RWA and return as a bundle.

        Args:
            data: CRM-adjusted exposures
            config: Calculation configuration

        Returns:
            SlottingResultBundle with results and audit trail
        """
        errors: list[SlottingCalculationError] = []

        # Get slotting exposures (may be None)
        exposures = data.slotting_exposures

        # Handle case where there are no slotting exposures
        if exposures is None:
            empty_frame = pl.LazyFrame({
                "exposure_reference": pl.Series([], dtype=pl.String),
                "slotting_category": pl.Series([], dtype=pl.String),
                "is_hvcre": pl.Series([], dtype=pl.Boolean),
                "ead_final": pl.Series([], dtype=pl.Float64),
                "risk_weight": pl.Series([], dtype=pl.Float64),
                "rwa": pl.Series([], dtype=pl.Float64),
            })
            return SlottingResultBundle(
                results=empty_frame,
                calculation_audit=empty_frame,
                errors=[],
            )

        # Step 1: Ensure required columns exist
        exposures = self._prepare_columns(exposures, config)

        # Step 2: Look up risk weights based on slotting category
        exposures = self._apply_slotting_weights(exposures, config)

        # Step 3: Calculate RWA
        exposures = self._calculate_rwa(exposures)

        # Step 4: Build audit trail
        audit = self._build_audit(exposures)

        return SlottingResultBundle(
            results=exposures,
            calculation_audit=audit,
            errors=errors,
        )

    def _prepare_columns(
        self,
        exposures: pl.LazyFrame,
        config: CalculationConfig,
    ) -> pl.LazyFrame:
        """Ensure all required columns exist with defaults."""
        schema = exposures.collect_schema()

        # EAD
        if "ead_final" not in schema.names():
            if "ead" in schema.names():
                exposures = exposures.with_columns([
                    pl.col("ead").alias("ead_final"),
                ])
            else:
                exposures = exposures.with_columns([
                    pl.lit(0.0).alias("ead_final"),
                ])

        # Slotting category
        if "slotting_category" not in schema.names():
            exposures = exposures.with_columns([
                pl.lit("satisfactory").alias("slotting_category"),
            ])

        # HVCRE flag
        if "is_hvcre" not in schema.names():
            exposures = exposures.with_columns([
                pl.lit(False).alias("is_hvcre"),
            ])

        # Specialised lending type
        if "sl_type" not in schema.names():
            exposures = exposures.with_columns([
                pl.lit("project_finance").alias("sl_type"),
            ])

        return exposures

    def _apply_slotting_weights(
        self,
        exposures: pl.LazyFrame,
        config: CalculationConfig,
    ) -> pl.LazyFrame:
        """
        Apply slotting risk weights based on category and HVCRE flag.

        CRR has same weights for HVCRE and non-HVCRE.
        Basel 3.1 has differentiated HVCRE weights.
        """
        if config.is_crr:
            # CRR weights (same for HVCRE and non-HVCRE)
            return exposures.with_columns([
                pl.when(pl.col("slotting_category").str.to_lowercase() == "strong")
                .then(pl.lit(0.70))
                .when(pl.col("slotting_category").str.to_lowercase() == "good")
                .then(pl.lit(0.70))
                .when(pl.col("slotting_category").str.to_lowercase() == "satisfactory")
                .then(pl.lit(1.15))
                .when(pl.col("slotting_category").str.to_lowercase() == "weak")
                .then(pl.lit(2.50))
                .when(pl.col("slotting_category").str.to_lowercase() == "default")
                .then(pl.lit(0.00))
                .otherwise(pl.lit(1.15))  # Default to satisfactory
                .alias("risk_weight"),
            ])
        else:
            # Basel 3.1 weights (different for HVCRE)
            return exposures.with_columns([
                pl.when(
                    ~pl.col("is_hvcre") &
                    (pl.col("slotting_category").str.to_lowercase() == "strong")
                ).then(pl.lit(0.50))
                .when(
                    ~pl.col("is_hvcre") &
                    (pl.col("slotting_category").str.to_lowercase() == "good")
                ).then(pl.lit(0.70))
                .when(
                    ~pl.col("is_hvcre") &
                    (pl.col("slotting_category").str.to_lowercase() == "satisfactory")
                ).then(pl.lit(1.00))
                .when(
                    ~pl.col("is_hvcre") &
                    (pl.col("slotting_category").str.to_lowercase() == "weak")
                ).then(pl.lit(1.50))
                .when(
                    ~pl.col("is_hvcre") &
                    (pl.col("slotting_category").str.to_lowercase() == "default")
                ).then(pl.lit(3.50))
                # HVCRE weights
                .when(
                    pl.col("is_hvcre") &
                    (pl.col("slotting_category").str.to_lowercase() == "strong")
                ).then(pl.lit(0.70))
                .when(
                    pl.col("is_hvcre") &
                    (pl.col("slotting_category").str.to_lowercase() == "good")
                ).then(pl.lit(0.95))
                .when(
                    pl.col("is_hvcre") &
                    (pl.col("slotting_category").str.to_lowercase() == "satisfactory")
                ).then(pl.lit(1.20))
                .when(
                    pl.col("is_hvcre") &
                    (pl.col("slotting_category").str.to_lowercase() == "weak")
                ).then(pl.lit(1.75))
                .when(
                    pl.col("is_hvcre") &
                    (pl.col("slotting_category").str.to_lowercase() == "default")
                ).then(pl.lit(3.50))
                .otherwise(pl.lit(1.00))  # Default to satisfactory non-HVCRE
                .alias("risk_weight"),
            ])

    def _calculate_rwa(
        self,
        exposures: pl.LazyFrame,
    ) -> pl.LazyFrame:
        """Calculate RWA = EAD × RW."""
        return exposures.with_columns([
            (pl.col("ead_final") * pl.col("risk_weight")).alias("rwa"),
            (pl.col("ead_final") * pl.col("risk_weight")).alias("rwa_final"),
        ])

    def _build_audit(
        self,
        exposures: pl.LazyFrame,
    ) -> pl.LazyFrame:
        """Build slotting calculation audit trail."""
        schema = exposures.collect_schema()
        available_cols = schema.names()

        # Select available audit columns
        select_cols = ["exposure_reference"]
        optional_cols = [
            "counterparty_reference",
            "exposure_class",
            "sl_type",
            "slotting_category",
            "is_hvcre",
            "ead_final",
            "risk_weight",
            "rwa",
        ]

        for col in optional_cols:
            if col in available_cols:
                select_cols.append(col)

        audit = exposures.select(select_cols)

        # Add calculation string
        audit = audit.with_columns([
            pl.concat_str([
                pl.lit("Slotting: Category="),
                pl.col("slotting_category"),
                pl.when(pl.col("is_hvcre"))
                .then(pl.lit(" (HVCRE)"))
                .otherwise(pl.lit("")),
                pl.lit(", RW="),
                (pl.col("risk_weight") * 100).round(0).cast(pl.String),
                pl.lit("%, RWA="),
                pl.col("rwa").round(0).cast(pl.String),
            ]).alias("slotting_calculation"),
        ])

        return audit

    def calculate_single_exposure(
        self,
        ead: Decimal,
        category: str,
        is_hvcre: bool = False,
        sl_type: str = "project_finance",
        config: CalculationConfig | None = None,
    ) -> dict:
        """
        Calculate RWA for a single slotting exposure (convenience method).

        Args:
            ead: Exposure at default
            category: Slotting category (strong, good, satisfactory, weak, default)
            is_hvcre: Whether this is high-volatility commercial real estate
            sl_type: Specialised lending type
            config: Calculation configuration (defaults to CRR)

        Returns:
            Dictionary with calculation results
        """
        from datetime import date
        from rwa_calc.contracts.config import CalculationConfig

        if config is None:
            config = CalculationConfig.crr(reporting_date=date.today())

        # Look up risk weight
        if config.is_crr:
            risk_weight = lookup_slotting_rw(category, is_hvcre)
        else:
            # Basel 3.1 weights
            risk_weight = self._get_basel31_slotting_rw(category, is_hvcre)

        # Calculate RWA
        rwa = ead * risk_weight

        return {
            "ead": float(ead),
            "category": category,
            "is_hvcre": is_hvcre,
            "sl_type": sl_type,
            "risk_weight": float(risk_weight),
            "rwa": float(rwa),
            "framework": "CRR" if config.is_crr else "Basel 3.1",
        }

    def _get_basel31_slotting_rw(
        self,
        category: str,
        is_hvcre: bool,
    ) -> Decimal:
        """Get Basel 3.1 slotting risk weight."""
        cat_lower = category.lower()

        if not is_hvcre:
            # Non-HVCRE weights
            weights = {
                "strong": Decimal("0.50"),
                "good": Decimal("0.70"),
                "satisfactory": Decimal("1.00"),
                "weak": Decimal("1.50"),
                "default": Decimal("3.50"),
            }
        else:
            # HVCRE weights
            weights = {
                "strong": Decimal("0.70"),
                "good": Decimal("0.95"),
                "satisfactory": Decimal("1.20"),
                "weak": Decimal("1.75"),
                "default": Decimal("3.50"),
            }

        return weights.get(cat_lower, Decimal("1.00"))


def create_slotting_calculator() -> SlottingCalculator:
    """
    Create a slotting calculator instance.

    Returns:
        SlottingCalculator ready for use
    """
    return SlottingCalculator()
