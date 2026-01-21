"""
Polars LazyFrame namespaces for Slotting calculations.

Provides fluent API for Specialised Lending slotting approach via registered namespaces:
- `lf.slotting.prepare_columns(config)` - Ensure required columns exist
- `lf.slotting.apply_slotting_weights(config)` - Apply slotting risk weights
- `lf.slotting.calculate_rwa()` - Calculate RWA

Usage:
    import polars as pl
    from rwa_calc.contracts.config import CalculationConfig
    import rwa_calc.engine.slotting.namespace  # Register namespace

    config = CalculationConfig.crr(reporting_date=date(2024, 12, 31))
    result = (exposures
        .slotting.prepare_columns(config)
        .slotting.apply_slotting_weights(config)
        .slotting.calculate_rwa()
    )

References:
- CRR Art. 153(5): Supervisory slotting approach
- CRR Art. 147(8): Specialised lending definition
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from rwa_calc.contracts.config import CalculationConfig


# =============================================================================
# SLOTTING RISK WEIGHTS
# =============================================================================

# CRR risk weights (same for HVCRE and non-HVCRE)
CRR_SLOTTING_WEIGHTS = {
    "strong": 0.70,
    "good": 0.70,
    "satisfactory": 1.15,
    "weak": 2.50,
    "default": 0.00,  # Fully provisioned
}

# Basel 3.1 risk weights (non-HVCRE)
BASEL31_SLOTTING_WEIGHTS = {
    "strong": 0.50,
    "good": 0.70,
    "satisfactory": 1.00,
    "weak": 1.50,
    "default": 3.50,
}

# Basel 3.1 risk weights (HVCRE)
BASEL31_SLOTTING_WEIGHTS_HVCRE = {
    "strong": 0.70,
    "good": 0.95,
    "satisfactory": 1.20,
    "weak": 1.75,
    "default": 3.50,
}


# =============================================================================
# LAZYFRAME NAMESPACE
# =============================================================================


@pl.api.register_lazyframe_namespace("slotting")
class SlottingLazyFrame:
    """
    Slotting calculation namespace for Polars LazyFrames.

    Provides fluent API for Specialised Lending slotting approach.

    Example:
        result = (exposures
            .slotting.prepare_columns(config)
            .slotting.apply_slotting_weights(config)
            .slotting.calculate_rwa()
        )
    """

    def __init__(self, lf: pl.LazyFrame) -> None:
        self._lf = lf

    # =========================================================================
    # PREPARATION METHODS
    # =========================================================================

    def prepare_columns(self, config: CalculationConfig) -> pl.LazyFrame:
        """
        Ensure all required columns exist with defaults.

        Adds/normalizes:
        - ead_final: Exposure at default
        - slotting_category: Slotting category
        - is_hvcre: HVCRE flag
        - sl_type: Specialised lending type

        Args:
            config: Calculation configuration

        Returns:
            LazyFrame with all required columns
        """
        schema = self._lf.collect_schema()
        lf = self._lf

        # EAD
        if "ead_final" not in schema.names():
            if "ead" in schema.names():
                lf = lf.with_columns([pl.col("ead").alias("ead_final")])
            elif "ead_pre_crm" in schema.names():
                lf = lf.with_columns([pl.col("ead_pre_crm").alias("ead_final")])
            else:
                lf = lf.with_columns([pl.lit(0.0).alias("ead_final")])

        # Refresh schema
        schema = lf.collect_schema()

        # Slotting category
        if "slotting_category" not in schema.names():
            lf = lf.with_columns([pl.lit("satisfactory").alias("slotting_category")])

        # HVCRE flag
        if "is_hvcre" not in schema.names():
            lf = lf.with_columns([pl.lit(False).alias("is_hvcre")])

        # Specialised lending type
        if "sl_type" not in schema.names():
            lf = lf.with_columns([pl.lit("project_finance").alias("sl_type")])

        return lf

    # =========================================================================
    # RISK WEIGHT APPLICATION
    # =========================================================================

    def apply_slotting_weights(self, config: CalculationConfig) -> pl.LazyFrame:
        """
        Apply slotting risk weights based on category and HVCRE flag.

        CRR has same weights for HVCRE and non-HVCRE.
        Basel 3.1 has differentiated HVCRE weights.

        Args:
            config: Calculation configuration

        Returns:
            LazyFrame with risk_weight column added
        """
        if config.is_crr:
            return self._apply_crr_weights()
        else:
            return self._apply_basel31_weights()

    def _apply_crr_weights(self) -> pl.LazyFrame:
        """Apply CRR slotting weights (same for HVCRE and non-HVCRE)."""
        return self._lf.with_columns([
            pl.when(pl.col("slotting_category").str.to_lowercase() == "strong")
            .then(pl.lit(CRR_SLOTTING_WEIGHTS["strong"]))
            .when(pl.col("slotting_category").str.to_lowercase() == "good")
            .then(pl.lit(CRR_SLOTTING_WEIGHTS["good"]))
            .when(pl.col("slotting_category").str.to_lowercase() == "satisfactory")
            .then(pl.lit(CRR_SLOTTING_WEIGHTS["satisfactory"]))
            .when(pl.col("slotting_category").str.to_lowercase() == "weak")
            .then(pl.lit(CRR_SLOTTING_WEIGHTS["weak"]))
            .when(pl.col("slotting_category").str.to_lowercase() == "default")
            .then(pl.lit(CRR_SLOTTING_WEIGHTS["default"]))
            .otherwise(pl.lit(CRR_SLOTTING_WEIGHTS["satisfactory"]))  # Default
            .alias("risk_weight"),
        ])

    def _apply_basel31_weights(self) -> pl.LazyFrame:
        """Apply Basel 3.1 slotting weights (different for HVCRE)."""
        return self._lf.with_columns([
            # Non-HVCRE weights
            pl.when(
                ~pl.col("is_hvcre") &
                (pl.col("slotting_category").str.to_lowercase() == "strong")
            ).then(pl.lit(BASEL31_SLOTTING_WEIGHTS["strong"]))
            .when(
                ~pl.col("is_hvcre") &
                (pl.col("slotting_category").str.to_lowercase() == "good")
            ).then(pl.lit(BASEL31_SLOTTING_WEIGHTS["good"]))
            .when(
                ~pl.col("is_hvcre") &
                (pl.col("slotting_category").str.to_lowercase() == "satisfactory")
            ).then(pl.lit(BASEL31_SLOTTING_WEIGHTS["satisfactory"]))
            .when(
                ~pl.col("is_hvcre") &
                (pl.col("slotting_category").str.to_lowercase() == "weak")
            ).then(pl.lit(BASEL31_SLOTTING_WEIGHTS["weak"]))
            .when(
                ~pl.col("is_hvcre") &
                (pl.col("slotting_category").str.to_lowercase() == "default")
            ).then(pl.lit(BASEL31_SLOTTING_WEIGHTS["default"]))

            # HVCRE weights
            .when(
                pl.col("is_hvcre") &
                (pl.col("slotting_category").str.to_lowercase() == "strong")
            ).then(pl.lit(BASEL31_SLOTTING_WEIGHTS_HVCRE["strong"]))
            .when(
                pl.col("is_hvcre") &
                (pl.col("slotting_category").str.to_lowercase() == "good")
            ).then(pl.lit(BASEL31_SLOTTING_WEIGHTS_HVCRE["good"]))
            .when(
                pl.col("is_hvcre") &
                (pl.col("slotting_category").str.to_lowercase() == "satisfactory")
            ).then(pl.lit(BASEL31_SLOTTING_WEIGHTS_HVCRE["satisfactory"]))
            .when(
                pl.col("is_hvcre") &
                (pl.col("slotting_category").str.to_lowercase() == "weak")
            ).then(pl.lit(BASEL31_SLOTTING_WEIGHTS_HVCRE["weak"]))
            .when(
                pl.col("is_hvcre") &
                (pl.col("slotting_category").str.to_lowercase() == "default")
            ).then(pl.lit(BASEL31_SLOTTING_WEIGHTS_HVCRE["default"]))

            # Default to satisfactory non-HVCRE
            .otherwise(pl.lit(BASEL31_SLOTTING_WEIGHTS["satisfactory"]))
            .alias("risk_weight"),
        ])

    # =========================================================================
    # RWA CALCULATION
    # =========================================================================

    def calculate_rwa(self) -> pl.LazyFrame:
        """
        Calculate RWA = EAD x Risk Weight.

        Returns:
            LazyFrame with rwa and rwa_final columns
        """
        return self._lf.with_columns([
            (pl.col("ead_final") * pl.col("risk_weight")).alias("rwa"),
            (pl.col("ead_final") * pl.col("risk_weight")).alias("rwa_final"),
        ])

    # =========================================================================
    # CONVENIENCE / PIPELINE METHODS
    # =========================================================================

    def apply_all(self, config: CalculationConfig) -> pl.LazyFrame:
        """
        Apply full slotting calculation pipeline.

        Steps:
        1. Prepare columns
        2. Apply slotting weights
        3. Calculate RWA

        Args:
            config: Calculation configuration

        Returns:
            LazyFrame with all slotting calculations
        """
        return (self._lf
            .slotting.prepare_columns(config)
            .slotting.apply_slotting_weights(config)
            .slotting.calculate_rwa()
        )

    def build_audit(self) -> pl.LazyFrame:
        """
        Build slotting calculation audit trail.

        Returns:
            LazyFrame with audit columns including slotting_calculation string
        """
        schema = self._lf.collect_schema()
        available_cols = schema.names()

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

        audit = self._lf.select(select_cols)

        # Add calculation string
        if "rwa" in available_cols:
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


# =============================================================================
# EXPRESSION NAMESPACE
# =============================================================================


@pl.api.register_expr_namespace("slotting")
class SlottingExpr:
    """
    Slotting calculation namespace for Polars Expressions.

    Provides column-level operations for slotting calculations.

    Example:
        df.with_columns(
            pl.col("slotting_category").slotting.lookup_rw(is_crr=True),
        )
    """

    def __init__(self, expr: pl.Expr) -> None:
        self._expr = expr

    def lookup_rw(self, is_crr: bool = True, is_hvcre: bool = False) -> pl.Expr:
        """
        Look up risk weight based on slotting category.

        Args:
            is_crr: Whether to use CRR weights (vs Basel 3.1)
            is_hvcre: Whether to use HVCRE weights (Basel 3.1 only)

        Returns:
            Expression with risk weight
        """
        if is_crr:
            weights = CRR_SLOTTING_WEIGHTS
        elif is_hvcre:
            weights = BASEL31_SLOTTING_WEIGHTS_HVCRE
        else:
            weights = BASEL31_SLOTTING_WEIGHTS

        return (
            pl.when(self._expr.str.to_lowercase() == "strong")
            .then(pl.lit(weights["strong"]))
            .when(self._expr.str.to_lowercase() == "good")
            .then(pl.lit(weights["good"]))
            .when(self._expr.str.to_lowercase() == "satisfactory")
            .then(pl.lit(weights["satisfactory"]))
            .when(self._expr.str.to_lowercase() == "weak")
            .then(pl.lit(weights["weak"]))
            .when(self._expr.str.to_lowercase() == "default")
            .then(pl.lit(weights["default"]))
            .otherwise(pl.lit(weights["satisfactory"]))
        )
