"""
Unit tests for FX converter module.

Tests currency conversion functionality for exposures, collateral,
guarantees, and provisions.
"""

from datetime import date
from decimal import Decimal

import polars as pl
import pytest

from rwa_calc.contracts.config import CalculationConfig, IRBPermissions
from rwa_calc.domain.enums import RegulatoryFramework
from rwa_calc.engine.fx_converter import FXConverter, create_fx_converter


# =============================================================================
# FIXTURES
# =============================================================================


@pytest.fixture
def fx_converter() -> FXConverter:
    """Create FX converter instance."""
    return FXConverter()


@pytest.fixture
def fx_rates() -> pl.LazyFrame:
    """Create sample FX rates for testing."""
    return pl.LazyFrame({
        "currency_from": ["GBP", "USD", "EUR", "JPY", "CHF"],
        "currency_to": ["GBP", "GBP", "GBP", "GBP", "GBP"],
        "rate": [1.0, 0.79, 0.88, 0.0053, 0.89],
    })


@pytest.fixture
def config() -> CalculationConfig:
    """Create calculation config with FX conversion enabled."""
    return CalculationConfig(
        framework=RegulatoryFramework.CRR,
        reporting_date=date(2026, 1, 1),
        base_currency="GBP",
        apply_fx_conversion=True,
    )


@pytest.fixture
def config_fx_disabled() -> CalculationConfig:
    """Create calculation config with FX conversion disabled."""
    return CalculationConfig(
        framework=RegulatoryFramework.CRR,
        reporting_date=date(2026, 1, 1),
        base_currency="GBP",
        apply_fx_conversion=False,
    )


@pytest.fixture
def usd_exposures() -> pl.LazyFrame:
    """Create sample USD exposures for testing."""
    return pl.LazyFrame({
        "exposure_reference": ["EXP_USD_001", "EXP_USD_002"],
        "exposure_type": ["loan", "loan"],
        "currency": ["USD", "USD"],
        "drawn_amount": [1000000.0, 500000.0],
        "undrawn_amount": [200000.0, 100000.0],
        "nominal_amount": [0.0, 0.0],
    })


@pytest.fixture
def gbp_exposures() -> pl.LazyFrame:
    """Create sample GBP exposures for testing."""
    return pl.LazyFrame({
        "exposure_reference": ["EXP_GBP_001"],
        "exposure_type": ["loan"],
        "currency": ["GBP"],
        "drawn_amount": [1000000.0],
        "undrawn_amount": [200000.0],
        "nominal_amount": [0.0],
    })


@pytest.fixture
def multi_currency_exposures() -> pl.LazyFrame:
    """Create exposures in multiple currencies."""
    return pl.LazyFrame({
        "exposure_reference": ["EXP_GBP", "EXP_USD", "EXP_EUR", "EXP_UNK"],
        "exposure_type": ["loan", "loan", "loan", "loan"],
        "currency": ["GBP", "USD", "EUR", "ZAR"],  # ZAR has no rate
        "drawn_amount": [1000000.0, 1000000.0, 1000000.0, 1000000.0],
        "undrawn_amount": [0.0, 0.0, 0.0, 0.0],
        "nominal_amount": [0.0, 0.0, 0.0, 0.0],
    })


@pytest.fixture
def collateral() -> pl.LazyFrame:
    """Create sample collateral for testing."""
    return pl.LazyFrame({
        "collateral_reference": ["COLL_001", "COLL_002"],
        "currency": ["USD", "EUR"],
        "market_value": [500000.0, 300000.0],
        "nominal_value": [500000.0, 300000.0],
    })


@pytest.fixture
def guarantees() -> pl.LazyFrame:
    """Create sample guarantees for testing."""
    return pl.LazyFrame({
        "guarantee_reference": ["GUAR_001", "GUAR_002"],
        "currency": ["USD", "GBP"],
        "amount_covered": [250000.0, 150000.0],
    })


@pytest.fixture
def provisions() -> pl.LazyFrame:
    """Create sample provisions for testing."""
    return pl.LazyFrame({
        "provision_reference": ["PROV_001", "PROV_002"],
        "currency": ["EUR", "GBP"],
        "amount": [50000.0, 30000.0],
    })


# =============================================================================
# EXPOSURE CONVERSION TESTS
# =============================================================================


class TestConvertExposures:
    """Tests for exposure conversion."""

    def test_convert_single_currency_usd_to_gbp(
        self,
        fx_converter: FXConverter,
        usd_exposures: pl.LazyFrame,
        fx_rates: pl.LazyFrame,
        config: CalculationConfig,
    ) -> None:
        """Test that USD exposures are correctly converted to GBP."""
        result = fx_converter.convert_exposures(usd_exposures, fx_rates, config)
        df = result.collect()

        # USD rate to GBP is 0.79
        assert df["drawn_amount"][0] == pytest.approx(1000000.0 * 0.79)
        assert df["drawn_amount"][1] == pytest.approx(500000.0 * 0.79)
        assert df["undrawn_amount"][0] == pytest.approx(200000.0 * 0.79)
        assert df["currency"][0] == "GBP"
        assert df["currency"][1] == "GBP"

    def test_gbp_no_conversion(
        self,
        fx_converter: FXConverter,
        gbp_exposures: pl.LazyFrame,
        fx_rates: pl.LazyFrame,
        config: CalculationConfig,
    ) -> None:
        """Test that GBP exposures remain unchanged."""
        result = fx_converter.convert_exposures(gbp_exposures, fx_rates, config)
        df = result.collect()

        assert df["drawn_amount"][0] == 1000000.0
        assert df["undrawn_amount"][0] == 200000.0
        assert df["currency"][0] == "GBP"

    def test_original_amounts_preserved(
        self,
        fx_converter: FXConverter,
        usd_exposures: pl.LazyFrame,
        fx_rates: pl.LazyFrame,
        config: CalculationConfig,
    ) -> None:
        """Test that original amounts are preserved in audit columns."""
        result = fx_converter.convert_exposures(usd_exposures, fx_rates, config)
        df = result.collect()

        # Original currency should be preserved
        assert df["original_currency"][0] == "USD"
        # Original amount should be drawn + nominal
        assert df["original_amount"][0] == 1000000.0
        # FX rate applied should be recorded
        assert df["fx_rate_applied"][0] == pytest.approx(0.79)

    def test_original_amounts_null_for_gbp(
        self,
        fx_converter: FXConverter,
        gbp_exposures: pl.LazyFrame,
        fx_rates: pl.LazyFrame,
        config: CalculationConfig,
    ) -> None:
        """Test that fx_rate_applied is null for GBP exposures."""
        result = fx_converter.convert_exposures(gbp_exposures, fx_rates, config)
        df = result.collect()

        assert df["original_currency"][0] == "GBP"
        assert df["fx_rate_applied"][0] is None

    def test_missing_rate_keeps_original(
        self,
        fx_converter: FXConverter,
        multi_currency_exposures: pl.LazyFrame,
        fx_rates: pl.LazyFrame,
        config: CalculationConfig,
    ) -> None:
        """Test that exposures with missing rates keep original values."""
        result = fx_converter.convert_exposures(multi_currency_exposures, fx_rates, config)
        df = result.collect()

        # ZAR has no rate, so should keep original values
        zar_row = df.filter(pl.col("exposure_reference") == "EXP_UNK")
        assert zar_row["drawn_amount"][0] == 1000000.0
        assert zar_row["currency"][0] == "ZAR"

    def test_fx_disabled_skips_conversion(
        self,
        fx_converter: FXConverter,
        usd_exposures: pl.LazyFrame,
        fx_rates: pl.LazyFrame,
        config_fx_disabled: CalculationConfig,
    ) -> None:
        """Test that FX conversion is skipped when disabled."""
        result = fx_converter.convert_exposures(usd_exposures, fx_rates, config_fx_disabled)
        df = result.collect()

        # Should keep original USD values
        assert df["drawn_amount"][0] == 1000000.0
        assert df["currency"][0] == "USD"
        assert df["fx_rate_applied"][0] is None

    def test_none_fx_rates_skips_conversion(
        self,
        fx_converter: FXConverter,
        usd_exposures: pl.LazyFrame,
        config: CalculationConfig,
    ) -> None:
        """Test that conversion is skipped when fx_rates is None."""
        result = fx_converter.convert_exposures(usd_exposures, None, config)
        df = result.collect()

        assert df["drawn_amount"][0] == 1000000.0
        assert df["currency"][0] == "USD"


# =============================================================================
# COLLATERAL CONVERSION TESTS
# =============================================================================


class TestConvertCollateral:
    """Tests for collateral conversion."""

    def test_convert_collateral_values(
        self,
        fx_converter: FXConverter,
        collateral: pl.LazyFrame,
        fx_rates: pl.LazyFrame,
        config: CalculationConfig,
    ) -> None:
        """Test that collateral values are correctly converted."""
        result = fx_converter.convert_collateral(collateral, fx_rates, config)
        df = result.collect()

        # USD collateral: 500000 * 0.79 = 395000
        usd_row = df.filter(pl.col("collateral_reference") == "COLL_001")
        assert usd_row["market_value"][0] == pytest.approx(500000.0 * 0.79)
        assert usd_row["currency"][0] == "GBP"

        # EUR collateral: 300000 * 0.88 = 264000
        eur_row = df.filter(pl.col("collateral_reference") == "COLL_002")
        assert eur_row["market_value"][0] == pytest.approx(300000.0 * 0.88)
        assert eur_row["currency"][0] == "GBP"

    def test_collateral_fx_disabled(
        self,
        fx_converter: FXConverter,
        collateral: pl.LazyFrame,
        fx_rates: pl.LazyFrame,
        config_fx_disabled: CalculationConfig,
    ) -> None:
        """Test that collateral conversion is skipped when disabled."""
        result = fx_converter.convert_collateral(collateral, fx_rates, config_fx_disabled)
        df = result.collect()

        assert df["market_value"][0] == 500000.0
        assert df["currency"][0] == "USD"


# =============================================================================
# GUARANTEE CONVERSION TESTS
# =============================================================================


class TestConvertGuarantees:
    """Tests for guarantee conversion."""

    def test_convert_guarantee_amounts(
        self,
        fx_converter: FXConverter,
        guarantees: pl.LazyFrame,
        fx_rates: pl.LazyFrame,
        config: CalculationConfig,
    ) -> None:
        """Test that guarantee amounts are correctly converted."""
        result = fx_converter.convert_guarantees(guarantees, fx_rates, config)
        df = result.collect()

        # USD guarantee: 250000 * 0.79 = 197500
        usd_row = df.filter(pl.col("guarantee_reference") == "GUAR_001")
        assert usd_row["amount_covered"][0] == pytest.approx(250000.0 * 0.79)
        assert usd_row["currency"][0] == "GBP"

        # GBP guarantee: unchanged
        gbp_row = df.filter(pl.col("guarantee_reference") == "GUAR_002")
        assert gbp_row["amount_covered"][0] == 150000.0
        assert gbp_row["currency"][0] == "GBP"


# =============================================================================
# PROVISION CONVERSION TESTS
# =============================================================================


class TestConvertProvisions:
    """Tests for provision conversion."""

    def test_convert_provision_amounts(
        self,
        fx_converter: FXConverter,
        provisions: pl.LazyFrame,
        fx_rates: pl.LazyFrame,
        config: CalculationConfig,
    ) -> None:
        """Test that provision amounts are correctly converted."""
        result = fx_converter.convert_provisions(provisions, fx_rates, config)
        df = result.collect()

        # EUR provision: 50000 * 0.88 = 44000
        eur_row = df.filter(pl.col("provision_reference") == "PROV_001")
        assert eur_row["amount"][0] == pytest.approx(50000.0 * 0.88)
        assert eur_row["currency"][0] == "GBP"

        # GBP provision: unchanged
        gbp_row = df.filter(pl.col("provision_reference") == "PROV_002")
        assert gbp_row["amount"][0] == 30000.0
        assert gbp_row["currency"][0] == "GBP"


# =============================================================================
# FACTORY FUNCTION TESTS
# =============================================================================


class TestCreateFXConverter:
    """Tests for factory function."""

    def test_create_fx_converter(self) -> None:
        """Test that factory function creates valid converter."""
        converter = create_fx_converter()
        assert isinstance(converter, FXConverter)


# =============================================================================
# INTEGRATION TESTS
# =============================================================================


class TestFXConverterIntegration:
    """Integration tests with the full pipeline."""

    def test_multiple_currencies_in_batch(
        self,
        fx_converter: FXConverter,
        fx_rates: pl.LazyFrame,
        config: CalculationConfig,
    ) -> None:
        """Test conversion of multiple currencies in a single batch."""
        exposures = pl.LazyFrame({
            "exposure_reference": ["EXP_GBP", "EXP_USD", "EXP_EUR", "EXP_JPY", "EXP_CHF"],
            "exposure_type": ["loan"] * 5,
            "currency": ["GBP", "USD", "EUR", "JPY", "CHF"],
            "drawn_amount": [1000.0, 1000.0, 1000.0, 100000.0, 1000.0],  # JPY needs larger amount
            "undrawn_amount": [0.0] * 5,
            "nominal_amount": [0.0] * 5,
        })

        result = fx_converter.convert_exposures(exposures, fx_rates, config)
        df = result.collect()

        # All should be converted to GBP
        assert df["currency"].to_list() == ["GBP"] * 5

        # Check conversions
        gbp_row = df.filter(pl.col("exposure_reference") == "EXP_GBP")
        assert gbp_row["drawn_amount"][0] == 1000.0  # No change

        usd_row = df.filter(pl.col("exposure_reference") == "EXP_USD")
        assert usd_row["drawn_amount"][0] == pytest.approx(1000.0 * 0.79)

        eur_row = df.filter(pl.col("exposure_reference") == "EXP_EUR")
        assert eur_row["drawn_amount"][0] == pytest.approx(1000.0 * 0.88)

        jpy_row = df.filter(pl.col("exposure_reference") == "EXP_JPY")
        assert jpy_row["drawn_amount"][0] == pytest.approx(100000.0 * 0.0053)

        chf_row = df.filter(pl.col("exposure_reference") == "EXP_CHF")
        assert chf_row["drawn_amount"][0] == pytest.approx(1000.0 * 0.89)

    def test_alternative_base_currency_eur(
        self,
        fx_converter: FXConverter,
    ) -> None:
        """Test conversion to EUR instead of GBP."""
        fx_rates_to_eur = pl.LazyFrame({
            "currency_from": ["GBP", "EUR", "USD"],
            "currency_to": ["EUR", "EUR", "EUR"],
            "rate": [1.14, 1.0, 0.90],
        })

        config_eur = CalculationConfig(
            framework=RegulatoryFramework.CRR,
            reporting_date=date(2026, 1, 1),
            base_currency="EUR",
            apply_fx_conversion=True,
        )

        exposures = pl.LazyFrame({
            "exposure_reference": ["EXP_GBP", "EXP_USD"],
            "exposure_type": ["loan", "loan"],
            "currency": ["GBP", "USD"],
            "drawn_amount": [1000.0, 1000.0],
            "undrawn_amount": [0.0, 0.0],
            "nominal_amount": [0.0, 0.0],
        })

        result = fx_converter.convert_exposures(exposures, fx_rates_to_eur, config_eur)
        df = result.collect()

        # All should be converted to EUR
        assert df["currency"].to_list() == ["EUR", "EUR"]

        gbp_row = df.filter(pl.col("exposure_reference") == "EXP_GBP")
        assert gbp_row["drawn_amount"][0] == pytest.approx(1000.0 * 1.14)

        usd_row = df.filter(pl.col("exposure_reference") == "EXP_USD")
        assert usd_row["drawn_amount"][0] == pytest.approx(1000.0 * 0.90)
