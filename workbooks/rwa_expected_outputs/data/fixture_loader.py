"""
Fixture loader for test data.

Loads parquet fixture files from tests/fixtures and provides them
as a structured FixtureData object for use in RWA calculations.
"""

from dataclasses import dataclass
from pathlib import Path

import polars as pl


@dataclass
class FixtureData:
    """Container for all loaded fixture data."""

    # Counterparties
    sovereigns: pl.LazyFrame
    institutions: pl.LazyFrame
    corporates: pl.LazyFrame
    retail: pl.LazyFrame
    specialised_lending: pl.LazyFrame

    # Exposures
    facilities: pl.LazyFrame
    loans: pl.LazyFrame
    contingents: pl.LazyFrame
    facility_mappings: pl.LazyFrame

    # Credit risk mitigation
    collateral: pl.LazyFrame
    guarantees: pl.LazyFrame
    provisions: pl.LazyFrame

    # Ratings
    ratings: pl.LazyFrame

    # Mappings
    org_mappings: pl.LazyFrame
    lending_mappings: pl.LazyFrame

    def get_all_counterparties(self) -> pl.LazyFrame:
        """Combine all counterparty types into a single LazyFrame."""
        return pl.concat([
            self.sovereigns,
            self.institutions,
            self.corporates,
            self.retail,
        ])

    def get_counterparty(self, reference: str) -> dict | None:
        """Get a single counterparty by reference."""
        all_cpty = self.get_all_counterparties()
        result = all_cpty.filter(
            pl.col("counterparty_reference") == reference
        ).collect()
        if result.height == 0:
            return None
        return result.row(0, named=True)

    def get_loan(self, reference: str) -> dict | None:
        """Get a single loan by reference."""
        result = self.loans.filter(
            pl.col("loan_reference") == reference
        ).collect()
        if result.height == 0:
            return None
        return result.row(0, named=True)

    def get_collateral_for_beneficiary(
        self, beneficiary_reference: str
    ) -> pl.LazyFrame:
        """Get all collateral linked to a specific beneficiary."""
        return self.collateral.filter(
            pl.col("beneficiary_reference") == beneficiary_reference
        )

    def get_guarantee_for_beneficiary(
        self, beneficiary_reference: str
    ) -> pl.LazyFrame:
        """Get all guarantees linked to a specific beneficiary."""
        return self.guarantees.filter(
            pl.col("beneficiary_reference") == beneficiary_reference
        )

    def get_provision_for_beneficiary(
        self, beneficiary_reference: str
    ) -> pl.LazyFrame:
        """Get all provisions linked to a specific beneficiary."""
        return self.provisions.filter(
            pl.col("beneficiary_reference") == beneficiary_reference
        )

    def get_rating(self, counterparty_reference: str) -> dict | None:
        """Get the most recent external rating for a counterparty."""
        result = (
            self.ratings
            .filter(pl.col("counterparty_reference") == counterparty_reference)
            .filter(pl.col("rating_type") == "external")
            .sort("rating_date", descending=True)
            .collect()
        )
        if result.height == 0:
            return None
        return result.row(0, named=True)


def get_fixture_path() -> Path:
    """Get the path to the fixtures directory."""
    # Navigate from workbooks/rwa_expected_outputs/data to tests/fixtures
    current_dir = Path(__file__).parent
    project_root = current_dir.parent.parent.parent
    return project_root / "tests" / "fixtures"


def load_fixtures() -> FixtureData:
    """
    Load all fixture parquet files into a FixtureData container.

    Returns:
        FixtureData: Container with all loaded fixtures as LazyFrames
    """
    fixture_path = get_fixture_path()

    return FixtureData(
        # Counterparties
        sovereigns=pl.scan_parquet(fixture_path / "counterparty" / "sovereign.parquet"),
        institutions=pl.scan_parquet(fixture_path / "counterparty" / "institution.parquet"),
        corporates=pl.scan_parquet(fixture_path / "counterparty" / "corporate.parquet"),
        retail=pl.scan_parquet(fixture_path / "counterparty" / "retail.parquet"),
        specialised_lending=pl.scan_parquet(fixture_path / "counterparty" / "specialised_lending.parquet"),

        # Exposures
        facilities=pl.scan_parquet(fixture_path / "exposures" / "facilities.parquet"),
        loans=pl.scan_parquet(fixture_path / "exposures" / "loans.parquet"),
        contingents=pl.scan_parquet(fixture_path / "exposures" / "contingents.parquet"),
        facility_mappings=pl.scan_parquet(fixture_path / "exposures" / "facility_mapping.parquet"),

        # Credit risk mitigation
        collateral=pl.scan_parquet(fixture_path / "collateral" / "collateral.parquet"),
        guarantees=pl.scan_parquet(fixture_path / "guarantee" / "guarantee.parquet"),
        provisions=pl.scan_parquet(fixture_path / "provision" / "provision.parquet"),

        # Ratings
        ratings=pl.scan_parquet(fixture_path / "ratings" / "ratings.parquet"),

        # Mappings
        org_mappings=pl.scan_parquet(fixture_path / "mapping" / "org_mapping.parquet"),
        lending_mappings=pl.scan_parquet(fixture_path / "mapping" / "lending_mapping.parquet"),
    )


def load_fixtures_eager() -> dict[str, pl.DataFrame]:
    """
    Load all fixtures as eager DataFrames (for interactive use in Marimo).

    Returns:
        dict: Dictionary mapping fixture names to DataFrames
    """
    fixture_path = get_fixture_path()

    return {
        # Counterparties
        "sovereigns": pl.read_parquet(fixture_path / "counterparty" / "sovereign.parquet"),
        "institutions": pl.read_parquet(fixture_path / "counterparty" / "institution.parquet"),
        "corporates": pl.read_parquet(fixture_path / "counterparty" / "corporate.parquet"),
        "retail": pl.read_parquet(fixture_path / "counterparty" / "retail.parquet"),
        "specialised_lending": pl.read_parquet(fixture_path / "counterparty" / "specialised_lending.parquet"),

        # Exposures
        "facilities": pl.read_parquet(fixture_path / "exposures" / "facilities.parquet"),
        "loans": pl.read_parquet(fixture_path / "exposures" / "loans.parquet"),
        "contingents": pl.read_parquet(fixture_path / "exposures" / "contingents.parquet"),
        "facility_mappings": pl.read_parquet(fixture_path / "exposures" / "facility_mapping.parquet"),

        # Credit risk mitigation
        "collateral": pl.read_parquet(fixture_path / "collateral" / "collateral.parquet"),
        "guarantees": pl.read_parquet(fixture_path / "guarantee" / "guarantee.parquet"),
        "provisions": pl.read_parquet(fixture_path / "provision" / "provision.parquet"),

        # Ratings
        "ratings": pl.read_parquet(fixture_path / "ratings" / "ratings.parquet"),

        # Mappings
        "org_mappings": pl.read_parquet(fixture_path / "mapping" / "org_mapping.parquet"),
        "lending_mappings": pl.read_parquet(fixture_path / "mapping" / "lending_mapping.parquet"),
    }
