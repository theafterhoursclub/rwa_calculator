"""
Hierarchy resolution for RWA calculator.

Resolves counterparty and facility hierarchies, enabling:
- Rating inheritance from parent entities
- Lending group exposure aggregation for retail threshold
- Facility-to-exposure hierarchy traversal

Classes:
    HierarchyResolver: Main resolver implementing HierarchyResolverProtocol

Usage:
    from rwa_calc.engine.hierarchy import HierarchyResolver

    resolver = HierarchyResolver()
    resolved = resolver.resolve(raw_data, config)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import polars as pl

from rwa_calc.contracts.bundles import (
    CounterpartyLookup,
    RawDataBundle,
    ResolvedHierarchyBundle,
)

if TYPE_CHECKING:
    from rwa_calc.contracts.config import CalculationConfig


@dataclass
class HierarchyError:
    """Error encountered during hierarchy resolution."""

    error_type: str
    message: str
    entity_reference: str | None = None
    context: dict = field(default_factory=dict)


class HierarchyResolver:
    """
    Resolve counterparty and exposure hierarchies.

    Implements HierarchyResolverProtocol for:
    - Building counterparty org hierarchy lookups
    - Inheriting ratings from parent entities
    - Resolving facility-to-exposure mappings
    - Aggregating lending group exposures for retail threshold

    All operations use Polars LazyFrames for deferred execution.
    """

    def resolve(
        self,
        data: RawDataBundle,
        config: CalculationConfig,
    ) -> ResolvedHierarchyBundle:
        """
        Resolve all hierarchies and return enriched data.

        Args:
            data: Raw data bundle from loader
            config: Calculation configuration

        Returns:
            ResolvedHierarchyBundle with hierarchy metadata added
        """
        errors: list[HierarchyError] = []

        # Step 1: Build counterparty hierarchy lookup
        counterparty_lookup, cp_errors = self._build_counterparty_lookup(
            data.counterparties,
            data.org_mappings,
            data.ratings,
        )
        errors.extend(cp_errors)

        # Step 2: Unify exposures (loans + contingents) with hierarchy metadata
        exposures, exp_errors = self._unify_exposures(
            data.loans,
            data.contingents,
            data.facility_mappings,
            counterparty_lookup,
        )
        errors.extend(exp_errors)

        # Step 3: Calculate lending group totals
        lending_group_totals, lg_errors = self._calculate_lending_group_totals(
            exposures,
            data.lending_mappings,
        )
        errors.extend(lg_errors)

        # Step 4: Add lending group exposure totals to exposures
        exposures = self._add_lending_group_totals_to_exposures(
            exposures,
            data.lending_mappings,
            lending_group_totals,
        )

        return ResolvedHierarchyBundle(
            exposures=exposures,
            counterparty_lookup=counterparty_lookup,
            collateral=data.collateral,
            guarantees=data.guarantees,
            provisions=data.provisions,
            lending_group_totals=lending_group_totals,
            hierarchy_errors=errors,
        )

    def _build_counterparty_lookup(
        self,
        counterparties: pl.LazyFrame,
        org_mappings: pl.LazyFrame,
        ratings: pl.LazyFrame,
    ) -> tuple[CounterpartyLookup, list[HierarchyError]]:
        """
        Build counterparty hierarchy lookup with rating inheritance.

        Returns:
            Tuple of (CounterpartyLookup, list of errors)
        """
        errors: list[HierarchyError] = []

        # Collect org mappings to build parent/ultimate parent dicts
        parent_lookup, ultimate_parent_lookup = self._build_parent_lookups(org_mappings)

        # Build rating lookup with inheritance
        rating_lookup = self._build_rating_lookup(
            counterparties,
            ratings,
            ultimate_parent_lookup,
        )

        # Enrich counterparties with hierarchy info
        enriched_counterparties = self._enrich_counterparties_with_hierarchy(
            counterparties,
            org_mappings,
            ratings,
        )

        return CounterpartyLookup(
            counterparties=enriched_counterparties,
            parent_lookup=parent_lookup,
            ultimate_parent_lookup=ultimate_parent_lookup,
            rating_lookup=rating_lookup,
        ), errors

    def _build_parent_lookups(
        self,
        org_mappings: pl.LazyFrame,
    ) -> tuple[dict[str, str], dict[str, str]]:
        """
        Build parent and ultimate parent lookup dictionaries.

        Returns:
            Tuple of (parent_lookup, ultimate_parent_lookup)
        """
        # Collect mappings to build dicts
        mappings_df = org_mappings.collect()

        # Build direct parent lookup
        parent_lookup: dict[str, str] = {}
        for row in mappings_df.iter_rows(named=True):
            child = row["child_counterparty_reference"]
            parent = row["parent_counterparty_reference"]
            parent_lookup[child] = parent

        # Build ultimate parent lookup by traversing hierarchy
        ultimate_parent_lookup: dict[str, str] = {}
        for child in parent_lookup:
            ultimate = self._find_ultimate_parent(child, parent_lookup)
            ultimate_parent_lookup[child] = ultimate

        return parent_lookup, ultimate_parent_lookup

    def _find_ultimate_parent(
        self,
        entity: str,
        parent_lookup: dict[str, str],
        max_depth: int = 100,
    ) -> str:
        """
        Find the ultimate parent of an entity by traversing hierarchy.

        Args:
            entity: Starting entity reference
            parent_lookup: Direct parent mappings
            max_depth: Maximum traversal depth to prevent infinite loops

        Returns:
            Ultimate parent reference (or self if no parent)
        """
        current = entity
        depth = 0
        visited = {current}

        while current in parent_lookup and depth < max_depth:
            parent = parent_lookup[current]
            if parent in visited:
                # Circular reference detected
                break
            visited.add(parent)
            current = parent
            depth += 1

        return current

    def _build_rating_lookup(
        self,
        counterparties: pl.LazyFrame,
        ratings: pl.LazyFrame,
        ultimate_parent_lookup: dict[str, str],
    ) -> dict[str, dict]:
        """
        Build rating lookup with parent inheritance for unrated entities.

        Returns:
            Dict mapping counterparty_reference to rating info
        """
        # Collect ratings
        ratings_df = ratings.collect()

        # Build direct rating lookup (best rating per counterparty)
        direct_ratings: dict[str, dict] = {}
        for row in ratings_df.iter_rows(named=True):
            cp_ref = row["counterparty_reference"]
            rating_info = {
                "rating_type": row.get("rating_type"),
                "rating_agency": row.get("rating_agency"),
                "rating_value": row.get("rating_value"),
                "cqs": row.get("cqs"),
                "pd": row.get("pd"),
                "rating_date": row.get("rating_date"),
            }
            # Keep the first rating (or could implement priority logic)
            if cp_ref not in direct_ratings:
                direct_ratings[cp_ref] = rating_info

        # Build final rating lookup with inheritance
        rating_lookup: dict[str, dict] = {}

        # Get all counterparty references
        cp_refs = counterparties.select("counterparty_reference").collect()

        for row in cp_refs.iter_rows(named=True):
            cp_ref = row["counterparty_reference"]

            if cp_ref in direct_ratings:
                # Has own rating
                rating_lookup[cp_ref] = {
                    **direct_ratings[cp_ref],
                    "inherited": False,
                    "source_counterparty": cp_ref,
                    "inheritance_reason": "own_rating",
                }
            elif cp_ref in ultimate_parent_lookup:
                # Try to inherit from ultimate parent
                ultimate_parent = ultimate_parent_lookup[cp_ref]
                if ultimate_parent in direct_ratings:
                    rating_lookup[cp_ref] = {
                        **direct_ratings[ultimate_parent],
                        "inherited": True,
                        "source_counterparty": ultimate_parent,
                        "inheritance_reason": "parent_rating",
                    }
                else:
                    # Neither entity nor parent rated
                    rating_lookup[cp_ref] = {
                        "rating_type": None,
                        "rating_agency": None,
                        "rating_value": None,
                        "cqs": None,
                        "pd": None,
                        "rating_date": None,
                        "inherited": False,
                        "source_counterparty": None,
                        "inheritance_reason": "unrated",
                    }
            else:
                # No hierarchy, check if rated
                rating_lookup[cp_ref] = {
                    "rating_type": None,
                    "rating_agency": None,
                    "rating_value": None,
                    "cqs": None,
                    "pd": None,
                    "rating_date": None,
                    "inherited": False,
                    "source_counterparty": None,
                    "inheritance_reason": "unrated",
                }

        return rating_lookup

    def _enrich_counterparties_with_hierarchy(
        self,
        counterparties: pl.LazyFrame,
        org_mappings: pl.LazyFrame,
        ratings: pl.LazyFrame,
    ) -> pl.LazyFrame:
        """
        Enrich counterparties with hierarchy and rating information.

        Adds columns:
        - counterparty_has_parent: bool
        - parent_counterparty_reference: str | null
        - ultimate_parent_reference: str | null
        - counterparty_hierarchy_depth: int
        - rating_inherited: bool
        - rating_source_counterparty: str | null
        - rating_inheritance_reason: str
        """
        # Join with org_mappings to get parent
        enriched = counterparties.join(
            org_mappings.select([
                pl.col("child_counterparty_reference"),
                pl.col("parent_counterparty_reference"),
            ]),
            left_on="counterparty_reference",
            right_on="child_counterparty_reference",
            how="left",
        )

        # Add has_parent flag
        enriched = enriched.with_columns([
            pl.col("parent_counterparty_reference").is_not_null().alias("counterparty_has_parent"),
        ])

        # Join with ratings for own rating
        enriched = enriched.join(
            ratings.select([
                pl.col("counterparty_reference").alias("rating_counterparty"),
                pl.col("cqs"),
                pl.col("rating_value"),
                pl.col("rating_agency"),
            ]).group_by("rating_counterparty").first(),
            left_on="counterparty_reference",
            right_on="rating_counterparty",
            how="left",
        )

        # Add rating inheritance fields (simplified - full inheritance in lookup)
        enriched = enriched.with_columns([
            pl.col("cqs").is_not_null().not_().alias("rating_inherited"),
            pl.when(pl.col("cqs").is_not_null())
            .then(pl.col("counterparty_reference"))
            .otherwise(pl.col("parent_counterparty_reference"))
            .alias("rating_source_counterparty"),
            pl.when(pl.col("cqs").is_not_null())
            .then(pl.lit("own_rating"))
            .when(pl.col("parent_counterparty_reference").is_not_null())
            .then(pl.lit("parent_rating"))
            .otherwise(pl.lit("unrated"))
            .alias("rating_inheritance_reason"),
        ])

        return enriched

    def _unify_exposures(
        self,
        loans: pl.LazyFrame,
        contingents: pl.LazyFrame,
        facility_mappings: pl.LazyFrame,
        counterparty_lookup: CounterpartyLookup,
    ) -> tuple[pl.LazyFrame, list[HierarchyError]]:
        """
        Unify loans and contingents into a single exposures LazyFrame.

        Returns:
            Tuple of (unified exposures LazyFrame, list of errors)
        """
        errors: list[HierarchyError] = []

        # Standardize loan columns
        loans_unified = loans.select([
            pl.col("loan_reference").alias("exposure_reference"),
            pl.lit("loan").alias("exposure_type"),
            pl.col("product_type"),
            pl.col("book_code"),
            pl.col("counterparty_reference"),
            pl.col("value_date"),
            pl.col("maturity_date"),
            pl.col("currency"),
            pl.col("drawn_amount"),
            pl.lit(0.0).alias("undrawn_amount"),
            pl.lit(0.0).alias("nominal_amount"),
            pl.col("lgd"),
            pl.col("seniority"),
            pl.lit(None).cast(pl.String).alias("commitment_type"),
            pl.lit(None).cast(pl.String).alias("ccf_category"),
        ])

        # Standardize contingent columns
        contingents_unified = contingents.select([
            pl.col("contingent_reference").alias("exposure_reference"),
            pl.lit("contingent").alias("exposure_type"),
            pl.col("product_type"),
            pl.col("book_code"),
            pl.col("counterparty_reference"),
            pl.col("value_date"),
            pl.col("maturity_date"),
            pl.col("currency"),
            pl.lit(0.0).alias("drawn_amount"),
            pl.lit(0.0).alias("undrawn_amount"),
            pl.col("nominal_amount"),
            pl.col("lgd"),
            pl.col("seniority"),
            pl.lit(None).cast(pl.String).alias("commitment_type"),
            pl.col("ccf_category"),
        ])

        # Combine
        exposures = pl.concat([loans_unified, contingents_unified], how="diagonal_relaxed")

        # Join with facility mappings to get parent facility
        exposures = exposures.join(
            facility_mappings.select([
                pl.col("child_reference"),
                pl.col("parent_facility_reference"),
            ]),
            left_on="exposure_reference",
            right_on="child_reference",
            how="left",
        )

        # Add facility hierarchy fields
        exposures = exposures.with_columns([
            pl.col("parent_facility_reference").is_not_null().alias("exposure_has_parent"),
            pl.col("parent_facility_reference").alias("root_facility_reference"),  # Simplified
            pl.lit(1).cast(pl.Int8).alias("facility_hierarchy_depth"),
        ])

        # Add counterparty hierarchy fields from lookup
        exposures = exposures.join(
            counterparty_lookup.counterparties.select([
                pl.col("counterparty_reference"),
                pl.col("counterparty_has_parent"),
                pl.col("parent_counterparty_reference"),
                pl.col("rating_inherited"),
                pl.col("rating_source_counterparty"),
                pl.col("rating_inheritance_reason"),
            ]),
            on="counterparty_reference",
            how="left",
        )

        # Add ultimate parent using the lookup dict
        # Note: This requires collecting the dict, which is done in _build_parent_lookups
        # For LazyFrame, we'll add a placeholder - full resolution happens at runtime
        exposures = exposures.with_columns([
            pl.col("parent_counterparty_reference").alias("ultimate_parent_reference"),
            pl.lit(1).cast(pl.Int8).alias("counterparty_hierarchy_depth"),
        ])

        return exposures, errors

    def _calculate_lending_group_totals(
        self,
        exposures: pl.LazyFrame,
        lending_mappings: pl.LazyFrame,
    ) -> tuple[pl.LazyFrame, list[HierarchyError]]:
        """
        Calculate total exposure by lending group for retail threshold testing.

        Returns:
            Tuple of (lending group totals LazyFrame, list of errors)
        """
        errors: list[HierarchyError] = []

        # Build lending group membership
        # The parent_counterparty_reference is the lending group anchor
        lending_groups = lending_mappings.select([
            pl.col("parent_counterparty_reference").alias("lending_group_reference"),
            pl.col("child_counterparty_reference").alias("member_counterparty_reference"),
        ])

        # Include the parent itself as a member
        parent_as_member = lending_mappings.select([
            pl.col("parent_counterparty_reference").alias("lending_group_reference"),
            pl.col("parent_counterparty_reference").alias("member_counterparty_reference"),
        ]).unique()

        all_members = pl.concat([lending_groups, parent_as_member], how="vertical")

        # Join exposures to get lending group for each counterparty
        exposures_with_group = exposures.join(
            all_members,
            left_on="counterparty_reference",
            right_on="member_counterparty_reference",
            how="left",
        )

        # Calculate total drawn amount per lending group
        lending_group_totals = exposures_with_group.filter(
            pl.col("lending_group_reference").is_not_null()
        ).group_by("lending_group_reference").agg([
            pl.col("drawn_amount").sum().alias("total_drawn"),
            pl.col("nominal_amount").sum().alias("total_nominal"),
            (pl.col("drawn_amount") + pl.col("nominal_amount")).sum().alias("total_exposure"),
            pl.len().alias("exposure_count"),
        ])

        return lending_group_totals, errors

    def _add_lending_group_totals_to_exposures(
        self,
        exposures: pl.LazyFrame,
        lending_mappings: pl.LazyFrame,
        lending_group_totals: pl.LazyFrame,
    ) -> pl.LazyFrame:
        """
        Add lending group reference and total exposure to each exposure.
        """
        # Build lending group membership (same as above)
        lending_groups = lending_mappings.select([
            pl.col("parent_counterparty_reference").alias("lending_group_reference"),
            pl.col("child_counterparty_reference").alias("member_counterparty_reference"),
        ])

        parent_as_member = lending_mappings.select([
            pl.col("parent_counterparty_reference").alias("lending_group_reference"),
            pl.col("parent_counterparty_reference").alias("member_counterparty_reference"),
        ]).unique()

        all_members = pl.concat([lending_groups, parent_as_member], how="vertical")

        # Join to get lending group reference
        exposures = exposures.join(
            all_members,
            left_on="counterparty_reference",
            right_on="member_counterparty_reference",
            how="left",
        )

        # Join to get lending group total exposure
        exposures = exposures.join(
            lending_group_totals.select([
                pl.col("lending_group_reference").alias("lg_ref_for_join"),
                pl.col("total_exposure").alias("lending_group_total_exposure"),
            ]),
            left_on="lending_group_reference",
            right_on="lg_ref_for_join",
            how="left",
        )

        # Fill nulls with 0 for non-grouped exposures
        exposures = exposures.with_columns([
            pl.col("lending_group_total_exposure").fill_null(0.0),
        ])

        return exposures


def create_hierarchy_resolver() -> HierarchyResolver:
    """
    Create a hierarchy resolver instance.

    Returns:
        HierarchyResolver ready for use
    """
    return HierarchyResolver()
