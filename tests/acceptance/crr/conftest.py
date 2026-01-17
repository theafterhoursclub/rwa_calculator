"""
Shared fixtures for CRR acceptance tests.

Provides common test configuration and helper utilities.
"""

import pytest
from decimal import Decimal
from pathlib import Path
import sys

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


@pytest.fixture
def crr_config():
    """Standard CRR configuration for all tests."""
    return {
        "regulatory_framework": "CRR",
        "basel_version": "3.0",
        "reporting_date": "2025-12-31",
        "apply_sme_supporting_factor": True,
        "apply_infrastructure_factor": True,
        "sme_turnover_threshold_gbp": Decimal("44000000"),
        "sme_supporting_factor": Decimal("0.7619"),
        "infrastructure_factor": Decimal("0.75"),
        "pd_floor": Decimal("0.0003"),  # 0.03% single floor
        "maturity_floor": Decimal("1.0"),
        "maturity_cap": Decimal("5.0"),
    }


@pytest.fixture
def crr_risk_weights():
    """CRR SA risk weight lookup tables."""
    return {
        "sovereign": {
            1: Decimal("0.00"),
            2: Decimal("0.20"),
            3: Decimal("0.50"),
            4: Decimal("1.00"),
            5: Decimal("1.00"),
            6: Decimal("1.50"),
            None: Decimal("1.00"),
        },
        "institution_uk": {
            1: Decimal("0.20"),
            2: Decimal("0.30"),  # UK deviation
            3: Decimal("0.50"),
            4: Decimal("1.00"),
            5: Decimal("1.00"),
            6: Decimal("1.50"),
            None: Decimal("0.40"),
        },
        "corporate": {
            1: Decimal("0.20"),
            2: Decimal("0.50"),
            3: Decimal("1.00"),
            4: Decimal("1.00"),
            5: Decimal("1.50"),
            6: Decimal("1.50"),
            None: Decimal("1.00"),
        },
        "retail": Decimal("0.75"),
        "residential_mortgage": {
            "low_ltv": Decimal("0.35"),  # LTV <= 80%
            "high_ltv": Decimal("0.75"),  # Portion above 80%
            "threshold": Decimal("0.80"),
        },
        "commercial_re": {
            "low_ltv": Decimal("0.50"),  # LTV <= 50% with income cover
            "standard": Decimal("1.00"),
            "threshold": Decimal("0.50"),
        },
    }


@pytest.fixture
def crr_firb_lgd():
    """CRR F-IRB supervisory LGD values."""
    return {
        "unsecured_senior": Decimal("0.45"),
        "subordinated": Decimal("0.75"),
        "financial_collateral": Decimal("0.00"),
        "receivables": Decimal("0.35"),
        "residential_re": Decimal("0.35"),
        "commercial_re": Decimal("0.35"),
        "other_physical": Decimal("0.40"),
    }


@pytest.fixture
def crr_haircuts():
    """CRR supervisory haircuts."""
    return {
        "cash": Decimal("0.00"),
        "gold": Decimal("0.15"),
        "govt_bond_cqs1_0_1y": Decimal("0.005"),
        "govt_bond_cqs1_1_5y": Decimal("0.02"),
        "govt_bond_cqs1_5y_plus": Decimal("0.04"),
        "equity_main_index": Decimal("0.15"),
        "equity_other": Decimal("0.25"),
        "fx_mismatch": Decimal("0.08"),
    }


@pytest.fixture
def crr_ccf():
    """CRR credit conversion factors."""
    return {
        "full_risk": Decimal("1.00"),
        "medium_risk": Decimal("0.50"),
        "medium_low_risk": Decimal("0.20"),
        "low_risk": Decimal("0.00"),
    }


@pytest.fixture
def crr_slotting_rw():
    """CRR slotting risk weights."""
    return {
        "strong": Decimal("0.70"),
        "good": Decimal("0.70"),
        "satisfactory": Decimal("1.15"),
        "weak": Decimal("2.50"),
        "default": Decimal("0.00"),
    }


@pytest.fixture
def load_test_fixtures():
    """Load test fixtures from tests/fixtures directory."""
    from workbooks.shared.fixture_loader import load_fixtures
    return load_fixtures()


def assert_rwa_within_tolerance(actual: Decimal, expected: Decimal, tolerance: Decimal = Decimal("0.01")):
    """Assert RWA values are within acceptable tolerance (default 1%)."""
    if expected == 0:
        assert actual == 0, f"Expected 0, got {actual}"
    else:
        relative_diff = abs(actual - expected) / expected
        assert relative_diff <= tolerance, (
            f"RWA difference {relative_diff*100:.2f}% exceeds tolerance {tolerance*100:.0f}%: "
            f"actual={actual}, expected={expected}"
        )
