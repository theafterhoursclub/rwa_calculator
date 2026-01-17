# CRR Implementation Plan - Detailed Tasks

This document outlines the specific tasks required to update the current project structure to focus on CRR (Basel 3.0) implementation first.

## Overview

The existing project has:
- Test fixtures (complete)
- Basel 3.1-focused expected outputs workbook
- Comprehensive schema definitions

We need to:
1. Restructure workbooks for dual-framework support
2. Create CRR-specific expected outputs
3. Update schemas for framework-specific fields
4. Create CRR acceptance tests

---

## Phase 1: Restructure Existing Workbooks

### Task 1.1: Rename Existing Basel 3.1 Workbook

**Current Structure:**
```
workbooks/
└── rwa_expected_outputs/          # Currently Basel 3.1 focused
    ├── main.py
    ├── data/
    ├── calculations/
    └── scenarios/
```

**Target Structure:**
```
workbooks/
├── shared/                        # Shared components
│   ├── __init__.py
│   ├── fixture_loader.py          # Move from rwa_expected_outputs/data
│   └── irb_formulas.py            # Shared IRB calculations
├── crr_expected_outputs/          # NEW: CRR workbook
│   ├── main.py
│   ├── data/
│   │   └── crr_params.py
│   ├── calculations/
│   │   ├── crr_risk_weights.py
│   │   ├── crr_ccf.py
│   │   ├── crr_haircuts.py
│   │   └── crr_supporting_factors.py
│   └── scenarios/
│       ├── group_crr_a_sa.py
│       ├── group_crr_b_firb.py
│       ├── group_crr_c_airb.py
│       ├── group_crr_d_crm.py
│       ├── group_crr_e_slotting.py
│       ├── group_crr_g_provisions.py
│       └── group_crr_h_complex.py
└── basel31_expected_outputs/      # Renamed from rwa_expected_outputs
    ├── main.py
    ├── data/
    │   └── b31_params.py
    ├── calculations/
    │   ├── b31_risk_weights.py
    │   ├── b31_ltv_bands.py
    │   ├── b31_ccf.py
    │   ├── b31_haircuts.py
    │   ├── b31_pd_floors.py
    │   ├── b31_lgd_floors.py
    │   └── output_floor.py
    └── scenarios/
        └── (existing group files renamed to b31_ prefix)
```

**Actions:**
1. Create `workbooks/shared/` directory
2. Move `fixture_loader.py` to shared
3. Rename `rwa_expected_outputs/` to `basel31_expected_outputs/`
4. Create `crr_expected_outputs/` directory structure

### Task 1.2: Create Shared Fixture Loader

Move the existing fixture loader to a shared location and update imports.

**File:** `workbooks/shared/fixture_loader.py`

```python
"""Shared fixture loader for CRR and Basel 3.1 workbooks."""

from pathlib import Path
import polars as pl

# ... existing loader code with minor updates for shared usage
```

### Task 1.3: Create Shared IRB Formulas

The IRB formula (capital K calculation) is the same for both frameworks. Create a shared module.

**File:** `workbooks/shared/irb_formulas.py`

```python
"""Shared IRB calculation formulas (Basel II/III formula unchanged)."""

import math
from scipy.stats import norm


def calculate_correlation(pd: float, exposure_class: str, turnover_m: float | None = None) -> float:
    """Calculate asset correlation based on exposure class and PD."""
    ...


def calculate_capital_k(
    pd: float,
    lgd: float,
    maturity: float,
    correlation: float,
    is_retail: bool = False
) -> float:
    """Calculate capital requirement K using IRB formula."""
    ...


def calculate_rwa(ead: float, k: float) -> float:
    """Convert capital K to RWA: RWA = K * 12.5 * EAD."""
    return k * 12.5 * ead
```

---

## Phase 2: Create CRR Workbook

### Task 2.1: CRR Regulatory Parameters

**File:** `workbooks/crr_expected_outputs/data/crr_params.py`

```python
"""CRR regulatory parameters (EU 575/2013 as onshored into UK law)."""

from decimal import Decimal


# === SA Risk Weights ===

# Sovereigns (Art. 114)
CRR_SOVEREIGN_RW = {
    1: Decimal("0.00"),
    2: Decimal("0.20"),
    3: Decimal("0.50"),
    4: Decimal("1.00"),
    5: Decimal("1.00"),
    6: Decimal("1.50"),
    None: Decimal("1.00"),  # Unrated
}

# Institutions ECRA (Art. 120-121) with UK deviation
CRR_INSTITUTION_RW_UK = {
    1: Decimal("0.20"),
    2: Decimal("0.30"),   # UK deviation (standard Basel is 50%)
    3: Decimal("0.50"),
    4: Decimal("1.00"),
    5: Decimal("1.00"),
    6: Decimal("1.50"),
    None: Decimal("0.40"),  # Unrated - derived from sovereign CQS2
}

# Corporates (Art. 122)
CRR_CORPORATE_RW = {
    1: Decimal("0.20"),
    2: Decimal("0.50"),
    3: Decimal("1.00"),
    4: Decimal("1.00"),
    5: Decimal("1.50"),
    6: Decimal("1.50"),
    None: Decimal("1.00"),  # Unrated
}

# Retail (Art. 123)
CRR_RETAIL_RW = Decimal("0.75")

# Real Estate (Art. 125-126)
CRR_RESIDENTIAL_RW_LOW_LTV = Decimal("0.35")  # LTV <= 80%
CRR_RESIDENTIAL_RW_HIGH_LTV = Decimal("0.75")  # Portion above 80% LTV (or 50% if split)
CRR_COMMERCIAL_RW_LOW_LTV = Decimal("0.50")   # LTV <= 50% with income cover
CRR_COMMERCIAL_RW_STANDARD = Decimal("1.00")  # Otherwise

# === CCF (Art. 111) ===

CRR_CCF = {
    "full_risk": Decimal("1.00"),       # 100%
    "medium_risk": Decimal("0.50"),     # 50%
    "medium_low_risk": Decimal("0.20"), # 20%
    "low_risk": Decimal("0.00"),        # 0%
}

# === Supporting Factors (Art. 501) ===

CRR_SME_SUPPORTING_FACTOR = Decimal("0.7619")
CRR_INFRASTRUCTURE_SUPPORTING_FACTOR = Decimal("0.75")
CRR_SME_TURNOVER_THRESHOLD_EUR = Decimal("50000000")  # €50m
CRR_SME_TURNOVER_THRESHOLD_GBP = Decimal("44000000")  # £44m (UK approximation)

# === CRM Haircuts (Art. 224) ===

CRR_HAIRCUTS = {
    "cash": Decimal("0.00"),
    "gold": Decimal("0.15"),
    "govt_bond_cqs1_0_1y": Decimal("0.005"),
    "govt_bond_cqs1_1_5y": Decimal("0.02"),
    "govt_bond_cqs1_5y_plus": Decimal("0.04"),
    "govt_bond_cqs2_3_0_1y": Decimal("0.01"),
    "govt_bond_cqs2_3_1_5y": Decimal("0.03"),
    "govt_bond_cqs2_3_5y_plus": Decimal("0.06"),
    "equity_main_index": Decimal("0.15"),
    "equity_other": Decimal("0.25"),
    "currency_mismatch": Decimal("0.08"),
}

# === F-IRB Supervisory LGDs (Art. 161) ===

CRR_FIRB_LGD = {
    "senior_unsecured": Decimal("0.45"),
    "subordinated": Decimal("0.75"),
    "financial_collateral": Decimal("0.00"),
    "receivables": Decimal("0.35"),
    "residential_re": Decimal("0.35"),
    "commercial_re": Decimal("0.35"),
    "other_physical": Decimal("0.40"),
}

# === PD Floor (Art. 163) ===

CRR_PD_FLOOR = Decimal("0.0003")  # 0.03% for all classes

# === Slotting Risk Weights (Art. 153(5)) ===

CRR_SLOTTING_RW = {
    "strong": Decimal("0.70"),
    "good": Decimal("0.70"),       # Same as Strong under CRR
    "satisfactory": Decimal("1.15"),
    "weak": Decimal("2.50"),
    "default": Decimal("0.00"),    # EL treatment
}

# HVCRE uses same weights under CRR
CRR_SLOTTING_RW_HVCRE = CRR_SLOTTING_RW.copy()
```

### Task 2.2: CRR Risk Weight Calculations

**File:** `workbooks/crr_expected_outputs/calculations/crr_risk_weights.py`

```python
"""CRR SA risk weight lookup functions."""

from decimal import Decimal
from ..data.crr_params import (
    CRR_SOVEREIGN_RW,
    CRR_INSTITUTION_RW_UK,
    CRR_CORPORATE_RW,
    CRR_RETAIL_RW,
    CRR_RESIDENTIAL_RW_LOW_LTV,
    CRR_RESIDENTIAL_RW_HIGH_LTV,
    CRR_COMMERCIAL_RW_LOW_LTV,
    CRR_COMMERCIAL_RW_STANDARD,
    CRR_SME_SUPPORTING_FACTOR,
)


def get_sovereign_rw(cqs: int | None) -> Decimal:
    """Get risk weight for sovereign exposure (Art. 114)."""
    return CRR_SOVEREIGN_RW.get(cqs, CRR_SOVEREIGN_RW[None])


def get_institution_rw(cqs: int | None, country: str = "GB") -> Decimal:
    """Get risk weight for institution (Art. 120-121, with UK deviation)."""
    if country == "GB":
        return CRR_INSTITUTION_RW_UK.get(cqs, CRR_INSTITUTION_RW_UK[None])
    # For non-UK, use standard Basel treatment
    # (implement if needed)
    return CRR_INSTITUTION_RW_UK.get(cqs, CRR_INSTITUTION_RW_UK[None])


def get_corporate_rw(cqs: int | None) -> Decimal:
    """Get risk weight for corporate exposure (Art. 122)."""
    return CRR_CORPORATE_RW.get(cqs, CRR_CORPORATE_RW[None])


def get_retail_rw() -> Decimal:
    """Get risk weight for retail exposure (Art. 123)."""
    return CRR_RETAIL_RW


def get_residential_mortgage_rw(ltv: Decimal) -> tuple[Decimal, str]:
    """
    Get risk weight for residential mortgage (Art. 125).

    Returns:
        Tuple of (average_rw, treatment_description)

    CRR Treatment:
    - LTV <= 80%: 35% on whole exposure
    - LTV > 80%: Split approach or 50% average (simplified)
    """
    if ltv <= Decimal("0.80"):
        return CRR_RESIDENTIAL_RW_LOW_LTV, "35% RW (LTV <= 80%)"
    else:
        # Split treatment: 35% on portion up to 80% LTV, 75% on excess
        # Simplified: use weighted average or 50%
        # Here we calculate weighted average
        portion_low = Decimal("0.80") / ltv
        portion_high = (ltv - Decimal("0.80")) / ltv
        avg_rw = (CRR_RESIDENTIAL_RW_LOW_LTV * portion_low +
                  CRR_RESIDENTIAL_RW_HIGH_LTV * portion_high)
        return avg_rw, f"Split RW (LTV={ltv:.0%})"


def get_commercial_re_rw(ltv: Decimal, has_income_cover: bool = True) -> Decimal:
    """Get risk weight for commercial real estate (Art. 126)."""
    if ltv <= Decimal("0.50") and has_income_cover:
        return CRR_COMMERCIAL_RW_LOW_LTV
    return CRR_COMMERCIAL_RW_STANDARD


def apply_sme_supporting_factor(rwa: Decimal, is_sme: bool, turnover_gbp: Decimal | None) -> tuple[Decimal, bool]:
    """
    Apply SME supporting factor (Art. 501).

    Returns:
        Tuple of (adjusted_rwa, factor_applied)
    """
    if not is_sme:
        return rwa, False

    if turnover_gbp is not None and turnover_gbp <= Decimal("44000000"):
        return rwa * CRR_SME_SUPPORTING_FACTOR, True

    return rwa, False
```

### Task 2.3: CRR CCF Calculations

**File:** `workbooks/crr_expected_outputs/calculations/crr_ccf.py`

```python
"""CRR Credit Conversion Factors (Art. 111)."""

from decimal import Decimal
from ..data.crr_params import CRR_CCF


def get_ccf(commitment_type: str, maturity_years: float | None = None) -> Decimal:
    """
    Get CCF for off-balance sheet item (Art. 111).

    Categories under CRR:
    - Full risk (100%): Guarantees, credit derivatives, acceptances
    - Medium risk (50%): Undrawn commitments >1 year
    - Medium-low risk (20%): Undrawn commitments <=1 year, documentary credits
    - Low risk (0%): Unconditionally cancellable commitments
    """
    # Map commitment types to CCF categories
    ccf_mapping = {
        "guarantee_given": "full_risk",
        "acceptance": "full_risk",
        "credit_derivative": "full_risk",
        "undrawn_unconditional": "low_risk",
        "undrawn_conditional_short": "medium_low_risk",  # <=1 year
        "undrawn_conditional_long": "medium_risk",       # >1 year
        "documentary_credit": "medium_low_risk",
        "standby_lc": "medium_risk",
        "performance_guarantee": "medium_risk",
        "nif_ruf": "medium_risk",
    }

    category = ccf_mapping.get(commitment_type, "medium_risk")
    return CRR_CCF[category]


def calculate_ead_off_balance_sheet(
    nominal_amount: Decimal,
    commitment_type: str,
    maturity_years: float | None = None
) -> tuple[Decimal, Decimal]:
    """
    Calculate EAD for off-balance sheet item.

    Returns:
        Tuple of (ead, ccf_applied)
    """
    ccf = get_ccf(commitment_type, maturity_years)
    ead = nominal_amount * ccf
    return ead, ccf
```

### Task 2.4: CRR CRM Haircuts

**File:** `workbooks/crr_expected_outputs/calculations/crr_haircuts.py`

```python
"""CRR CRM supervisory haircuts (Art. 224)."""

from decimal import Decimal
from ..data.crr_params import CRR_HAIRCUTS


def get_collateral_haircut(
    collateral_type: str,
    cqs: int | None = None,
    residual_maturity_years: float | None = None,
    is_main_index: bool = False
) -> Decimal:
    """
    Get supervisory haircut for collateral (Art. 224).

    Args:
        collateral_type: Type of collateral (cash, gold, govt_bond, equity, etc.)
        cqs: Credit quality step of issuer (for debt securities)
        residual_maturity_years: Remaining maturity
        is_main_index: For equity, whether it's on a main index

    Returns:
        Haircut as decimal (e.g., 0.02 for 2%)
    """
    if collateral_type == "cash":
        return CRR_HAIRCUTS["cash"]

    if collateral_type == "gold":
        return CRR_HAIRCUTS["gold"]

    if collateral_type in ("govt_bond", "sovereign_bond"):
        if cqs == 1:
            if residual_maturity_years <= 1:
                return CRR_HAIRCUTS["govt_bond_cqs1_0_1y"]
            elif residual_maturity_years <= 5:
                return CRR_HAIRCUTS["govt_bond_cqs1_1_5y"]
            else:
                return CRR_HAIRCUTS["govt_bond_cqs1_5y_plus"]
        elif cqs in (2, 3):
            if residual_maturity_years <= 1:
                return CRR_HAIRCUTS["govt_bond_cqs2_3_0_1y"]
            elif residual_maturity_years <= 5:
                return CRR_HAIRCUTS["govt_bond_cqs2_3_1_5y"]
            else:
                return CRR_HAIRCUTS["govt_bond_cqs2_3_5y_plus"]
        # CQS 4+ or unrated
        return Decimal("0.15")

    if collateral_type == "equity":
        if is_main_index:
            return CRR_HAIRCUTS["equity_main_index"]
        return CRR_HAIRCUTS["equity_other"]

    # Default for other types
    return Decimal("0.25")


def get_fx_haircut(exposure_currency: str, collateral_currency: str) -> Decimal:
    """Get FX mismatch haircut (Art. 224)."""
    if exposure_currency != collateral_currency:
        return CRR_HAIRCUTS["currency_mismatch"]
    return Decimal("0.00")


def calculate_adjusted_collateral_value(
    collateral_value: Decimal,
    collateral_haircut: Decimal,
    fx_haircut: Decimal = Decimal("0.00")
) -> Decimal:
    """
    Calculate adjusted collateral value after haircuts.

    Formula: C_adjusted = C / (1 + Hc + Hfx)
    """
    total_haircut = collateral_haircut + fx_haircut
    return collateral_value / (1 + total_haircut)


def calculate_maturity_adjustment(
    collateral_value: Decimal,
    collateral_maturity_years: float,
    exposure_maturity_years: float
) -> Decimal:
    """
    Calculate maturity mismatch adjustment (Art. 238).

    If collateral maturity < exposure maturity, apply adjustment:
    Adjusted = C * (t - 0.25) / (T - 0.25)

    Where:
    - t = residual maturity of collateral (min 0.25 years)
    - T = residual maturity of exposure (min 0.25 years, max 5 years)
    """
    if collateral_maturity_years >= exposure_maturity_years:
        return collateral_value

    # Apply maturity adjustment
    t = max(collateral_maturity_years, 0.25)
    T = min(max(exposure_maturity_years, 0.25), 5.0)

    if t < 0.25:  # Collateral expired
        return Decimal("0.00")

    adjustment_factor = Decimal(str((t - 0.25) / (T - 0.25)))
    return collateral_value * adjustment_factor
```

### Task 2.5: CRR Scenario Files

Create scenario files for each CRR test group. Example for Group A (SA):

**File:** `workbooks/crr_expected_outputs/scenarios/group_crr_a_sa.py`

```python
"""CRR Group A: Standardised Approach scenarios."""

from decimal import Decimal
from dataclasses import dataclass

from workbooks.shared.fixture_loader import FixtureData
from ..calculations.crr_risk_weights import (
    get_sovereign_rw,
    get_institution_rw,
    get_corporate_rw,
    get_retail_rw,
    get_residential_mortgage_rw,
    get_commercial_re_rw,
    apply_sme_supporting_factor,
)
from ..calculations.crr_ccf import calculate_ead_off_balance_sheet


@dataclass
class ScenarioResult:
    """Result of a single scenario calculation."""
    scenario_id: str
    scenario_group: str
    description: str
    exposure_reference: str
    counterparty_reference: str
    regulatory_framework: str
    approach: str
    exposure_class: str
    ead: Decimal
    risk_weight: Decimal
    rwa: Decimal
    supporting_factor: Decimal | None
    calculation_details: dict
    regulatory_reference: str


def scenario_crr_a1_uk_sovereign(fixtures: FixtureData) -> ScenarioResult:
    """CRR-A1: UK Sovereign exposure with CQS1 = 0% RW."""
    # Inputs
    loan = fixtures.get_loan("LOAN_SOV_UK_001")
    counterparty = fixtures.get_counterparty("SOV_UK_001")
    rating = fixtures.get_rating("SOV_UK_001")

    ead = Decimal(str(loan["drawn_amount"]))
    cqs = rating["cqs"] if rating else 1
    risk_weight = get_sovereign_rw(cqs)
    rwa = ead * risk_weight

    return ScenarioResult(
        scenario_id="CRR-A1",
        scenario_group="CRR-A",
        description="UK Sovereign exposure with CQS1",
        exposure_reference="LOAN_SOV_UK_001",
        counterparty_reference="SOV_UK_001",
        regulatory_framework="CRR",
        approach="SA",
        exposure_class="CENTRAL_GOVERNMENTS_CENTRAL_BANKS",
        ead=ead,
        risk_weight=risk_weight,
        rwa=rwa,
        supporting_factor=None,
        calculation_details={
            "cqs": cqs,
            "rw_lookup": f"Art. 114 CQS {cqs}",
        },
        regulatory_reference="CRR Art. 114",
    )


def scenario_crr_a2_unrated_corporate(fixtures: FixtureData) -> ScenarioResult:
    """CRR-A2: Unrated corporate = 100% RW."""
    loan = fixtures.get_loan("LOAN_CORP_UR_001")

    ead = Decimal(str(loan["drawn_amount"]))
    risk_weight = get_corporate_rw(None)  # Unrated
    rwa = ead * risk_weight

    return ScenarioResult(
        scenario_id="CRR-A2",
        scenario_group="CRR-A",
        description="Unrated corporate exposure",
        exposure_reference="LOAN_CORP_UR_001",
        counterparty_reference="CORP_UR_001",
        regulatory_framework="CRR",
        approach="SA",
        exposure_class="CORPORATES",
        ead=ead,
        risk_weight=risk_weight,
        rwa=rwa,
        supporting_factor=None,
        calculation_details={
            "cqs": None,
            "rw_lookup": "Art. 122 unrated",
        },
        regulatory_reference="CRR Art. 122",
    )


def scenario_crr_a3_rated_corporate_cqs2(fixtures: FixtureData) -> ScenarioResult:
    """CRR-A3: Rated corporate CQS2 = 50% RW."""
    loan = fixtures.get_loan("LOAN_CORP_UK_003")
    rating = fixtures.get_rating("CORP_UK_003")

    ead = Decimal(str(loan["drawn_amount"]))
    cqs = rating["cqs"] if rating else 2
    risk_weight = get_corporate_rw(cqs)
    rwa = ead * risk_weight

    return ScenarioResult(
        scenario_id="CRR-A3",
        scenario_group="CRR-A",
        description="Rated corporate CQS2",
        exposure_reference="LOAN_CORP_UK_003",
        counterparty_reference="CORP_UK_003",
        regulatory_framework="CRR",
        approach="SA",
        exposure_class="CORPORATES",
        ead=ead,
        risk_weight=risk_weight,
        rwa=rwa,
        supporting_factor=None,
        calculation_details={
            "cqs": cqs,
            "rw_lookup": f"Art. 122 CQS {cqs}",
        },
        regulatory_reference="CRR Art. 122",
    )


def scenario_crr_a4_institution_cqs2_uk(fixtures: FixtureData) -> ScenarioResult:
    """CRR-A4: UK Institution CQS2 = 30% RW (UK deviation)."""
    loan = fixtures.get_loan("LOAN_INST_UK_003")
    rating = fixtures.get_rating("INST_UK_003")

    ead = Decimal(str(loan["drawn_amount"]))
    cqs = rating["cqs"] if rating else 2
    risk_weight = get_institution_rw(cqs, country="GB")  # UK deviation
    rwa = ead * risk_weight

    return ScenarioResult(
        scenario_id="CRR-A4",
        scenario_group="CRR-A",
        description="UK Institution CQS2 (UK deviation)",
        exposure_reference="LOAN_INST_UK_003",
        counterparty_reference="INST_UK_003",
        regulatory_framework="CRR",
        approach="SA",
        exposure_class="INSTITUTIONS",
        ead=ead,
        risk_weight=risk_weight,
        rwa=rwa,
        supporting_factor=None,
        calculation_details={
            "cqs": cqs,
            "uk_deviation": True,
            "standard_rw": "50%",
            "uk_rw": "30%",
            "rw_lookup": f"Art. 120 CQS {cqs} with UK deviation",
        },
        regulatory_reference="CRR Art. 120-121 + UK deviation",
    )


def scenario_crr_a5_residential_mortgage_low_ltv(fixtures: FixtureData) -> ScenarioResult:
    """CRR-A5: Residential mortgage LTV <= 80% = 35% RW."""
    loan = fixtures.get_loan("LOAN_RTL_MTG_001")
    collateral = fixtures.get_collateral_for_beneficiary("LOAN_RTL_MTG_001")

    ead = Decimal(str(loan["drawn_amount"]))
    ltv = Decimal(str(collateral[0]["property_ltv"])) if collateral else Decimal("0.75")
    risk_weight, treatment = get_residential_mortgage_rw(ltv)
    rwa = ead * risk_weight

    return ScenarioResult(
        scenario_id="CRR-A5",
        scenario_group="CRR-A",
        description="Residential mortgage LTV <= 80%",
        exposure_reference="LOAN_RTL_MTG_001",
        counterparty_reference="RTL_MTG_001",
        regulatory_framework="CRR",
        approach="SA",
        exposure_class="SECURED_BY_REAL_ESTATE",
        ead=ead,
        risk_weight=risk_weight,
        rwa=rwa,
        supporting_factor=None,
        calculation_details={
            "ltv": float(ltv),
            "treatment": treatment,
            "rw_lookup": "Art. 125",
        },
        regulatory_reference="CRR Art. 125",
    )


def scenario_crr_a10_sme_with_factor(fixtures: FixtureData) -> ScenarioResult:
    """CRR-A10: SME with supporting factor = 100% × 0.7619."""
    loan = fixtures.get_loan("LOAN_CORP_SME_001")
    counterparty = fixtures.get_counterparty("CORP_SME_001")

    ead = Decimal(str(loan["drawn_amount"]))
    risk_weight = get_corporate_rw(None)  # Assume unrated SME
    rwa_pre_factor = ead * risk_weight

    turnover = Decimal(str(counterparty.get("annual_revenue", 10_000_000)))
    rwa_post_factor, factor_applied = apply_sme_supporting_factor(
        rwa_pre_factor, is_sme=True, turnover_gbp=turnover
    )

    return ScenarioResult(
        scenario_id="CRR-A10",
        scenario_group="CRR-A",
        description="SME with supporting factor",
        exposure_reference="LOAN_CORP_SME_001",
        counterparty_reference="CORP_SME_001",
        regulatory_framework="CRR",
        approach="SA",
        exposure_class="CORPORATES",
        ead=ead,
        risk_weight=risk_weight,
        rwa=rwa_post_factor,
        supporting_factor=Decimal("0.7619") if factor_applied else None,
        calculation_details={
            "rwa_pre_factor": float(rwa_pre_factor),
            "sme_factor_applied": factor_applied,
            "sme_factor": 0.7619 if factor_applied else None,
            "turnover": float(turnover),
            "rw_lookup": "Art. 122 unrated + Art. 501 SME factor",
        },
        regulatory_reference="CRR Art. 122 + Art. 501",
    )


def get_all_crr_a_scenarios(fixtures: FixtureData) -> list[ScenarioResult]:
    """Generate all CRR Group A scenarios."""
    return [
        scenario_crr_a1_uk_sovereign(fixtures),
        scenario_crr_a2_unrated_corporate(fixtures),
        scenario_crr_a3_rated_corporate_cqs2(fixtures),
        scenario_crr_a4_institution_cqs2_uk(fixtures),
        scenario_crr_a5_residential_mortgage_low_ltv(fixtures),
        # Add remaining scenarios: A6, A7, A8, A9
        scenario_crr_a10_sme_with_factor(fixtures),
    ]
```

---

## Phase 3: Update Expected Outputs Structure

### Task 3.1: Create Dual Output Directories

**Target Structure:**
```
tests/
├── expected_outputs/
│   ├── crr/
│   │   ├── expected_rwa_crr.parquet
│   │   └── expected_rwa_crr.json
│   └── basel31/
│       ├── expected_rwa_b31.parquet
│       └── expected_rwa_b31.json
```

### Task 3.2: Update Output Schema

**File:** `src/rwa_calc/data/schemas.py` (additions)

```python
# Add framework-specific schema fields

CRR_OUTPUT_SCHEMA_ADDITIONS = {
    "regulatory_framework": pl.Utf8,  # "CRR"
    "sme_supporting_factor_applied": pl.Boolean,
    "sme_supporting_factor": pl.Float64,
    "infrastructure_supporting_factor_applied": pl.Boolean,
    "infrastructure_supporting_factor": pl.Float64,
    "crr_exposure_class": pl.Utf8,  # CRR Art. 112 classes
    "crr_ccf_category": pl.Utf8,
}

BASEL31_OUTPUT_SCHEMA_ADDITIONS = {
    "regulatory_framework": pl.Utf8,  # "BASEL_3_1"
    "output_floor_applicable": pl.Boolean,
    "rwa_pre_floor": pl.Float64,
    "rwa_sa_equivalent": pl.Float64,
    "rwa_floor_impact": pl.Float64,
    "is_floor_binding": pl.Boolean,
    "ltv_band": pl.Utf8,  # For granular RE treatment
    "pd_floor_applied": pl.Float64,  # Differentiated PD floors
    "lgd_floor_applied": pl.Float64,  # A-IRB LGD floors
}
```

---

## Phase 4: Create CRR Acceptance Tests

### Task 4.1: Create Test Directory Structure

```bash
mkdir -p tests/acceptance/crr
mkdir -p tests/acceptance/basel31
```

### Task 4.2: Create CRR Test Files

**File:** `tests/acceptance/crr/test_scenario_crr_a_sa.py`

```python
"""CRR Group A: Standardised Approach acceptance tests."""

import pytest
from decimal import Decimal

# These will fail until implementation is complete (TDD approach)


class TestCRRGroupA_StandardisedApproach:
    """CRR SA acceptance tests with hand-calculated expected outputs."""

    @pytest.fixture
    def crr_config(self):
        return {
            "regulatory_framework": "CRR",
            "reporting_date": "2025-12-31",
            "apply_sme_supporting_factor": True,
            "apply_infrastructure_factor": True,
        }

    def test_crr_a1_uk_sovereign_zero_rw(self, crr_config):
        """CRR-A1: UK Sovereign with CQS1 should have 0% risk weight."""
        # Expected values (hand-calculated)
        expected_ead = Decimal("1000000")
        expected_rw = Decimal("0.00")
        expected_rwa = Decimal("0")

        # This will fail until calculate_rwa is implemented
        pytest.skip("Implementation not yet complete")

        # from rwa_calc.engine.orchestrator import calculate_rwa
        # result = calculate_rwa(inputs, crr_config)
        # assert result.rwa == expected_rwa
        # assert result.risk_weight == expected_rw

    def test_crr_a2_unrated_corporate_100_rw(self, crr_config):
        """CRR-A2: Unrated corporate should have 100% risk weight."""
        expected_ead = Decimal("1000000")
        expected_rw = Decimal("1.00")
        expected_rwa = Decimal("1000000")

        pytest.skip("Implementation not yet complete")

    def test_crr_a4_uk_institution_30_rw_deviation(self, crr_config):
        """CRR-A4: UK Institution CQS2 gets 30% RW (UK deviation from 50%)."""
        expected_ead = Decimal("1000000")
        expected_rw = Decimal("0.30")  # UK deviation
        expected_rwa = Decimal("300000")

        pytest.skip("Implementation not yet complete")

    def test_crr_a5_residential_mortgage_35_rw(self, crr_config):
        """CRR-A5: Residential mortgage LTV <= 80% gets 35% RW."""
        expected_ead = Decimal("500000")
        expected_rw = Decimal("0.35")
        expected_rwa = Decimal("175000")

        pytest.skip("Implementation not yet complete")

    def test_crr_a10_sme_supporting_factor(self, crr_config):
        """CRR-A10: SME should have supporting factor applied."""
        expected_ead = Decimal("1000000")
        expected_rw = Decimal("1.00")
        expected_sme_factor = Decimal("0.7619")
        expected_rwa = Decimal("761900")  # 1m × 100% × 0.7619

        pytest.skip("Implementation not yet complete")
```

---

## Phase 5: Implementation Checklist

### Immediate Tasks (CRR Focus)

- [ ] **1. Create shared directory** (`workbooks/shared/`)
  - [ ] Move fixture_loader.py
  - [ ] Create irb_formulas.py

- [ ] **2. Rename existing workbook** (`rwa_expected_outputs/` → `basel31_expected_outputs/`)
  - [ ] Update all imports
  - [ ] Rename scenario files with `b31_` prefix

- [ ] **3. Create CRR workbook structure**
  - [ ] Create directory tree
  - [ ] Create `crr_params.py`
  - [ ] Create `crr_risk_weights.py`
  - [ ] Create `crr_ccf.py`
  - [ ] Create `crr_haircuts.py`
  - [ ] Create `crr_supporting_factors.py`

- [ ] **4. Create CRR scenarios**
  - [ ] Group CRR-A (SA) - 10 scenarios
  - [ ] Group CRR-B (F-IRB) - 6 scenarios
  - [ ] Group CRR-C (A-IRB) - 3 scenarios
  - [ ] Group CRR-D (CRM) - 6 scenarios
  - [ ] Group CRR-E (Slotting) - 4 scenarios
  - [ ] Group CRR-G (Provisions) - 3 scenarios
  - [ ] Group CRR-H (Complex) - 4 scenarios

- [ ] **5. Create expected outputs**
  - [ ] Generate `tests/expected_outputs/crr/expected_rwa_crr.parquet`
  - [ ] Generate `tests/expected_outputs/crr/expected_rwa_crr.json`

- [ ] **6. Create acceptance tests**
  - [ ] `tests/acceptance/crr/test_scenario_crr_a_sa.py`
  - [ ] `tests/acceptance/crr/test_scenario_crr_b_firb.py`
  - [ ] `tests/acceptance/crr/test_scenario_crr_c_airb.py`
  - [ ] `tests/acceptance/crr/test_scenario_crr_d_crm.py`
  - [ ] `tests/acceptance/crr/test_scenario_crr_e_slotting.py`
  - [ ] `tests/acceptance/crr/test_scenario_crr_g_provisions.py`
  - [ ] `tests/acceptance/crr/test_scenario_crr_h_complex.py`

### Later Tasks (Basel 3.1)

- [ ] Update Basel 3.1 scenarios to use `b31_` prefix
- [ ] Create `tests/acceptance/basel31/` test files
- [ ] Create comparison workbook

---

## Estimated Effort

| Task Group | Estimated Effort |
|------------|------------------|
| Phase 1: Restructure workbooks | 2-3 hours |
| Phase 2: Create CRR workbook | 4-6 hours |
| Phase 3: Update output structure | 1-2 hours |
| Phase 4: Create CRR acceptance tests | 2-3 hours |
| **Total** | **9-14 hours** |

---

## Dependencies

```
workbooks/shared/
    ├── fixture_loader.py     # Loads test fixtures
    └── irb_formulas.py       # Shared IRB calculations

workbooks/crr_expected_outputs/
    ├── data/
    │   └── crr_params.py     # CRR regulatory parameters
    ├── calculations/
    │   ├── crr_risk_weights.py    # Depends on crr_params
    │   ├── crr_ccf.py             # Depends on crr_params
    │   ├── crr_haircuts.py        # Depends on crr_params
    │   └── crr_supporting_factors.py  # Depends on crr_params
    └── scenarios/
        └── group_crr_*.py    # Depends on calculations + fixture_loader

tests/acceptance/crr/
    └── test_scenario_crr_*.py    # Depends on expected_outputs
```

---

## Success Criteria

1. **CRR workbook generates correct expected outputs** for all 36 CRR scenarios
2. **All CRR acceptance tests** are created (initially skipped/failing)
3. **Hand-calculated values** documented for each scenario
4. **Regulatory references** included for traceability
5. **Clear separation** between CRR and Basel 3.1 code paths
