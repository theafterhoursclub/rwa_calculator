"""
CRR Expected Outputs Generator

Generates expected RWA output files for CRR scenarios.
Outputs are saved to tests/expected_outputs/crr/

Usage:
    uv run python workbooks/crr_expected_outputs/generate_outputs.py
"""

import json
import sys
from pathlib import Path
from datetime import datetime
from decimal import Decimal
from dataclasses import dataclass, asdict
from typing import Any

import polars as pl

# Add project root to path
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from workbooks.shared.fixture_loader import load_fixtures
from workbooks.crr_expected_outputs.calculations.crr_risk_weights import (
    get_sovereign_rw,
    get_institution_rw,
    get_corporate_rw,
    get_retail_rw,
    get_residential_mortgage_rw,
    get_commercial_re_rw,
    calculate_sa_rwa,
)
from workbooks.crr_expected_outputs.calculations.crr_ccf import (
    get_ccf,
    calculate_ead_off_balance_sheet,
)
from workbooks.crr_expected_outputs.calculations.crr_supporting_factors import (
    apply_sme_supporting_factor,
    is_sme_eligible,
)
from workbooks.crr_expected_outputs.calculations.crr_irb import (
    apply_pd_floor,
    get_firb_lgd,
    calculate_irb_rwa,
)
from workbooks.shared.correlation import calculate_correlation


@dataclass
class CRRScenarioOutput:
    """Expected output for a CRR scenario."""
    scenario_id: str
    scenario_group: str
    description: str
    regulatory_framework: str
    approach: str
    exposure_class: str
    exposure_reference: str
    counterparty_reference: str
    # Inputs
    ead: float
    pd: float | None
    lgd: float | None
    maturity: float | None
    cqs: int | None
    ltv: float | None
    turnover: float | None
    # Outputs
    risk_weight: float
    rwa_before_sf: float
    supporting_factor: float
    rwa_after_sf: float
    expected_loss: float | None
    # Metadata
    regulatory_reference: str
    calculation_notes: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def decimal_to_float(obj):
    """JSON encoder for Decimal types."""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def generate_crr_a_scenarios(fixtures) -> list[CRRScenarioOutput]:
    """Generate CRR Group A (SA) scenario outputs."""
    scenarios = []

    # CRR-A1: UK Sovereign - 0% RW
    loan_a1 = fixtures.get_loan("LOAN_SOV_UK_001")
    cpty_a1 = fixtures.get_counterparty("SOV_UK_001")
    rating_a1 = fixtures.get_rating("SOV_UK_001")
    cqs_a1 = rating_a1["cqs"] if rating_a1 else 1
    ead_a1 = Decimal(str(loan_a1["drawn_amount"]))
    rw_a1 = get_sovereign_rw(cqs_a1)
    rwa_a1 = calculate_sa_rwa(ead_a1, rw_a1)

    scenarios.append(CRRScenarioOutput(
        scenario_id="CRR-A1",
        scenario_group="CRR-A",
        description="UK Sovereign exposure - 0% RW",
        regulatory_framework="CRR",
        approach="SA",
        exposure_class="SOVEREIGN",
        exposure_reference="LOAN_SOV_UK_001",
        counterparty_reference="SOV_UK_001",
        ead=float(ead_a1),
        pd=None,
        lgd=None,
        maturity=None,
        cqs=cqs_a1,
        ltv=None,
        turnover=None,
        risk_weight=float(rw_a1),
        rwa_before_sf=float(rwa_a1),
        supporting_factor=1.0,
        rwa_after_sf=float(rwa_a1),
        expected_loss=None,
        regulatory_reference="CRR Art. 114",
        calculation_notes=f"CQS {cqs_a1} -> 0% RW per Art. 114",
    ))

    # CRR-A2: Unrated Corporate - 100% RW
    loan_a2 = fixtures.get_loan("LOAN_CORP_UR_001")
    cpty_a2 = fixtures.get_counterparty("CORP_UR_001")
    ead_a2 = Decimal(str(loan_a2["drawn_amount"]))
    rw_a2 = get_corporate_rw(None)  # Unrated
    rwa_a2 = calculate_sa_rwa(ead_a2, rw_a2)

    scenarios.append(CRRScenarioOutput(
        scenario_id="CRR-A2",
        scenario_group="CRR-A",
        description="Unrated corporate - 100% RW",
        regulatory_framework="CRR",
        approach="SA",
        exposure_class="CORPORATE",
        exposure_reference="LOAN_CORP_UR_001",
        counterparty_reference="CORP_UR_001",
        ead=float(ead_a2),
        pd=None,
        lgd=None,
        maturity=None,
        cqs=None,
        ltv=None,
        turnover=None,
        risk_weight=float(rw_a2),
        rwa_before_sf=float(rwa_a2),
        supporting_factor=1.0,
        rwa_after_sf=float(rwa_a2),
        expected_loss=None,
        regulatory_reference="CRR Art. 122",
        calculation_notes="Unrated corporate -> 100% RW per Art. 122",
    ))

    # CRR-A3: Rated Corporate CQS 2 - 50% RW
    loan_a3 = fixtures.get_loan("LOAN_CORP_UK_003")
    rating_a3 = fixtures.get_rating("CORP_UK_003")
    cqs_a3 = rating_a3["cqs"] if rating_a3 else 2
    ead_a3 = Decimal(str(loan_a3["drawn_amount"]))
    rw_a3 = get_corporate_rw(cqs_a3)
    rwa_a3 = calculate_sa_rwa(ead_a3, rw_a3)

    scenarios.append(CRRScenarioOutput(
        scenario_id="CRR-A3",
        scenario_group="CRR-A",
        description="Rated corporate CQS 2 - 50% RW",
        regulatory_framework="CRR",
        approach="SA",
        exposure_class="CORPORATE",
        exposure_reference="LOAN_CORP_UK_003",
        counterparty_reference="CORP_UK_003",
        ead=float(ead_a3),
        pd=None,
        lgd=None,
        maturity=None,
        cqs=cqs_a3,
        ltv=None,
        turnover=None,
        risk_weight=float(rw_a3),
        rwa_before_sf=float(rwa_a3),
        supporting_factor=1.0,
        rwa_after_sf=float(rwa_a3),
        expected_loss=None,
        regulatory_reference="CRR Art. 122",
        calculation_notes=f"CQS {cqs_a3} -> 50% RW per Art. 122",
    ))

    # CRR-A4: UK Institution CQS 2 - 30% RW (UK Deviation)
    loan_a4 = fixtures.get_loan("LOAN_INST_UK_003")
    rating_a4 = fixtures.get_rating("INST_UK_003")
    cqs_a4 = rating_a4["cqs"] if rating_a4 else 2
    ead_a4 = Decimal(str(loan_a4["drawn_amount"]))
    rw_a4 = get_institution_rw(cqs_a4, country="GB", use_uk_deviation=True)
    rwa_a4 = calculate_sa_rwa(ead_a4, rw_a4)

    scenarios.append(CRRScenarioOutput(
        scenario_id="CRR-A4",
        scenario_group="CRR-A",
        description="UK Institution CQS 2 - 30% RW (UK deviation)",
        regulatory_framework="CRR",
        approach="SA",
        exposure_class="INSTITUTION",
        exposure_reference="LOAN_INST_UK_003",
        counterparty_reference="INST_UK_003",
        ead=float(ead_a4),
        pd=None,
        lgd=None,
        maturity=None,
        cqs=cqs_a4,
        ltv=None,
        turnover=None,
        risk_weight=float(rw_a4),
        rwa_before_sf=float(rwa_a4),
        supporting_factor=1.0,
        rwa_after_sf=float(rwa_a4),
        expected_loss=None,
        regulatory_reference="CRR Art. 120-121 (UK deviation)",
        calculation_notes="UK deviation: CQS 2 -> 30% RW (standard Basel is 50%)",
    ))

    # CRR-A5: Residential Mortgage 60% LTV - 35% RW
    loan_a5 = fixtures.get_loan("LOAN_RTL_MTG_001")
    ead_a5 = Decimal(str(loan_a5["drawn_amount"]))
    ltv_a5 = Decimal("0.60")  # Assume 60% LTV
    rw_a5, rw_desc_a5 = get_residential_mortgage_rw(ltv_a5)
    rwa_a5 = calculate_sa_rwa(ead_a5, rw_a5)

    scenarios.append(CRRScenarioOutput(
        scenario_id="CRR-A5",
        scenario_group="CRR-A",
        description="Residential mortgage 60% LTV - 35% RW",
        regulatory_framework="CRR",
        approach="SA",
        exposure_class="RETAIL_MORTGAGE",
        exposure_reference="LOAN_RTL_MTG_001",
        counterparty_reference="RTL_MTG_001",
        ead=float(ead_a5),
        pd=None,
        lgd=None,
        maturity=None,
        cqs=None,
        ltv=float(ltv_a5),
        turnover=None,
        risk_weight=float(rw_a5),
        rwa_before_sf=float(rwa_a5),
        supporting_factor=1.0,
        rwa_after_sf=float(rwa_a5),
        expected_loss=None,
        regulatory_reference="CRR Art. 125",
        calculation_notes=f"{rw_desc_a5}. CRR uses 35%/75% split at 80% LTV.",
    ))

    # CRR-A6: Residential Mortgage 85% LTV - Split Treatment
    loan_a6 = fixtures.get_loan("LOAN_RTL_MTG_002")
    ead_a6 = Decimal(str(loan_a6["drawn_amount"]))
    ltv_a6 = Decimal("0.85")
    rw_a6, rw_desc_a6 = get_residential_mortgage_rw(ltv_a6)
    rwa_a6 = calculate_sa_rwa(ead_a6, rw_a6)

    scenarios.append(CRRScenarioOutput(
        scenario_id="CRR-A6",
        scenario_group="CRR-A",
        description="Residential mortgage 85% LTV - split treatment",
        regulatory_framework="CRR",
        approach="SA",
        exposure_class="RETAIL_MORTGAGE",
        exposure_reference="LOAN_RTL_MTG_002",
        counterparty_reference="RTL_MTG_002",
        ead=float(ead_a6),
        pd=None,
        lgd=None,
        maturity=None,
        cqs=None,
        ltv=float(ltv_a6),
        turnover=None,
        risk_weight=float(rw_a6),
        rwa_before_sf=float(rwa_a6),
        supporting_factor=1.0,
        rwa_after_sf=float(rwa_a6),
        expected_loss=None,
        regulatory_reference="CRR Art. 125",
        calculation_notes=f"{rw_desc_a6}. 35% on portion up to 80% LTV, 75% on excess.",
    ))

    # CRR-A7: Commercial RE 40% LTV - 50% RW
    ead_a7 = Decimal("400000")
    ltv_a7 = Decimal("0.40")
    rw_a7, rw_desc_a7 = get_commercial_re_rw(ltv_a7, has_income_cover=True)
    rwa_a7 = calculate_sa_rwa(ead_a7, rw_a7)

    scenarios.append(CRRScenarioOutput(
        scenario_id="CRR-A7",
        scenario_group="CRR-A",
        description="Commercial RE 40% LTV - 50% RW",
        regulatory_framework="CRR",
        approach="SA",
        exposure_class="CRE",
        exposure_reference="LOAN_CRE_001",
        counterparty_reference="CORP_CRE_001",
        ead=float(ead_a7),
        pd=None,
        lgd=None,
        maturity=None,
        cqs=None,
        ltv=float(ltv_a7),
        turnover=None,
        risk_weight=float(rw_a7),
        rwa_before_sf=float(rwa_a7),
        supporting_factor=1.0,
        rwa_after_sf=float(rwa_a7),
        expected_loss=None,
        regulatory_reference="CRR Art. 126",
        calculation_notes=f"{rw_desc_a7}",
    ))

    # CRR-A8: Off-Balance Sheet - 50% CCF
    nominal_a8 = Decimal("1000000")
    ead_a8, ccf_a8, ead_desc_a8 = calculate_ead_off_balance_sheet(
        nominal_amount=nominal_a8,
        commitment_type="undrawn_long_term",
        original_maturity_years=2.0,
    )
    rw_a8 = get_corporate_rw(None)
    rwa_a8 = calculate_sa_rwa(ead_a8, rw_a8)

    scenarios.append(CRRScenarioOutput(
        scenario_id="CRR-A8",
        scenario_group="CRR-A",
        description="Off-balance sheet commitment - 50% CCF",
        regulatory_framework="CRR",
        approach="SA",
        exposure_class="CORPORATE",
        exposure_reference="CONT_CCF_001",
        counterparty_reference="CORP_OBS_001",
        ead=float(ead_a8),
        pd=None,
        lgd=None,
        maturity=None,
        cqs=None,
        ltv=None,
        turnover=None,
        risk_weight=float(rw_a8),
        rwa_before_sf=float(rwa_a8),
        supporting_factor=1.0,
        rwa_after_sf=float(rwa_a8),
        expected_loss=None,
        regulatory_reference="CRR Art. 111",
        calculation_notes=f"Nominal £{nominal_a8:,.0f} × {ccf_a8:.0%} CCF = £{ead_a8:,.0f} EAD",
    ))

    # CRR-A9: Retail - 75% RW
    loan_a9 = fixtures.get_loan("LOAN_RTL_IND_001")
    ead_a9 = Decimal(str(loan_a9["drawn_amount"]))
    rw_a9 = get_retail_rw()
    rwa_a9 = calculate_sa_rwa(ead_a9, rw_a9)

    scenarios.append(CRRScenarioOutput(
        scenario_id="CRR-A9",
        scenario_group="CRR-A",
        description="Retail exposure - 75% RW",
        regulatory_framework="CRR",
        approach="SA",
        exposure_class="RETAIL",
        exposure_reference="LOAN_RTL_IND_001",
        counterparty_reference="RTL_IND_001",
        ead=float(ead_a9),
        pd=None,
        lgd=None,
        maturity=None,
        cqs=None,
        ltv=None,
        turnover=None,
        risk_weight=float(rw_a9),
        rwa_before_sf=float(rwa_a9),
        supporting_factor=1.0,
        rwa_after_sf=float(rwa_a9),
        expected_loss=None,
        regulatory_reference="CRR Art. 123",
        calculation_notes="Retail -> 75% RW per Art. 123",
    ))

    # CRR-A10: SME Corporate with Supporting Factor
    loan_a10 = fixtures.get_loan("LOAN_CORP_SME_001")
    ead_a10 = Decimal(str(loan_a10["drawn_amount"]))
    turnover_a10 = Decimal("30000000")  # £30m
    rw_a10 = get_corporate_rw(None)
    rwa_before_sf_a10 = calculate_sa_rwa(ead_a10, rw_a10)
    rwa_after_sf_a10, sf_applied_a10, sf_desc_a10 = apply_sme_supporting_factor(
        rwa=rwa_before_sf_a10,
        is_sme=True,
        turnover=turnover_a10,
        currency="GBP",
    )
    sf_a10 = Decimal("0.7619") if sf_applied_a10 else Decimal("1.0")

    scenarios.append(CRRScenarioOutput(
        scenario_id="CRR-A10",
        scenario_group="CRR-A",
        description="SME corporate with supporting factor",
        regulatory_framework="CRR",
        approach="SA",
        exposure_class="CORPORATE_SME",
        exposure_reference="LOAN_CORP_SME_001",
        counterparty_reference="CORP_SME_001",
        ead=float(ead_a10),
        pd=None,
        lgd=None,
        maturity=None,
        cqs=None,
        ltv=None,
        turnover=float(turnover_a10),
        risk_weight=float(rw_a10),
        rwa_before_sf=float(rwa_before_sf_a10),
        supporting_factor=float(sf_a10),
        rwa_after_sf=float(rwa_after_sf_a10),
        expected_loss=None,
        regulatory_reference="CRR Art. 122 + Art. 501",
        calculation_notes=f"{sf_desc_a10}. NOT available under Basel 3.1.",
    ))

    # CRR-A11: SME Retail with Supporting Factor
    loan_a11 = fixtures.get_loan("LOAN_RTL_SME_001")
    ead_a11 = Decimal(str(loan_a11["drawn_amount"]))
    turnover_a11 = Decimal("750000")
    rw_a11 = get_retail_rw()
    rwa_before_sf_a11 = calculate_sa_rwa(ead_a11, rw_a11)
    rwa_after_sf_a11, sf_applied_a11, sf_desc_a11 = apply_sme_supporting_factor(
        rwa=rwa_before_sf_a11,
        is_sme=True,
        turnover=turnover_a11,
        currency="GBP",
    )
    sf_a11 = Decimal("0.7619") if sf_applied_a11 else Decimal("1.0")

    scenarios.append(CRRScenarioOutput(
        scenario_id="CRR-A11",
        scenario_group="CRR-A",
        description="SME retail with supporting factor",
        regulatory_framework="CRR",
        approach="SA",
        exposure_class="RETAIL_SME",
        exposure_reference="LOAN_RTL_SME_001",
        counterparty_reference="RTL_SME_001",
        ead=float(ead_a11),
        pd=None,
        lgd=None,
        maturity=None,
        cqs=None,
        ltv=None,
        turnover=float(turnover_a11),
        risk_weight=float(rw_a11),
        rwa_before_sf=float(rwa_before_sf_a11),
        supporting_factor=float(sf_a11),
        rwa_after_sf=float(rwa_after_sf_a11),
        expected_loss=None,
        regulatory_reference="CRR Art. 123 + Art. 501",
        calculation_notes=f"{sf_desc_a11}. NOT available under Basel 3.1.",
    ))

    # CRR-A12: Large Corporate - No Factor
    loan_a12 = fixtures.get_loan("LOAN_CORP_UK_001")
    ead_a12 = Decimal(str(loan_a12["drawn_amount"]))
    turnover_a12 = Decimal("500000000")  # £500m
    rw_a12 = get_corporate_rw(None)
    rwa_a12 = calculate_sa_rwa(ead_a12, rw_a12)

    scenarios.append(CRRScenarioOutput(
        scenario_id="CRR-A12",
        scenario_group="CRR-A",
        description="Large corporate (no supporting factor)",
        regulatory_framework="CRR",
        approach="SA",
        exposure_class="CORPORATE",
        exposure_reference="LOAN_CORP_UK_001",
        counterparty_reference="CORP_UK_001",
        ead=float(ead_a12),
        pd=None,
        lgd=None,
        maturity=None,
        cqs=None,
        ltv=None,
        turnover=float(turnover_a12),
        risk_weight=float(rw_a12),
        rwa_before_sf=float(rwa_a12),
        supporting_factor=1.0,
        rwa_after_sf=float(rwa_a12),
        expected_loss=None,
        regulatory_reference="CRR Art. 122",
        calculation_notes="Turnover > £44m threshold, SME factor not applicable.",
    ))

    return scenarios


def generate_crr_b_scenarios(fixtures) -> list[CRRScenarioOutput]:
    """Generate CRR Group B (F-IRB) scenario outputs."""
    scenarios = []

    # CRR-B1: Corporate F-IRB - Low PD
    loan_b1 = fixtures.get_loan("LOAN_CORP_UK_001")
    ead_b1 = float(loan_b1["drawn_amount"])
    pd_raw_b1 = 0.001  # 0.10%
    pd_floored_b1 = apply_pd_floor(pd_raw_b1)
    lgd_b1 = float(get_firb_lgd("unsecured"))
    maturity_b1 = 2.5
    correlation_b1 = calculate_correlation(pd_floored_b1, "CORPORATE")

    result_b1 = calculate_irb_rwa(
        ead=ead_b1,
        pd=pd_raw_b1,
        lgd=lgd_b1,
        correlation=correlation_b1,
        maturity=maturity_b1,
        exposure_class="CORPORATE",
    )

    scenarios.append(CRRScenarioOutput(
        scenario_id="CRR-B1",
        scenario_group="CRR-B",
        description="Corporate F-IRB - low PD",
        regulatory_framework="CRR",
        approach="F-IRB",
        exposure_class="CORPORATE",
        exposure_reference="LOAN_CORP_UK_001",
        counterparty_reference="CORP_UK_001",
        ead=ead_b1,
        pd=pd_floored_b1,
        lgd=lgd_b1,
        maturity=maturity_b1,
        cqs=None,
        ltv=None,
        turnover=None,
        risk_weight=result_b1["rwa"] / ead_b1 if ead_b1 > 0 else 0,
        rwa_before_sf=result_b1["rwa"],
        supporting_factor=1.0,
        rwa_after_sf=result_b1["rwa"],
        expected_loss=pd_floored_b1 * lgd_b1 * ead_b1,
        regulatory_reference="CRR Art. 153, 161, 162, 163",
        calculation_notes=f"PD {pd_raw_b1:.2%}, LGD {lgd_b1:.0%} (supervisory), K={result_b1['k']:.4f}",
    ))

    # CRR-B2: Corporate F-IRB - High PD
    ead_b2 = 5_000_000.0
    pd_raw_b2 = 0.05  # 5.00%
    pd_floored_b2 = apply_pd_floor(pd_raw_b2)
    lgd_b2 = float(get_firb_lgd("unsecured"))
    maturity_b2 = 3.0
    correlation_b2 = calculate_correlation(pd_floored_b2, "CORPORATE")

    result_b2 = calculate_irb_rwa(
        ead=ead_b2,
        pd=pd_raw_b2,
        lgd=lgd_b2,
        correlation=correlation_b2,
        maturity=maturity_b2,
        exposure_class="CORPORATE",
    )

    scenarios.append(CRRScenarioOutput(
        scenario_id="CRR-B2",
        scenario_group="CRR-B",
        description="Corporate F-IRB - high PD",
        regulatory_framework="CRR",
        approach="F-IRB",
        exposure_class="CORPORATE",
        exposure_reference="LOAN_CORP_UK_002",
        counterparty_reference="CORP_UK_002",
        ead=ead_b2,
        pd=pd_floored_b2,
        lgd=lgd_b2,
        maturity=maturity_b2,
        cqs=None,
        ltv=None,
        turnover=None,
        risk_weight=result_b2["rwa"] / ead_b2 if ead_b2 > 0 else 0,
        rwa_before_sf=result_b2["rwa"],
        supporting_factor=1.0,
        rwa_after_sf=result_b2["rwa"],
        expected_loss=pd_floored_b2 * lgd_b2 * ead_b2,
        regulatory_reference="CRR Art. 153, 161, 162",
        calculation_notes=f"High PD {pd_raw_b2:.2%} -> lower correlation {correlation_b2:.3f}",
    ))

    # CRR-B3: Subordinated - 75% LGD
    ead_b3 = 2_000_000.0
    pd_b3 = 0.01
    lgd_b3 = float(get_firb_lgd("unsecured", is_subordinated=True))  # 75%
    maturity_b3 = 4.0
    correlation_b3 = calculate_correlation(pd_b3, "CORPORATE")

    result_b3 = calculate_irb_rwa(
        ead=ead_b3,
        pd=pd_b3,
        lgd=lgd_b3,
        correlation=correlation_b3,
        maturity=maturity_b3,
        exposure_class="CORPORATE",
    )

    scenarios.append(CRRScenarioOutput(
        scenario_id="CRR-B3",
        scenario_group="CRR-B",
        description="Subordinated exposure - 75% LGD",
        regulatory_framework="CRR",
        approach="F-IRB",
        exposure_class="CORPORATE_SUBORDINATED",
        exposure_reference="LOAN_SUB_001",
        counterparty_reference="CORP_SUB_001",
        ead=ead_b3,
        pd=pd_b3,
        lgd=lgd_b3,
        maturity=maturity_b3,
        cqs=None,
        ltv=None,
        turnover=None,
        risk_weight=result_b3["rwa"] / ead_b3 if ead_b3 > 0 else 0,
        rwa_before_sf=result_b3["rwa"],
        supporting_factor=1.0,
        rwa_after_sf=result_b3["rwa"],
        expected_loss=pd_b3 * lgd_b3 * ead_b3,
        regulatory_reference="CRR Art. 153, 161",
        calculation_notes=f"Subordinated: LGD {lgd_b3:.0%} (vs 45% senior)",
    ))

    return scenarios


def generate_all_outputs():
    """Generate all CRR expected outputs."""
    print("Loading fixtures...")
    fixtures = load_fixtures()

    print("Generating CRR-A (SA) scenarios...")
    crr_a_scenarios = generate_crr_a_scenarios(fixtures)
    print(f"  Generated {len(crr_a_scenarios)} SA scenarios")

    print("Generating CRR-B (F-IRB) scenarios...")
    crr_b_scenarios = generate_crr_b_scenarios(fixtures)
    print(f"  Generated {len(crr_b_scenarios)} F-IRB scenarios")

    all_scenarios = crr_a_scenarios + crr_b_scenarios

    # Build output structure
    output = {
        "framework": "CRR",
        "version": "1.0",
        "effective_until": "2026-12-31",
        "generated_at": datetime.now().isoformat(),
        "regulatory_references": {
            "primary": "UK CRR (Regulation (EU) No 575/2013 as retained)",
            "supporting_factors": "CRR Art. 501 (SME), Art. 501a (Infrastructure)",
            "sa_risk_weights": "CRR Art. 112-134",
            "irb_approach": "CRR Art. 142-191",
        },
        "key_differences_from_basel31": [
            "SME supporting factor (0.7619) available under CRR",
            "Infrastructure supporting factor (0.75) available under CRR",
            "No output floor under CRR",
            "Residential mortgage: 35%/75% split at 80% LTV (vs granular LTV bands)",
            "Single PD floor 0.03% for all classes (vs differentiated floors)",
            "No A-IRB LGD floors under CRR",
        ],
        "scenario_count": len(all_scenarios),
        "scenarios": [s.to_dict() for s in all_scenarios],
    }

    return output, all_scenarios


def save_outputs(output: dict, scenarios: list[CRRScenarioOutput]):
    """Save outputs to files."""
    output_dir = project_root / "tests" / "expected_outputs" / "crr"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save JSON
    json_path = output_dir / "expected_rwa_crr.json"
    with open(json_path, 'w') as f:
        json.dump(output, f, indent=2, default=decimal_to_float)
    print(f"Saved JSON: {json_path}")

    # Save Parquet using Polars
    df_data = [s.to_dict() for s in scenarios]
    df = pl.DataFrame(df_data)
    parquet_path = output_dir / "expected_rwa_crr.parquet"
    df.write_parquet(parquet_path)
    print(f"Saved Parquet: {parquet_path}")

    # Save CSV for easy viewing
    csv_path = output_dir / "expected_rwa_crr.csv"
    df.write_csv(csv_path)
    print(f"Saved CSV: {csv_path}")

    return json_path, parquet_path, csv_path


def main():
    """Main entry point."""
    print("=" * 60)
    print("CRR Expected Outputs Generator")
    print("=" * 60)

    output, scenarios = generate_all_outputs()

    print("\n" + "-" * 60)
    print("Saving outputs...")
    json_path, parquet_path, csv_path = save_outputs(output, scenarios)

    print("\n" + "=" * 60)
    print("Generation complete!")
    print(f"Total scenarios: {len(scenarios)}")
    print(f"\nOutput files:")
    print(f"  - {json_path}")
    print(f"  - {parquet_path}")
    print(f"  - {csv_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
