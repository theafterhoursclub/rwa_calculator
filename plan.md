# RWA Calculator Implementation Plan

## Overview

This plan follows a **test-first approach** starting with User Acceptance Tests (UATs) that define expected inputs and outputs, then establishing contracts between components to enable isolated development and testing.

---

## Current Status

### Phase 1.1: Test Data Fixtures - COMPLETE

All test data fixtures have been created with Python generators and parquet output files.

| Fixture Type | Status | Records | Generator |
|--------------|--------|---------|-----------|
| Counterparty - Sovereign | Complete | 9 | `counterparty/sovereign.py` |
| Counterparty - Institution | Complete | 12 | `counterparty/institution.py` |
| Counterparty - Corporate | Complete | 24 | `counterparty/corporate.py` |
| Counterparty - Retail | Complete | 26 | `counterparty/retail.py` |
| Counterparty - Specialised Lending | Complete | 14 | `counterparty/specialised_lending.py` |
| Exposures - Facilities | Complete | 17 | `exposures/facilities.py` |
| Exposures - Loans | Complete | 28 | `exposures/loans.py` |
| Exposures - Contingents | Complete | 17 | `exposures/contingents.py` |
| Exposures - Facility Mapping | Complete | 15 | `exposures/facility_mapping.py` |
| Collateral | Complete | 22 | `collateral/collateral.py` |
| Guarantee | Complete | 15 | `guarantee/guarantee.py` |
| Provision | Complete | 18 | `provision/provision.py` |
| Ratings | Complete | 73 | `ratings/ratings.py` |
| Mapping - Org | Complete | 8 | `mapping/org_mapping.py` |
| Mapping - Lending | Complete | 7 | `mapping/lending_mapping.py` |

### Phase 1.2: Acceptance Test Scenarios - NOT STARTED

### Phase 2: Process Contracts - NOT STARTED

### Phase 3: Implementation - NOT STARTED

---

## Phase 1: User Acceptance Tests

Define end-to-end acceptance tests that verify the complete calculation pipeline produces correct RWA outputs for known inputs.

### 1.1 Test Data Fixtures

Test datasets have been created covering key regulatory scenarios. Each fixture type has:
- A Python generator module with dataclass definitions
- A `create_*()` function returning a Polars DataFrame
- A `save_*()` function writing to parquet format
- A `generate_all.py` runner script with summary output

```
tests/
├── fixtures/
│   ├── counterparty/
│   │   ├── sovereign.py              # UK Govt, US Govt, Brazil, Argentina, etc.
│   │   ├── institution.py            # Banks (Barclays, HSBC, JPMorgan), CCPs
│   │   ├── corporate.py              # Large corp, SME, unrated, org hierarchy groups
│   │   ├── retail.py                 # Individuals, mortgages, SME retail, lending groups
│   │   ├── specialised_lending.py    # Project finance, IPRE, HVCRE, object finance
│   │   ├── __init__.py
│   │   └── generate_all.py
│   ├── exposures/
│   │   ├── facilities.py             # RCFs, term facilities, mortgages, hierarchy test
│   │   ├── loans.py                  # Drawn exposures for all acceptance scenarios
│   │   ├── contingents.py            # Off-balance sheet (LCs, guarantees, commitments)
│   │   ├── facility_mapping.py       # Facility-to-loan/contingent relationships
│   │   ├── __init__.py
│   │   └── generate_all.py
│   ├── collateral/
│   │   ├── collateral.py             # Cash, bonds, equity, real estate, receivables
│   │   ├── __init__.py
│   │   └── generate_all.py
│   ├── guarantee/
│   │   ├── guarantee.py              # Sovereign, bank, corporate guarantees
│   │   ├── __init__.py
│   │   └── generate_all.py
│   ├── provision/
│   │   ├── provision.py              # SCRA/GCRA, IFRS9 stages 1-3
│   │   ├── __init__.py
│   │   └── generate_all.py
│   ├── ratings/
│   │   ├── ratings.py                # External (S&P/Moody's) and internal ratings
│   │   ├── __init__.py
│   │   └── generate_all.py
│   └── mapping/
│       ├── org_mapping.py            # Parent-subsidiary relationships
│       ├── lending_mapping.py        # Retail lending group connections
│       ├── __init__.py
│       └── generate_all.py
```

#### Fixture Data Coverage

**Counterparties (85 total):**
- Sovereigns: CQS 1-6 coverage (UK, US, Germany, Saudi Arabia, Mexico, Brazil, Argentina), unrated, defaulted
- Institutions: UK banks (CQS 1-2), US banks, German banks, CCPs, unrated
- Corporates: Large corp (CQS 1-3), SME, unrated, defaulted, 3 org hierarchy test groups
- Retail: Individuals, mortgage borrowers, QRRE, SME retail, defaulted, 5 lending groups
- Specialised Lending: Project finance, IPRE, HVCRE, object finance across slotting categories

**Exposures (77 total):**
- Facilities: Corporate RCFs, term facilities, mortgages, QRRE, hierarchy test facilities
- Loans: Coverage for all acceptance test scenarios (A1-A10, H1-H4)
- Contingents: All CCF categories (0%, 20%, 40%, 50%, 100%)
- Facility mappings: Single facility with multiple loans, multi-level hierarchies

**Credit Risk Mitigation:**
- Collateral: Cash (0% haircut), govt bonds, equity (25%), real estate (LTV-based), receivables
- Guarantees: Sovereign, bank (D4 scenario), corporate/parent guarantees
- Provisions: Stage 1/2/3, SCRA/GCRA, H4 full CRM chain

**Hierarchy Test Data:**
- Org hierarchy: 3 corporate groups for rating inheritance testing
- Lending groups: 5 retail groups for threshold aggregation testing
- Exposure hierarchy: H1 (multiple loans), H2 (multi-level facilities)

### 1.2 Acceptance Test Scenarios

Each scenario defines **specific inputs** and **expected outputs** with hand-calculated values.

#### Test Data to Scenario Mapping

| Scenario | Counterparty | Loan/Exposure | Collateral | Guarantee | Provision |
|----------|--------------|---------------|------------|-----------|-----------|
| A1 | SOV_UK_001 | LOAN_SOV_UK_001 | - | - | - |
| A2 | CORP_UR_001 | LOAN_CORP_UR_001 | COLL_EQ_001 | - | PROV_S2_CORP_001 |
| A3 | CORP_UK_003 | LOAN_CORP_UK_003 | COLL_GILT_001 | GUAR_BANK_001 | - |
| A4 | INST_UK_003 | LOAN_INST_UK_003 | - | - | - |
| A5 | RTL_MTG_001 | LOAN_RTL_MTG_001 | COLL_RRE_001 (60% LTV) | - | PROV_S1_MTG_001 |
| A6 | RTL_MTG_002 | LOAN_RTL_MTG_002 | COLL_RRE_002 (85% LTV) | - | - |
| A8 | Various | CONT_CCF_* | - | - | - |
| A9 | RTL_IND_001 | LOAN_RTL_IND_001 | - | - | - |
| A10 | RTL_SME_001 | LOAN_RTL_SME_001 | COLL_REC_002 | - | PROV_S2_SME_001 |
| D1 | CORP_UK_001 | LOAN_CORP_UK_001 | COLL_CASH_001 | - | - |
| D2 | CORP_UK_003 | LOAN_CORP_UK_003 | COLL_GILT_001 | - | - |
| D3 | CORP_UR_001 | LOAN_CORP_UR_001 | COLL_EQ_001 | - | - |
| D4 | CORP_UK_003 | LOAN_CORP_UK_003 | - | GUAR_BANK_001 | - |
| D5 | CORP_GRP2_OPSUB1 | LOAN_HIER_SUB_001_A | COLL_MAT_MISMATCH_001 | - | - |
| D6 | CORP_GRP2_OPSUB2 | LOAN_HIER_SUB_002_A | COLL_CCY_MISMATCH_001 | - | - |
| H1 | CORP_GRP1_PARENT | LOAN_HIER_001_A/B/C | COLL_CRE_003/004 | GUAR_MAT_MISMATCH_001 | PROV_S2_HIER_001 |
| H2 | CORP_GRP1_* | via org_mapping | - | GUAR_CORP_001 | - |
| H4 | CORP_UK_002 | LOAN_FAC_CORP_002_A | COLL_CRE_002 | GUAR_CRM_CHAIN_001 | PROV_CRM_CHAIN_001 |

#### Scenario Group A: Standardised Approach (SA)

| ID | Description | Key Inputs | Expected RWA | Regulatory Basis |
|----|-------------|------------|--------------|------------------|
| A1 | UK Sovereign exposure | £1m loan to UK Govt, CQS1 | £0 (0% RW) | CRE20.7 |
| A2 | Unrated corporate | £1m loan, no rating, no SME | £1m (100% RW) | CRE20.26 |
| A3 | Rated corporate CQS2 | £1m loan, A-rated | £500k (50% RW) | CRE20.25 |
| A4 | Institution ECRA CQS2 | £1m loan to UK bank, A-rated | £300k (30% RW) | UK deviation |
| A5 | Residential mortgage 60% LTV | £500k loan, £833k property | £100k (20% RW) | CRE20.71 |
| A6 | Residential mortgage 85% LTV | £850k loan, £1m property | £297.5k (35% RW) | CRE20.71 |
| A7 | Commercial real estate 60% LTV | £600k loan, £1m property | £360k (60% RW) | CRE20.83 |
| A8 | Off-balance sheet commitment | £1m undrawn, 40% CCF | £400k EAD | CRE20.94 |
| A9 | Retail exposure | £50k loan to individual | £37.5k (75% RW) | CRE20.66 |
| A10 | SME retail (under threshold) | £500k loan, SME turnover <£880k | £375k (75% RW) | CRE20.66 |

#### Scenario Group B: Foundation IRB (F-IRB)

| ID | Description | Key Inputs | Expected RWA | Notes |
|----|-------------|------------|--------------|-------|
| B1 | Corporate unsecured | PD=1%, LGD=45%, M=2.5y, EAD=£1m | Calculate using IRB formula | Supervisory LGD |
| B2 | Corporate with financial collateral | PD=1%, £500k cash collateral, EAD=£1m | Adjusted LGD calculation | LGD=0% for cash |
| B3 | Corporate with real estate | PD=1%, £1m property (LTV 60%), EAD=£1m | LGD=20% (RE portion) | FIRB supervisory LGD |
| B4 | Retail mortgage | PD=0.5%, LGD=10%, EAD=£500k | IRB retail formula | Different correlation |
| B5 | QRRE (qualifying revolving) | PD=0.5%, LGD=85%, EAD=£10k | IRB QRRE formula | Higher correlation |
| B6 | PD floor test | Internal PD=0.01%, Floor=0.03% | Uses floored PD | Basel 3.1 PD floors |

#### Scenario Group C: Advanced IRB (A-IRB)

| ID | Description | Key Inputs | Expected RWA | Notes |
|----|-------------|------------|--------------|-------|
| C1 | Corporate own LGD | PD=1%, own LGD=35%, M=2.5y, EAD=£1m | IRB calculation | Bank-estimated LGD |
| C2 | LGD floor test | Internal LGD=5%, Floor=25% | Uses floored LGD | Basel 3.1 LGD floors |
| C3 | Retail with own estimates | PD=0.3%, LGD=15%, EAD=£100k | IRB retail formula | Own PD and LGD |

#### Scenario Group D: Credit Risk Mitigation (CRM)

| ID | Description | Key Inputs | Expected RWA | Notes |
|----|-------------|------------|--------------|-------|
| D1 | Cash collateral (SA) | £1m exposure, £500k cash | £500k EAD after CRM | 0% haircut for cash |
| D2 | Govt bond collateral | £1m exp, £600k gilts (5y) | Haircut applied | Supervisory haircut |
| D3 | Equity collateral | £1m exp, £400k listed equity | Higher haircut | 25% haircut |
| D4 | Guarantee substitution | £1m corp, £600k bank guarantee | Split RW calculation | Substitution approach |
| D5 | Maturity mismatch | £1m exp 5y, £500k collateral 2y | Adjusted collateral value | Maturity adjustment |
| D6 | Currency mismatch | £1m GBP exp, €500k collateral | FX haircut applied | 8% FX haircut |

#### Scenario Group E: Specialised Lending (Slotting)

| ID | Description | Key Inputs | Expected RWA | Notes |
|----|-------------|------------|--------------|-------|
| E1 | Project finance - Strong | £10m, Strong category | £7m (70% RW) | Slotting approach |
| E2 | Project finance - Good | £10m, Good category | £9m (90% RW) | Slotting approach |
| E3 | IPRE - Speculative | £5m, Speculative category | £5.75m (115% RW) | Higher RW |
| E4 | HVCRE | £5m, High volatility CRE | Higher RW applied | HVCRE treatment |

#### Scenario Group F: Output Floor (Basel 3.1 only)

| ID | Description | Key Inputs | Expected RWA | Notes |
|----|-------------|------------|--------------|-------|
| F1 | Floor binding | IRB RWA=£50m, SA RWA=£100m | £72.5m | 72.5% floor |
| F2 | Floor not binding | IRB RWA=£80m, SA RWA=£100m | £80m | IRB > floor |
| F3 | Transitional floor | 2027: 50%, 2028: 55%... | Phased-in floor | Transitional provisions |

#### Scenario Group G: Provisions & Impairments

| ID | Description | Key Inputs | Expected RWA | Notes |
|----|-------------|------------|--------------|-------|
| G1 | SA with SCRA | £1m exposure, £50k specific provision | £950k net EAD | SA treatment |
| G2 | IRB Stage 1 | £1m exp, £10k Stage 1 ECL | EL comparison | IRB EL vs provisions |
| G3 | IRB Stage 3 | £1m defaulted, £400k Stage 3 provision | Defaulted treatment | LGD in-default |

#### Scenario Group H: Complex/Combined

| ID | Description | Key Inputs | Expected RWA | Notes |
|----|-------------|------------|--------------|-------|
| H1 | Facility with multiple loans | £5m facility, 3 loans drawn | Aggregate correctly | Hierarchy test |
| H2 | Counterparty group | Parent + 2 subsidiaries | Rating inheritance | Org hierarchy |
| H3 | Mixed approach portfolio | Some SA, some IRB exposures | Correct separation | Approach routing |
| H4 | Full CRM chain | Exposure + collateral + guarantee + provision | All CRM applied | Integration test |

### 1.3 Acceptance Test Implementation

```python
# tests/acceptance/test_rwa_scenarios.py

import pytest
import polars as pl
from decimal import Decimal

from rwa_calc.engine.orchestrator import calculate_rwa
from rwa_calc.data.loaders import load_test_fixtures


class TestScenarioA_StandardisedApproach:
    """SA acceptance tests with hand-calculated expected outputs."""

    @pytest.fixture
    def sa_config(self):
        return {
            "basel_version": "3.1",
            "reporting_date": "2027-01-01",
            "output_floor_percentage": 0.725,
        }

    def test_a1_uk_sovereign_zero_rw(self, sa_config):
        """UK Sovereign with CQS1 should have 0% risk weight."""
        # Arrange
        inputs = {
            "counterparty": {"type": "sovereign", "country": "GB", "cqs": 1},
            "exposure": {"ead": Decimal("1000000"), "currency": "GBP"},
        }
        expected_rwa = Decimal("0")
        expected_rw = Decimal("0.00")

        # Act
        result = calculate_rwa(inputs, sa_config)

        # Assert
        assert result.rwa == expected_rwa
        assert result.risk_weight == expected_rw
        assert result.approach == "SA"
        assert result.exposure_class == "SOVEREIGN"

    def test_a4_uk_institution_cqs2_30pct(self, sa_config):
        """UK deviation: Institution CQS2 gets 30% RW (not Basel standard 50%)."""
        inputs = {
            "counterparty": {"type": "institution", "country": "GB", "cqs": 2},
            "exposure": {"ead": Decimal("1000000"), "currency": "GBP"},
        }
        expected_rwa = Decimal("300000")  # 30% of £1m
        expected_rw = Decimal("0.30")

        result = calculate_rwa(inputs, sa_config)

        assert result.rwa == expected_rwa
        assert result.risk_weight == expected_rw

    def test_a5_residential_mortgage_60_ltv(self, sa_config):
        """Residential mortgage at 60% LTV should have 20% RW."""
        inputs = {
            "counterparty": {"type": "retail", "country": "GB"},
            "exposure": {"ead": Decimal("500000"), "currency": "GBP"},
            "collateral": {
                "type": "residential_real_estate",
                "value": Decimal("833333"),
                "ltv": Decimal("0.60"),
            },
        }
        expected_rwa = Decimal("100000")  # 20% of £500k

        result = calculate_rwa(inputs, sa_config)

        assert result.rwa == expected_rwa
```

---

## Phase 2: Process Contracts

Define clear interfaces between components to enable isolated testing and parallel development.

### 2.1 Contract Definitions

Each contract specifies:
- **Input schema** (Polars DataFrame schema)
- **Output schema** (Polars DataFrame schema)
- **Invariants** (rules that must always hold)
- **Error handling** (how failures are communicated)

### 2.2 Core Contracts

#### Contract 1: Data Loader Contract

```python
# src/rwa_calc/contracts/loader_contract.py

from dataclasses import dataclass
from typing import Protocol
import polars as pl

from rwa_calc.data.schemas import (
    COUNTERPARTY_SCHEMA,
    FACILITY_SCHEMA,
    LOAN_SCHEMA,
    CONTINGENTS_SCHEMA,
    COLLATERAL_SCHEMA,
    GUARANTEE_SCHEMA,
    PROVISION_SCHEMA,
    RATINGS_SCHEMA,
)


@dataclass
class LoaderOutput:
    """Output contract for data loaders."""
    counterparties: pl.LazyFrame  # Must match COUNTERPARTY_SCHEMA
    facilities: pl.LazyFrame      # Must match FACILITY_SCHEMA
    loans: pl.LazyFrame           # Must match LOAN_SCHEMA
    contingents: pl.LazyFrame     # Must match CONTINGENTS_SCHEMA
    collateral: pl.LazyFrame      # Must match COLLATERAL_SCHEMA
    guarantees: pl.LazyFrame      # Must match GUARANTEE_SCHEMA
    provisions: pl.LazyFrame      # Must match PROVISION_SCHEMA
    ratings: pl.LazyFrame         # Must match RATINGS_SCHEMA
    facility_mappings: pl.LazyFrame
    org_mappings: pl.LazyFrame
    lending_mappings: pl.LazyFrame


class LoaderProtocol(Protocol):
    """Contract that all loaders must implement."""

    def load(self) -> LoaderOutput:
        """Load and return all required data."""
        ...

    def validate(self) -> list[str]:
        """Validate loaded data, return list of validation errors."""
        ...
```

#### Contract 2: Hierarchy Builder Contract

```python
# src/rwa_calc/contracts/hierarchy_contract.py

from dataclasses import dataclass
import polars as pl


@dataclass
class ExposureHierarchyInput:
    """Input contract for exposure hierarchy builder."""
    facilities: pl.LazyFrame
    loans: pl.LazyFrame
    contingents: pl.LazyFrame
    facility_mappings: pl.LazyFrame


@dataclass
class ExposureHierarchyOutput:
    """Output contract for exposure hierarchy builder."""
    # Flattened exposures with facility context propagated
    exposures: pl.LazyFrame
    # Schema additions:
    #   - exposure_reference: str (loan/contingent reference)
    #   - facility_reference: str | None (parent facility if exists)
    #   - exposure_type: Literal["loan", "contingent"]
    #   - ead: Decimal (drawn_amount for loans, nominal * CCF for contingents)
    #   - hierarchy_level: int (1 = standalone, 2+ = under facility)


@dataclass
class CounterpartyHierarchyInput:
    """Input contract for counterparty hierarchy builder."""
    counterparties: pl.LazyFrame
    org_mappings: pl.LazyFrame
    lending_mappings: pl.LazyFrame
    ratings: pl.LazyFrame


@dataclass
class CounterpartyHierarchyOutput:
    """Output contract for counterparty hierarchy builder."""
    # Counterparties with inherited attributes
    counterparties: pl.LazyFrame
    # Schema additions:
    #   - ultimate_parent_reference: str
    #   - effective_rating: str (inherited if no own rating)
    #   - effective_cqs: int
    #   - lending_group_reference: str | None
    #   - lending_group_total_exposure: Decimal (for retail threshold)
```

#### Contract 3: Exposure Classification Contract

```python
# src/rwa_calc/contracts/classification_contract.py

from dataclasses import dataclass
from typing import Literal
import polars as pl


ExposureClass = Literal[
    "SOVEREIGN",
    "PSE",
    "MDB",
    "INSTITUTION",
    "CORPORATE",
    "CORPORATE_SME",
    "SPECIALISED_LENDING",
    "RETAIL",
    "RETAIL_MORTGAGE",
    "RETAIL_QRRE",
    "RETAIL_SME",
    "EQUITY",
    "OTHER",
    "DEFAULTED",
]

Approach = Literal["SA", "FIRB", "AIRB"]


@dataclass
class ClassificationInput:
    """Input contract for exposure classifier."""
    exposures: pl.LazyFrame           # From hierarchy builder
    counterparties: pl.LazyFrame      # From hierarchy builder
    irb_permissions: pl.LazyFrame     # Which classes can use IRB
    config: dict                       # Basel version, reporting date, etc.


@dataclass
class ClassificationOutput:
    """Output contract for exposure classifier."""
    classified_exposures: pl.LazyFrame
    # Schema additions:
    #   - exposure_class: ExposureClass
    #   - approach: Approach
    #   - is_defaulted: bool
    #   - classification_reason: str (audit trail)
```

#### Contract 4: CRM Processor Contract

```python
# src/rwa_calc/contracts/crm_contract.py

from dataclasses import dataclass
import polars as pl


@dataclass
class CRMInput:
    """Input contract for CRM processor."""
    classified_exposures: pl.LazyFrame
    collateral: pl.LazyFrame
    guarantees: pl.LazyFrame
    provisions: pl.LazyFrame
    haircut_lookup: pl.LazyFrame
    config: dict


@dataclass
class CRMOutput:
    """Output contract for CRM processor."""
    mitigated_exposures: pl.LazyFrame
    # Schema additions:
    #   - ead_pre_crm: Decimal
    #   - ead_post_crm: Decimal
    #   - collateral_value_adjusted: Decimal
    #   - guarantee_value_adjusted: Decimal
    #   - provision_deducted: Decimal
    #   - lgd_adjusted: Decimal (for IRB)
    #   - substitute_rw: Decimal | None (for guarantee substitution)
    #   - crm_details: str (audit trail JSON)
```

#### Contract 5: SA Calculator Contract

```python
# src/rwa_calc/contracts/sa_contract.py

from dataclasses import dataclass
import polars as pl


@dataclass
class SACalculatorInput:
    """Input contract for SA calculator."""
    mitigated_exposures: pl.LazyFrame  # Only SA-approach exposures
    risk_weight_lookups: dict[str, pl.LazyFrame]  # By exposure class
    config: dict


@dataclass
class SACalculatorOutput:
    """Output contract for SA calculator."""
    sa_results: pl.LazyFrame
    # Schema:
    #   - exposure_reference: str
    #   - exposure_class: str
    #   - ead: Decimal
    #   - risk_weight: Decimal
    #   - rwa: Decimal
    #   - rw_lookup_key: str (audit trail)
```

#### Contract 6: IRB Calculator Contract

```python
# src/rwa_calc/contracts/irb_contract.py

from dataclasses import dataclass
import polars as pl


@dataclass
class IRBCalculatorInput:
    """Input contract for IRB calculator."""
    mitigated_exposures: pl.LazyFrame  # Only IRB-approach exposures
    pd_floors: pl.LazyFrame
    lgd_floors: pl.LazyFrame           # For A-IRB only
    correlation_params: pl.LazyFrame
    config: dict


@dataclass
class IRBCalculatorOutput:
    """Output contract for IRB calculator."""
    irb_results: pl.LazyFrame
    # Schema:
    #   - exposure_reference: str
    #   - exposure_class: str
    #   - approach: Literal["FIRB", "AIRB"]
    #   - pd_raw: Decimal
    #   - pd_floored: Decimal
    #   - lgd_raw: Decimal
    #   - lgd_floored: Decimal
    #   - ead: Decimal
    #   - maturity: Decimal
    #   - correlation: Decimal
    #   - k: Decimal (capital requirement %)
    #   - rwa: Decimal
```

#### Contract 7: Output Floor Contract (Basel 3.1)

```python
# src/rwa_calc/contracts/output_floor_contract.py

from dataclasses import dataclass
import polars as pl


@dataclass
class OutputFloorInput:
    """Input contract for output floor calculation."""
    irb_results: pl.LazyFrame
    sa_equivalent_results: pl.LazyFrame  # SA calc for IRB exposures
    floor_percentage: float              # 72.5% at full implementation
    config: dict


@dataclass
class OutputFloorOutput:
    """Output contract for output floor calculation."""
    floored_results: pl.LazyFrame
    # Schema additions:
    #   - rwa_irb: Decimal
    #   - rwa_sa_equivalent: Decimal
    #   - rwa_floor: Decimal
    #   - rwa_final: Decimal (max of IRB and floor)
    #   - is_floor_binding: bool
    summary: dict  # Total RWA, floor impact, etc.
```

#### Contract 8: Orchestrator Contract

```python
# src/rwa_calc/contracts/orchestrator_contract.py

from dataclasses import dataclass
import polars as pl


@dataclass
class OrchestratorInput:
    """Input contract for the main orchestrator."""
    loader_output: "LoaderOutput"
    config: dict


@dataclass
class OrchestratorOutput:
    """Output contract for the main orchestrator."""
    results: pl.LazyFrame
    # Full schema with all columns from all stages

    summary: dict
    # {
    #   "total_rwa": Decimal,
    #   "total_ead": Decimal,
    #   "rwa_by_approach": {"SA": ..., "FIRB": ..., "AIRB": ...},
    #   "rwa_by_exposure_class": {...},
    #   "output_floor_impact": Decimal,
    #   "calculation_timestamp": datetime,
    # }

    audit_trail: pl.LazyFrame
    # Detailed calculation steps for each exposure
```

### 2.3 Contract Test Structure

```python
# tests/contracts/test_loader_contract.py

import pytest
import polars as pl
from rwa_calc.contracts.loader_contract import LoaderOutput, LoaderProtocol
from rwa_calc.data.schemas import COUNTERPARTY_SCHEMA


class TestLoaderContract:
    """Tests that verify loader implementations meet the contract."""

    def test_output_schemas_match(self, loader: LoaderProtocol):
        """All output DataFrames must match expected schemas."""
        output = loader.load()

        # Verify counterparties schema
        assert output.counterparties.collect_schema() == COUNTERPARTY_SCHEMA

        # ... verify other schemas

    def test_referential_integrity(self, loader: LoaderProtocol):
        """All foreign key references must be valid."""
        output = loader.load()

        # All loans must reference valid counterparties
        loan_cpty_refs = output.loans.select("counterparty_reference").unique()
        cpty_refs = output.counterparties.select("reference").unique()

        invalid_refs = loan_cpty_refs.join(
            cpty_refs,
            left_on="counterparty_reference",
            right_on="reference",
            how="anti"
        )
        assert invalid_refs.collect().height == 0

    def test_no_duplicate_references(self, loader: LoaderProtocol):
        """Primary keys must be unique."""
        output = loader.load()

        cpty_refs = output.counterparties.select("reference").collect()
        assert cpty_refs.height == cpty_refs.unique().height
```

---

## Phase 3: Implementation Order

With acceptance tests and contracts defined, implement components in dependency order.

### 3.1 Foundation Layer

| Step | Component | Dependencies | Contract |
|------|-----------|--------------|----------|
| 3.1.1 | Domain enums | None | N/A |
| 3.1.2 | Contract definitions | Schemas | N/A |
| 3.1.3 | File loader | Schemas | LoaderContract |
| 3.1.4 | In-memory loader (for tests) | Schemas | LoaderContract |

### 3.2 Data Processing Layer

| Step | Component | Dependencies | Contract |
|------|-----------|--------------|----------|
| 3.2.1 | Counterparty hierarchy builder | Loader | HierarchyContract |
| 3.2.2 | Exposure hierarchy builder | Loader | HierarchyContract |
| 3.2.3 | Exposure classifier | Hierarchies | ClassificationContract |

### 3.3 Calculation Layer

| Step | Component | Dependencies | Contract |
|------|-----------|--------------|----------|
| 3.3.1 | CRM processor | Classifier | CRMContract |
| 3.3.2 | SA calculator (Basel 3.0) | CRM | SAContract |
| 3.3.3 | SA calculator (Basel 3.1) | CRM | SAContract |
| 3.3.4 | IRB calculator (F-IRB) | CRM | IRBContract |
| 3.3.5 | IRB calculator (A-IRB) | CRM | IRBContract |
| 3.3.6 | Slotting calculator | Classifier | SAContract |
| 3.3.7 | Output floor | SA + IRB | OutputFloorContract |

### 3.4 Integration Layer

| Step | Component | Dependencies | Contract |
|------|-----------|--------------|----------|
| 3.4.1 | Orchestrator | All calculators | OrchestratorContract |
| 3.4.2 | Results aggregator | Orchestrator | N/A |
| 3.4.3 | Audit trail generator | Orchestrator | N/A |

### 3.5 Reporting Layer

| Step | Component | Dependencies | Contract |
|------|-----------|--------------|----------|
| 3.5.1 | COREP template generator | Results | N/A |
| 3.5.2 | PRA CAP+ generator | Results | N/A |
| 3.5.3 | Marimo workbooks | All | N/A |

---

## Phase 4: Testing Strategy

### 4.1 Test Pyramid

```
                    ┌─────────────────┐
                    │   Acceptance    │  ← Phase 1 scenarios
                    │     Tests       │     (E2E, slow)
                    └────────┬────────┘
                             │
              ┌──────────────┴──────────────┐
              │        Contract Tests       │  ← Phase 2 contracts
              │    (Integration, medium)    │     (Component boundaries)
              └──────────────┬──────────────┘
                             │
    ┌────────────────────────┴────────────────────────┐
    │                   Unit Tests                    │  ← Phase 3 implementation
    │           (Fast, isolated, many)                │     (Individual functions)
    └─────────────────────────────────────────────────┘
```

### 4.2 Test File Structure

```
tests/
├── acceptance/
│   ├── test_scenario_a_sa.py
│   ├── test_scenario_b_firb.py
│   ├── test_scenario_c_airb.py
│   ├── test_scenario_d_crm.py
│   ├── test_scenario_e_slotting.py
│   ├── test_scenario_f_output_floor.py
│   ├── test_scenario_g_provisions.py
│   └── test_scenario_h_complex.py
├── contracts/
│   ├── test_loader_contract.py
│   ├── test_hierarchy_contract.py
│   ├── test_classification_contract.py
│   ├── test_crm_contract.py
│   ├── test_sa_contract.py
│   ├── test_irb_contract.py
│   └── test_output_floor_contract.py
├── unit/
│   ├── test_domain_enums.py
│   ├── test_loaders.py
│   ├── test_hierarchy_counterparty.py
│   ├── test_hierarchy_exposure.py
│   ├── test_classification.py
│   ├── test_crm_haircuts.py
│   ├── test_crm_eligibility.py
│   ├── test_crm_allocation.py
│   ├── test_sa_risk_weights.py
│   ├── test_irb_formulas.py
│   ├── test_irb_correlation.py
│   ├── test_irb_maturity.py
│   ├── test_output_floor.py
│   └── test_provisions.py
├── fixtures/
│   └── ... (test data)
└── conftest.py
```

### 4.3 Continuous Integration

```yaml
# .github/workflows/test.yml
name: Test Suite

on: [push, pull_request]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Install uv
        uses: astral-sh/setup-uv@v4
      - name: Run unit tests
        run: uv run pytest tests/unit -v --tb=short

  contract-tests:
    runs-on: ubuntu-latest
    needs: unit-tests
    steps:
      - uses: actions/checkout@v4
      - name: Install uv
        uses: astral-sh/setup-uv@v4
      - name: Run contract tests
        run: uv run pytest tests/contracts -v --tb=short

  acceptance-tests:
    runs-on: ubuntu-latest
    needs: contract-tests
    steps:
      - uses: actions/checkout@v4
      - name: Install uv
        uses: astral-sh/setup-uv@v4
      - name: Run acceptance tests
        run: uv run pytest tests/acceptance -v --tb=short
```

---

## Phase 5: Documentation

### 5.1 Technical Documentation

- Architecture decision records (ADRs)
- Contract specifications
- Calculation methodology docs
- API reference (auto-generated)

### 5.2 Regulatory Documentation

- Mapping of code to regulatory articles
- Deviation register (UK-specific rules)
- Validation against supervisor examples

---

## Appendix A: Key Regulatory Parameters

Quick reference for test calculations:

### SA Risk Weights

| Exposure Class | CQS 1 | CQS 2 | CQS 3 | CQS 4 | CQS 5 | CQS 6 | Unrated |
|---------------|-------|-------|-------|-------|-------|-------|---------|
| Sovereign | 0% | 20% | 50% | 100% | 100% | 150% | 100% |
| Institution (UK) | 20% | **30%** | 50% | 100% | 100% | 150% | 40% |
| Corporate | 20% | 50% | 75% | 100% | 150% | 150% | 100% |

### IRB PD Floors (Basel 3.1)

| Exposure Class | PD Floor |
|---------------|----------|
| Corporate | 0.03% |
| Retail (non-QRRE) | 0.05% |
| Retail QRRE | 0.10% |

### IRB LGD Values

| Collateral Type | F-IRB LGD | A-IRB Floor |
|-----------------|-----------|-------------|
| Cash | 0% | 0% |
| Financial collateral | 0% | 0% |
| Receivables | 20% | 10% |
| Commercial RE | 20% | 10% |
| Residential RE | 20% | 5% |
| Unsecured | 45% | 25% |
| Subordinated | 75% | 25% |

---

## Appendix B: Test Fixture Reference

Summary of implemented test fixtures with key reference IDs.

### Counterparty References

| Type | Reference | Name | CQS | Notes |
|------|-----------|------|-----|-------|
| Sovereign | SOV_UK_001 | UK Government | 1 | 0% RW |
| Sovereign | SOV_US_001 | US Government | 1 | 0% RW |
| Sovereign | SOV_BR_001 | Brazil | 4 | 100% RW |
| Sovereign | SOV_AR_001 | Argentina | 6 | 150% RW |
| Institution | INST_UK_001 | Barclays Bank | 1 | 20% RW |
| Institution | INST_UK_003 | Metro Bank | 2 | 30% RW (UK deviation) |
| Institution | INST_UR_001 | Regional Bank | - | 40% RW (unrated) |
| Corporate | CORP_UK_001 | BP PLC | 1 | 20% RW |
| Corporate | CORP_UK_003 | Rolls-Royce | 2 | 50% RW |
| Corporate | CORP_UR_001 | Acme Ltd | - | 100% RW (unrated) |
| Corporate | CORP_SME_001 | Tech Startup | - | SME support factor |
| Retail | RTL_IND_001 | John Smith | - | 75% RW |
| Retail | RTL_MTG_001 | Sarah Johnson | - | Mortgage 60% LTV |
| Retail | RTL_MTG_002 | Michael Brown | - | Mortgage 85% LTV |
| Retail | RTL_SME_001 | Corner Shop Ltd | - | SME retail |

### Loan References

| Reference | Counterparty | Amount | Scenario |
|-----------|--------------|--------|----------|
| LOAN_SOV_UK_001 | SOV_UK_001 | £1,000,000 | A1 - Sovereign 0% |
| LOAN_CORP_UR_001 | CORP_UR_001 | £1,000,000 | A2 - Unrated 100% |
| LOAN_CORP_UK_003 | CORP_UK_003 | £1,000,000 | A3 - Rated CQS2 50% |
| LOAN_INST_UK_003 | INST_UK_003 | £1,000,000 | A4 - Institution 30% |
| LOAN_RTL_MTG_001 | RTL_MTG_001 | £500,000 | A5 - Mortgage 60% LTV |
| LOAN_RTL_MTG_002 | RTL_MTG_002 | £850,000 | A6 - Mortgage 85% LTV |
| LOAN_RTL_IND_001 | RTL_IND_001 | £50,000 | A9 - Retail 75% |
| LOAN_RTL_SME_001 | RTL_SME_001 | £500,000 | A10 - SME retail |
| LOAN_HIER_001_A/B/C | CORP_GRP1_PARENT | £4,500,000 | H1 - Multiple loans |
| LOAN_FAC_CORP_002_A | CORP_UK_002 | £30,000,000 | H4 - Full CRM chain |

### Collateral References

| Reference | Type | Value | Haircut | Scenario |
|-----------|------|-------|---------|----------|
| COLL_CASH_001 | Cash | £500,000 | 0% | D1 |
| COLL_GILT_001 | Govt Bond (5yr) | £600,000 | 4% | D2 |
| COLL_EQ_001 | Equity | £400,000 | 25% | D3 |
| COLL_RRE_001 | Residential RE | £833,333 | 60% LTV | A5 |
| COLL_RRE_002 | Residential RE | £1,000,000 | 85% LTV | A6 |
| COLL_MAT_MISMATCH_001 | Bond (2yr) | £500,000 | Maturity adj | D5 |
| COLL_CCY_MISMATCH_001 | Cash (EUR) | €500,000 | 8% FX | D6 |

### Guarantee References

| Reference | Guarantor | Amount | Coverage | Scenario |
|-----------|-----------|--------|----------|----------|
| GUAR_BANK_001 | INST_UK_001 | £600,000 | 60% | D4 |
| GUAR_CRM_CHAIN_001 | INST_UK_002 | £15,000,000 | 50% | H4 |
| GUAR_SOV_001 | SOV_UK_001 | £5,000,000 | 100% | Sovereign sub |

### Hierarchy Test References

| Test | Parent | Children | Purpose |
|------|--------|----------|---------|
| H1 | FAC_HIER_001 | LOAN_HIER_001_A/B/C | Multiple loans under facility |
| H2 | FAC_HIER_PARENT_001 | FAC_HIER_SUB_001/002 | Multi-level facility |
| Org Group 1 | CORP_GRP1_PARENT | CORP_GRP1_SUB1/2/3 | Rating inheritance |
| Lending Group 2 | RTL_LG2_OWNER | RTL_LG2_COMPANY | Threshold aggregation |

---

## Next Steps

### Completed
- [x] Create test fixtures directory structure
- [x] Implement counterparty fixtures (sovereign, institution, corporate, retail, specialised lending)
- [x] Implement exposure fixtures (facilities, loans, contingents, facility mappings)
- [x] Implement CRM fixtures (collateral, guarantees, provisions)
- [x] Implement ratings fixtures (external and internal)
- [x] Implement mapping fixtures (org hierarchy, lending groups)
- [x] Add hierarchy test data for acceptance scenarios H1, H2, H4

### In Progress
- [ ] Implement acceptance test shells (failing tests) for scenarios A1-A10, B1-B6, C1-C3, D1-D6, E1-E4, F1-F3, G1-G3, H1-H4

### Up Next
1. **Phase 1.2**: Create acceptance test file structure and implement test shells
2. **Phase 1.3**: Hand-calculate expected RWA values for each scenario
3. **Phase 2**: Define and implement contract protocols
4. **Phase 3**: Begin implementation following dependency order, making tests pass incrementally

### Fixture Generator Commands

To regenerate all fixtures at once (recommended):
```bash
uv run python tests/fixtures/generate_all.py
```

This master script:
- Generates all 15 parquet files across 7 fixture groups
- Runs data integrity checks (referential integrity, foreign keys)
- Produces a summary report with record counts

To regenerate individual fixture groups:
```bash
# Counterparties
uv run python tests/fixtures/counterparty/generate_all.py

# Exposures (facilities, loans, contingents, mappings)
uv run python tests/fixtures/exposures/generate_all.py

# Collateral
uv run python tests/fixtures/collateral/generate_all.py

# Guarantees
uv run python tests/fixtures/guarantee/generate_all.py

# Provisions
uv run python tests/fixtures/provision/generate_all.py

# Ratings
uv run python tests/fixtures/ratings/generate_all.py

# Mappings (org, lending)
uv run python tests/fixtures/mapping/generate_all.py
```
