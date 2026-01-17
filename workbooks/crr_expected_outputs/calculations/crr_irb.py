"""
CRR IRB formula calculations.

Wraps the shared IRB formulas with CRR specific defaults.
Key difference from Basel 3.1: single PD floor, no A-IRB LGD floors.
"""

from decimal import Decimal
import sys
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import shared IRB formulas (core K calculation is framework-independent)
from workbooks.shared.irb_formulas import (
    calculate_k,
    calculate_maturity_adjustment as _base_calculate_maturity_adjustment,
    calculate_irb_rwa as _base_calculate_irb_rwa,
    calculate_expected_loss,
    apply_pd_floor as _base_apply_pd_floor,
)

# Import CRR specific parameters
from workbooks.crr_expected_outputs.data.crr_params import (
    CRR_PD_FLOOR,
    CRR_FIRB_LGD,
    CRR_MATURITY_FLOOR,
    CRR_MATURITY_CAP,
)


def apply_pd_floor(pd: float, exposure_class: str | None = None) -> float:
    """
    Apply CRR PD floor.

    Args:
        pd: Raw probability of default
        exposure_class: Exposure class (not used in CRR - single floor)

    Returns:
        Floored PD

    CRR: Single 0.03% PD floor for all exposure classes
    (Unlike Basel 3.1 which has differentiated floors)
    """
    return _base_apply_pd_floor(pd, float(CRR_PD_FLOOR))


def get_firb_lgd(
    collateral_type: str = "unsecured",
    is_subordinated: bool = False
) -> Decimal:
    """
    Get F-IRB supervisory LGD (CRR Art. 161).

    Args:
        collateral_type: Type of collateral
        is_subordinated: Whether exposure is subordinated

    Returns:
        Supervisory LGD as Decimal
    """
    if is_subordinated:
        return CRR_FIRB_LGD["subordinated"]

    # Map collateral type to LGD
    lgd_mapping = {
        "unsecured": "unsecured_senior",
        "senior_unsecured": "unsecured_senior",
        "financial_collateral": "financial_collateral",
        "cash": "financial_collateral",
        "receivables": "receivables",
        "residential_re": "residential_re",
        "residential_real_estate": "residential_re",
        "rre": "residential_re",
        "commercial_re": "commercial_re",
        "commercial_real_estate": "commercial_re",
        "cre": "commercial_re",
        "other": "other_physical",
        "other_physical": "other_physical",
    }

    lgd_key = lgd_mapping.get(collateral_type.lower(), "unsecured_senior")
    return CRR_FIRB_LGD.get(lgd_key, CRR_FIRB_LGD["unsecured_senior"])


def calculate_maturity_adjustment(
    pd: float,
    maturity: float,
    apply_adjustment: bool = True,
) -> float:
    """
    Calculate maturity adjustment factor for corporate/wholesale exposures.

    Uses CRR maturity floor (1 year) and cap (5 years).

    Args:
        pd: Probability of default (floored)
        maturity: Effective maturity in years
        apply_adjustment: Whether to apply adjustment (False for retail)

    Returns:
        Maturity adjustment factor

    Reference: CRR Art. 162
    """
    return _base_calculate_maturity_adjustment(
        pd,
        maturity,
        apply_adjustment=apply_adjustment,
        maturity_floor=float(CRR_MATURITY_FLOOR),
        maturity_cap=float(CRR_MATURITY_CAP),
    )


def calculate_irb_rwa(
    ead: float,
    pd: float,
    lgd: float,
    correlation: float,
    maturity: float = 2.5,
    exposure_class: str = "CORPORATE",
    apply_maturity_adjustment: bool = True,
    apply_pd_floor_flag: bool = True,
) -> dict:
    """
    Calculate RWA using CRR IRB approach.

    Args:
        ead: Exposure at Default
        pd: Probability of default (raw)
        lgd: Loss given default
        correlation: Asset correlation
        maturity: Effective maturity in years
        exposure_class: Exposure class
        apply_maturity_adjustment: Whether to apply MA (False for retail)
        apply_pd_floor_flag: Whether to apply PD floor

    Returns:
        Dictionary with calculation details

    Key CRR differences from Basel 3.1:
    - Single PD floor (0.03%) for all classes
    - No A-IRB LGD floors

    Reference: CRR Art. 153-154
    """
    is_retail = "RETAIL" in exposure_class.upper()

    return _base_calculate_irb_rwa(
        ead=ead,
        pd=pd,
        lgd=lgd,
        correlation=correlation,
        maturity=maturity,
        pd_floor=float(CRR_PD_FLOOR) if apply_pd_floor_flag else 0.0,
        lgd_floor=None,  # CRR A-IRB has no LGD floors
        apply_maturity_adjustment=apply_maturity_adjustment,
        is_retail=is_retail,
    )


# Re-export for compatibility
__all__ = [
    "calculate_k",
    "calculate_maturity_adjustment",
    "calculate_irb_rwa",
    "calculate_expected_loss",
    "apply_pd_floor",
    "get_firb_lgd",
]
