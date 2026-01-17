"""
CRR Supporting Factors (CRR Art. 501).

Implements SME and infrastructure supporting factors unique to CRR.
These factors are NOT available under Basel 3.1.
"""

from decimal import Decimal
import sys
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from workbooks.crr_expected_outputs.data.crr_params import (
    CRR_SME_SUPPORTING_FACTOR,
    CRR_INFRASTRUCTURE_SUPPORTING_FACTOR,
    CRR_SME_TURNOVER_THRESHOLD_EUR,
    CRR_SME_TURNOVER_THRESHOLD_GBP,
)


def is_sme_eligible(
    turnover: Decimal | None,
    currency: str = "GBP"
) -> bool:
    """
    Check if counterparty qualifies as SME for supporting factor.

    Args:
        turnover: Annual turnover
        currency: Currency of turnover figure

    Returns:
        True if SME eligible (turnover < threshold)

    Threshold:
    - EUR 50m (standard)
    - GBP 44m (UK approximation)
    """
    if turnover is None:
        return False

    if currency.upper() in ("GBP", "£"):
        return turnover < CRR_SME_TURNOVER_THRESHOLD_GBP
    else:
        return turnover < CRR_SME_TURNOVER_THRESHOLD_EUR


def apply_sme_supporting_factor(
    rwa: Decimal,
    is_sme: bool,
    turnover: Decimal | None = None,
    currency: str = "GBP"
) -> tuple[Decimal, bool, str]:
    """
    Apply SME supporting factor (CRR Art. 501).

    Args:
        rwa: RWA before factor
        is_sme: Whether counterparty is known to be SME
        turnover: Annual turnover (for eligibility check)
        currency: Currency of turnover

    Returns:
        Tuple of (adjusted_rwa, factor_applied, description)

    Factor: 0.7619 (reduces RWA by ~24%)

    Note: This factor is NOT available under Basel 3.1
    """
    # Check eligibility
    eligible = is_sme or (turnover is not None and is_sme_eligible(turnover, currency))

    if not eligible:
        return rwa, False, "SME factor not applied (not eligible)"

    adjusted_rwa = rwa * CRR_SME_SUPPORTING_FACTOR
    reduction = rwa - adjusted_rwa

    return (
        adjusted_rwa,
        True,
        f"SME factor applied: {CRR_SME_SUPPORTING_FACTOR} (RWA reduced by {reduction:,.0f})"
    )


def apply_infrastructure_supporting_factor(
    rwa: Decimal,
    is_qualifying_infrastructure: bool
) -> tuple[Decimal, bool, str]:
    """
    Apply infrastructure supporting factor (CRR Art. 501a).

    Args:
        rwa: RWA before factor
        is_qualifying_infrastructure: Whether exposure qualifies

    Returns:
        Tuple of (adjusted_rwa, factor_applied, description)

    Factor: 0.75 (reduces RWA by 25%)

    Note: This factor is NOT available under Basel 3.1

    Qualifying criteria include:
    - Exposure to entity operating infrastructure project
    - Cash flows generated can cover financial obligations
    - Project finance structure with appropriate covenants
    """
    if not is_qualifying_infrastructure:
        return rwa, False, "Infrastructure factor not applied (not qualifying)"

    adjusted_rwa = rwa * CRR_INFRASTRUCTURE_SUPPORTING_FACTOR
    reduction = rwa - adjusted_rwa

    return (
        adjusted_rwa,
        True,
        f"Infrastructure factor applied: {CRR_INFRASTRUCTURE_SUPPORTING_FACTOR} (RWA reduced by {reduction:,.0f})"
    )


def get_combined_supporting_factor(
    is_sme: bool,
    is_infrastructure: bool,
    turnover: Decimal | None = None,
    currency: str = "GBP"
) -> Decimal:
    """
    Get combined supporting factor (for cases where both might apply).

    Note: In practice, exposures typically qualify for only one factor.
    If both apply, the lower factor (infrastructure 0.75) takes precedence
    as it provides greater relief.

    Args:
        is_sme: Whether SME eligible
        is_infrastructure: Whether infrastructure eligible
        turnover: Annual turnover
        currency: Currency

    Returns:
        Applicable supporting factor
    """
    sme_eligible = is_sme or (turnover is not None and is_sme_eligible(turnover, currency))

    if is_infrastructure:
        return CRR_INFRASTRUCTURE_SUPPORTING_FACTOR
    elif sme_eligible:
        return CRR_SME_SUPPORTING_FACTOR
    else:
        return Decimal("1.0")
