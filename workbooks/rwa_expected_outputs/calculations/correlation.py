"""
Asset correlation calculations for IRB approach.

Implements correlation formulas per CRE31.5 for different exposure classes.
"""

import math
import sys
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from workbooks.rwa_expected_outputs.data.regulatory_params import CORRELATION_PARAMS


def _pd_weight_factor(pd: float, decay_factor: float) -> float:
    """
    Calculate the PD-dependent weight factor.

    Formula: f(PD) = (1 - e^(-decay_factor * PD)) / (1 - e^(-decay_factor))

    Args:
        pd: Probability of default
        decay_factor: K parameter (50 for corporate, 35 for retail)

    Returns:
        Weight factor between 0 and 1
    """
    if decay_factor <= 0:
        return 0.5  # Default for fixed correlation

    numerator = 1 - math.exp(-decay_factor * pd)
    denominator = 1 - math.exp(-decay_factor)

    return numerator / denominator


def calculate_correlation(
    pd: float,
    exposure_class: str,
    turnover: float | None = None,
) -> float:
    """
    Calculate asset correlation for IRB formula.

    Args:
        pd: Probability of default
        exposure_class: Exposure class (CORPORATE, RETAIL, etc.)
        turnover: Annual turnover in millions (for SME adjustment)

    Returns:
        Asset correlation (R)

    Formulas (CRE31.5):
        Corporate: R = 0.12 * f(PD) + 0.24 * (1 - f(PD))
            where f(PD) = (1 - e^(-50*PD)) / (1 - e^(-50))

        Retail (other): R = 0.03 * f(PD) + 0.16 * (1 - f(PD))
            where f(PD) = (1 - e^(-35*PD)) / (1 - e^(-35))

        Retail mortgage: R = 0.15 (fixed)
        QRRE: R = 0.04 (fixed)

    For SME corporates (turnover < EUR 50m), apply firm size adjustment:
        R_adjusted = R - 0.04 * (1 - (max(S, 5) - 5) / 45)
        where S = annual turnover in EUR millions, capped at 50
    """
    # Get correlation parameters for exposure class
    params = CORRELATION_PARAMS.get(
        exposure_class,
        CORRELATION_PARAMS.get("CORPORATE")  # Default to corporate
    )

    if params is None:
        # Fallback defaults
        return 0.15

    # Fixed correlation
    if params["type"] == "fixed":
        return params["fixed"]

    # PD-dependent correlation
    r_min = params["r_min"]
    r_max = params["r_max"]
    decay = params["decay_factor"]

    # Calculate weight factor
    f_pd = _pd_weight_factor(pd, decay)

    # R = r_min * f(PD) + r_max * (1 - f(PD))
    # Note: r_min is the correlation at high PD, r_max at low PD
    # Formula is: R = r_max - (r_max - r_min) * f(PD)
    correlation = r_min * f_pd + r_max * (1 - f_pd)

    # SME firm size adjustment (CRE31.5)
    if exposure_class in ["CORPORATE", "CORPORATE_SME"] and turnover is not None:
        correlation = _apply_sme_adjustment(correlation, turnover)

    return correlation


def _apply_sme_adjustment(correlation: float, turnover: float) -> float:
    """
    Apply SME firm size adjustment to corporate correlation.

    Args:
        correlation: Base correlation
        turnover: Annual turnover in millions (EUR/GBP)

    Returns:
        Adjusted correlation

    Formula (CRE31.5):
        R_adjusted = R - 0.04 * (1 - (max(S, 5) - 5) / 45)
        where S = annual turnover capped at 50

    The adjustment reduces correlation for smaller firms (turnover < EUR 50m)
    """
    # EUR 50m threshold (approximately £44m at typical FX)
    SME_THRESHOLD = 50.0
    SME_FLOOR = 5.0
    SME_RANGE = 45.0  # 50 - 5

    if turnover >= SME_THRESHOLD:
        return correlation  # No adjustment for large corporates

    # Cap at threshold, floor at 5
    s = max(SME_FLOOR, min(turnover, SME_THRESHOLD))

    # Calculate adjustment
    # Adjustment = 0.04 * (1 - (S - 5) / 45)
    # At S = 5: adjustment = 0.04 * 1 = 0.04
    # At S = 50: adjustment = 0.04 * 0 = 0
    adjustment = 0.04 * (1 - (s - SME_FLOOR) / SME_RANGE)

    return correlation - adjustment


def get_correlation_for_class(exposure_class: str) -> dict:
    """
    Get correlation parameters for an exposure class.

    Args:
        exposure_class: The exposure class

    Returns:
        Dictionary with correlation parameters
    """
    return CORRELATION_PARAMS.get(
        exposure_class,
        CORRELATION_PARAMS.get("CORPORATE", {
            "type": "pd_dependent",
            "r_min": 0.12,
            "r_max": 0.24,
            "fixed": 0.0,
            "decay_factor": 50.0,
        })
    )
