"""
IRB (Internal Ratings-Based) formula calculations.

Implements the capital requirement (K) formula and RWA calculations
for F-IRB and A-IRB approaches per CRE31-32.
"""

import math
from typing import Literal
import sys
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from workbooks.rwa_expected_outputs.data.regulatory_params import (
    PD_FLOORS,
    AIRB_LGD_FLOORS,
    MATURITY_FLOOR,
    MATURITY_CAP,
)


def _norm_cdf(x: float) -> float:
    """
    Cumulative distribution function of standard normal distribution.

    Uses error function approximation.
    """
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def _norm_ppf(p: float) -> float:
    """
    Inverse of standard normal CDF (percent point function).

    Uses rational approximation.
    """
    if p <= 0:
        return float('-inf')
    if p >= 1:
        return float('inf')

    # Coefficients for rational approximation
    a = [
        -3.969683028665376e+01,
        2.209460984245205e+02,
        -2.759285104469687e+02,
        1.383577518672690e+02,
        -3.066479806614716e+01,
        2.506628277459239e+00,
    ]
    b = [
        -5.447609879822406e+01,
        1.615858368580409e+02,
        -1.556989798598866e+02,
        6.680131188771972e+01,
        -1.328068155288572e+01,
    ]
    c = [
        -7.784894002430293e-03,
        -3.223964580411365e-01,
        -2.400758277161838e+00,
        -2.549732539343734e+00,
        4.374664141464968e+00,
        2.938163982698783e+00,
    ]
    d = [
        7.784695709041462e-03,
        3.224671290700398e-01,
        2.445134137142996e+00,
        3.754408661907416e+00,
    ]

    p_low = 0.02425
    p_high = 1 - p_low

    if p < p_low:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0]*q + c[1])*q + c[2])*q + c[3])*q + c[4])*q + c[5]) / \
               ((((d[0]*q + d[1])*q + d[2])*q + d[3])*q + 1)
    elif p <= p_high:
        q = p - 0.5
        r = q * q
        return (((((a[0]*r + a[1])*r + a[2])*r + a[3])*r + a[4])*r + a[5])*q / \
               (((((b[0]*r + b[1])*r + b[2])*r + b[3])*r + b[4])*r + 1)
    else:
        q = math.sqrt(-2 * math.log(1 - p))
        return -(((((c[0]*q + c[1])*q + c[2])*q + c[3])*q + c[4])*q + c[5]) / \
                ((((d[0]*q + d[1])*q + d[2])*q + d[3])*q + 1)


def apply_pd_floor(pd: float, exposure_class: str) -> float:
    """
    Apply PD floor based on exposure class.

    Args:
        pd: Raw probability of default
        exposure_class: Exposure class for floor lookup

    Returns:
        Floored PD

    Reference: CRE31.6 (Basel 3.1 PD floors)
    """
    floor = PD_FLOORS.get(exposure_class, PD_FLOORS.get("CORPORATE", 0.0003))
    return max(pd, floor)


def apply_lgd_floor(
    lgd: float,
    collateral_type: str = "unsecured",
    exposure_class: str = "CORPORATE",
) -> float:
    """
    Apply LGD floor for A-IRB approach.

    Args:
        lgd: Raw LGD estimate
        collateral_type: Type of collateral securing the exposure
        exposure_class: Exposure class

    Returns:
        Floored LGD

    Reference: CRE32.20 (Basel 3.1 LGD floors)
    """
    # Map exposure class to floor key
    if "RETAIL" in exposure_class:
        if "MORTGAGE" in exposure_class:
            floor_key = "retail_mortgage"
        elif "QRRE" in exposure_class:
            floor_key = "retail_qrre"
        else:
            floor_key = "retail_unsecured"
    else:
        floor_key = collateral_type

    floor = AIRB_LGD_FLOORS.get(floor_key, AIRB_LGD_FLOORS.get("unsecured", 0.25))
    return max(lgd, floor)


def calculate_maturity_adjustment(
    pd: float,
    maturity: float,
    apply_adjustment: bool = True,
) -> float:
    """
    Calculate maturity adjustment factor for corporate/wholesale exposures.

    Args:
        pd: Probability of default (floored)
        maturity: Effective maturity in years
        apply_adjustment: Whether to apply adjustment (False for retail)

    Returns:
        Maturity adjustment factor

    Formula (CRE31.7):
        b = (0.11852 - 0.05478 * ln(PD))^2
        MA = (1 + (M - 2.5) * b) / (1 - 1.5 * b)

    Reference: CRE31.7
    """
    if not apply_adjustment:
        return 1.0

    # Apply maturity floor and cap
    m = max(MATURITY_FLOOR, min(MATURITY_CAP, maturity))

    # Avoid log of zero
    pd_safe = max(pd, 0.00001)

    # Calculate b coefficient
    b = (0.11852 - 0.05478 * math.log(pd_safe)) ** 2

    # Calculate maturity adjustment
    ma = (1 + (m - 2.5) * b) / (1 - 1.5 * b)

    return ma


def calculate_k(
    pd: float,
    lgd: float,
    correlation: float,
) -> float:
    """
    Calculate capital requirement (K) using IRB formula.

    Args:
        pd: Probability of default
        lgd: Loss given default
        correlation: Asset correlation (R)

    Returns:
        Capital requirement as decimal

    Formula (CRE31.4):
        K = LGD x N[(1-R)^(-0.5) x G(PD) + (R/(1-R))^(0.5) x G(0.999)] - PD x LGD

    Where:
        N() = cumulative normal distribution
        G() = inverse cumulative normal distribution
        R = asset correlation
    """
    if pd >= 1.0:
        # Defaulted exposure
        return lgd

    if pd <= 0:
        return 0.0

    # G(PD) - inverse normal of PD
    g_pd = _norm_ppf(pd)

    # G(0.999) - inverse normal of 99.9% confidence level
    g_999 = _norm_ppf(0.999)

    # Calculate the argument for the normal CDF
    # (1-R)^(-0.5) x G(PD) + (R/(1-R))^(0.5) x G(0.999)
    term1 = math.sqrt(1 / (1 - correlation)) * g_pd
    term2 = math.sqrt(correlation / (1 - correlation)) * g_999

    # N[...] - conditional PD at 99.9% confidence
    conditional_pd = _norm_cdf(term1 + term2)

    # K = LGD x N[...] - PD x LGD
    k = lgd * conditional_pd - pd * lgd

    return max(k, 0.0)


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
    Calculate RWA using IRB approach.

    Args:
        ead: Exposure at Default
        pd: Probability of default (raw)
        lgd: Loss given default
        correlation: Asset correlation
        maturity: Effective maturity in years
        exposure_class: Exposure class for floors
        apply_maturity_adjustment: Whether to apply MA (False for retail)
        apply_pd_floor_flag: Whether to apply PD floor

    Returns:
        Dictionary with calculation details:
        - pd_raw: Original PD
        - pd_floored: PD after floor
        - lgd: LGD used
        - correlation: Asset correlation
        - k: Capital requirement
        - maturity_adjustment: MA factor
        - rwa: Risk-weighted assets

    Formula: RWA = K x 12.5 x EAD x MA

    Reference: CRE31.4, CRE31.7
    """
    # Apply PD floor
    pd_floored = apply_pd_floor(pd, exposure_class) if apply_pd_floor_flag else pd

    # Calculate capital requirement
    k = calculate_k(pd_floored, lgd, correlation)

    # Calculate maturity adjustment
    is_retail = "RETAIL" in exposure_class
    ma = calculate_maturity_adjustment(
        pd_floored,
        maturity,
        apply_adjustment=apply_maturity_adjustment and not is_retail,
    )

    # Calculate RWA: K x 12.5 x EAD x MA
    rwa = k * 12.5 * ead * ma

    return {
        "pd_raw": pd,
        "pd_floored": pd_floored,
        "lgd": lgd,
        "correlation": correlation,
        "k": k,
        "maturity_adjustment": ma,
        "rwa": rwa,
        "ead": ead,
    }


def calculate_expected_loss(pd: float, lgd: float, ead: float) -> float:
    """
    Calculate expected loss for IRB comparison with provisions.

    Args:
        pd: Probability of default
        lgd: Loss given default
        ead: Exposure at default

    Returns:
        Expected loss amount

    Formula: EL = PD x LGD x EAD

    Reference: CRE35.1
    """
    return pd * lgd * ead
