"""
IRB (Internal Ratings-Based) formula calculations.

Implements the capital requirement (K) formula and RWA calculations
for F-IRB and A-IRB approaches. The core IRB formula (Basel II/III) is
the same for both CRR and Basel 3.1 - only the floors differ.

This is a shared module - floors should be passed as parameters.

References:
- CRR Art. 153-154: IRB risk weight functions
- CRE31: Basel 3.1 IRB approach
"""

import math


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


def apply_pd_floor(pd: float, pd_floor: float = 0.0003) -> float:
    """
    Apply PD floor.

    Args:
        pd: Raw probability of default
        pd_floor: PD floor to apply (default 0.03% for CRR/Basel 3.1 corporate)

    Returns:
        Floored PD

    Note: PD floors differ between CRR and Basel 3.1 for retail classes.
    - CRR: 0.03% for all classes
    - Basel 3.1: 0.03% corporate, 0.05% retail, 0.10% QRRE
    """
    return max(pd, pd_floor)


def apply_lgd_floor(lgd: float, lgd_floor: float | None = None) -> float:
    """
    Apply LGD floor for A-IRB approach.

    Args:
        lgd: Raw LGD estimate
        lgd_floor: LGD floor to apply (None = no floor, as in CRR A-IRB)

    Returns:
        Floored LGD

    Note: LGD floors only apply under Basel 3.1 A-IRB, not CRR.
    """
    if lgd_floor is None:
        return lgd
    return max(lgd, lgd_floor)


def calculate_maturity_adjustment(
    pd: float,
    maturity: float,
    apply_adjustment: bool = True,
    maturity_floor: float = 1.0,
    maturity_cap: float = 5.0,
) -> float:
    """
    Calculate maturity adjustment factor for corporate/wholesale exposures.

    Args:
        pd: Probability of default (floored)
        maturity: Effective maturity in years
        apply_adjustment: Whether to apply adjustment (False for retail)
        maturity_floor: Minimum maturity (default 1 year)
        maturity_cap: Maximum maturity (default 5 years)

    Returns:
        Maturity adjustment factor

    Formula (CRR Art. 153 / CRE31.7):
        b = (0.11852 - 0.05478 * ln(PD))^2
        MA = (1 + (M - 2.5) * b) / (1 - 1.5 * b)
    """
    if not apply_adjustment:
        return 1.0

    # Apply maturity floor and cap
    m = max(maturity_floor, min(maturity_cap, maturity))

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

    Formula (CRR Art. 153 / CRE31.4):
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
    pd_floor: float = 0.0003,
    lgd_floor: float | None = None,
    apply_maturity_adjustment: bool = True,
    is_retail: bool = False,
    apply_scaling_factor: bool = False,
) -> dict:
    """
    Calculate RWA using IRB approach.

    Args:
        ead: Exposure at Default
        pd: Probability of default (raw)
        lgd: Loss given default
        correlation: Asset correlation
        maturity: Effective maturity in years
        pd_floor: PD floor to apply
        lgd_floor: LGD floor to apply (None = no floor)
        apply_maturity_adjustment: Whether to apply MA
        is_retail: Whether this is a retail exposure (no MA, no scaling)
        apply_scaling_factor: Whether to apply 1.06 scaling factor (CRR only)

    Returns:
        Dictionary with calculation details:
        - pd_raw: Original PD
        - pd_floored: PD after floor
        - lgd_raw: Original LGD
        - lgd_floored: LGD after floor
        - correlation: Asset correlation
        - k: Capital requirement
        - maturity_adjustment: MA factor
        - scaling_factor: 1.06 if applied, 1.0 otherwise
        - rwa: Risk-weighted assets

    Formula:
        CRR:       RWA = K × 12.5 × 1.06 × EAD × MA (1.06 for all classes)
        Basel 3.1: RWA = K × 12.5 × EAD × MA (no 1.06 scaling)

    Note: The 1.06 scaling factor was introduced in Basel II/CRR to account
    for model uncertainty. It applies to all exposure classes under CRR.
    It was removed in Basel 3.1. Pass apply_scaling_factor=True for CRR.

    Reference: CRR Art. 153
    """
    # Apply PD floor
    pd_floored = apply_pd_floor(pd, pd_floor)

    # Apply LGD floor (Basel 3.1 A-IRB only)
    lgd_floored = apply_lgd_floor(lgd, lgd_floor)

    # Calculate capital requirement
    k = calculate_k(pd_floored, lgd_floored, correlation)

    # Calculate maturity adjustment (not for retail)
    ma = calculate_maturity_adjustment(
        pd_floored,
        maturity,
        apply_adjustment=apply_maturity_adjustment and not is_retail,
    )

    # 1.06 scaling factor: CRR only (applies to all exposure classes)
    # This factor was removed in Basel 3.1
    scaling_factor = 1.06 if apply_scaling_factor else 1.0

    # Calculate RWA: K × 12.5 × scaling_factor × EAD × MA
    rwa = k * 12.5 * scaling_factor * ead * ma

    return {
        "pd_raw": pd,
        "pd_floored": pd_floored,
        "lgd_raw": lgd,
        "lgd_floored": lgd_floored,
        "correlation": correlation,
        "k": k,
        "maturity_adjustment": ma,
        "scaling_factor": scaling_factor,
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

    Reference: CRR Art. 158 / CRE35.1
    """
    return pd * lgd * ead


def calculate_risk_weight_from_k(
    k: float,
    ma: float = 1.0,
    apply_scaling_factor: bool = False,
) -> float:
    """
    Convert capital K to equivalent risk weight.

    Args:
        k: Capital requirement
        ma: Maturity adjustment
        apply_scaling_factor: Whether to apply 1.06 scaling (CRR only)

    Returns:
        Equivalent risk weight (for comparison with SA)

    Formula:
        CRR:       RW = K × 12.5 × 1.06 × MA
        Basel 3.1: RW = K × 12.5 × MA
    """
    scaling_factor = 1.06 if apply_scaling_factor else 1.0
    return k * 12.5 * scaling_factor * ma
