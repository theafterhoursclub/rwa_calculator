"""IRB (Internal Ratings-Based) approach calculation components.

Provides:
- IRBCalculator: Main IRB calculator implementing IRBCalculatorProtocol
- IRB formulas: K formula, correlation, maturity adjustment

Supports both F-IRB (supervisory LGD) and A-IRB (own LGD estimates).

References:
- CRR Art. 153-154: IRB risk weight functions
- CRR Art. 161: F-IRB supervisory LGD
- CRR Art. 162-163: Maturity and PD floors
"""

from rwa_calc.engine.irb.calculator import IRBCalculator, create_irb_calculator
from rwa_calc.engine.irb.formulas import (
    calculate_correlation,
    calculate_k,
    calculate_maturity_adjustment,
    calculate_irb_rwa,
    calculate_expected_loss,
    apply_irb_formulas,
)

__all__ = [
    "IRBCalculator",
    "create_irb_calculator",
    "calculate_correlation",
    "calculate_k",
    "calculate_maturity_adjustment",
    "calculate_irb_rwa",
    "calculate_expected_loss",
    "apply_irb_formulas",
]
