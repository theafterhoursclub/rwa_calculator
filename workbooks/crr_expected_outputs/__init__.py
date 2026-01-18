"""CRR (Basel 3.0) RWA Expected Outputs Workbook System.

This package provides Marimo workbooks for generating expected RWA values
for CRR (EU 575/2013 as onshored into UK law) acceptance test scenarios.

This is the current UK regulatory framework, effective until 31 Dec 2026.
For Basel 3.1 expected outputs (effective 1 Jan 2027), see
workbooks/basel31_expected_outputs/.

Key CRR-specific features:
- SME supporting factor (0.7619) applies
- Infrastructure supporting factor (0.75) applies
- No output floor
- No A-IRB LGD floors
- 35%/50% residential mortgage treatment (not granular LTV bands)

Usage:
    # Interactive editing
    uv run marimo edit workbooks/crr_expected_outputs/main.py

    # Generate outputs
    uv run marimo run workbooks/crr_expected_outputs/main.py
"""
