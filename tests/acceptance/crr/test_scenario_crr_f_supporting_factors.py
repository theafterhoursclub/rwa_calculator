"""
CRR Group F: Supporting Factors Acceptance Tests.

These tests verify correct implementation of CRR-specific supporting factors
that are NOT available under Basel 3.1.

Regulatory References:
- CRR Art. 501: SME supporting factor (0.7619)
- CRR Art. 501a: Infrastructure supporting factor (0.75)
"""

import pytest
from decimal import Decimal


class TestCRRGroupF_SupportingFactors:
    """CRR Supporting Factors acceptance tests."""

    @pytest.fixture
    def crr_config(self):
        return {
            "regulatory_framework": "CRR",
            "reporting_date": "2025-12-31",
            "apply_sme_supporting_factor": True,
            "apply_infrastructure_factor": True,
        }

    # =========================================================================
    # CRR-F1: SME Corporate with Supporting Factor
    # =========================================================================

    def test_crr_f1_sme_corporate_factor(self, crr_config):
        """
        CRR-F1: SME corporate (turnover < EUR 50m) gets 0.7619 factor.

        Hand calculation:
        - EAD: £5,000,000
        - Turnover: £30,000,000 (< £44m threshold)
        - Risk Weight: 100%
        - RWA before factor: £5,000,000
        - SME factor: 0.7619
        - RWA after factor: £5,000,000 × 0.7619 = £3,809,500

        Note: NOT available under Basel 3.1.

        Regulatory Reference: CRR Art. 501
        """
        expected_ead = Decimal("5000000")
        expected_turnover = Decimal("30000000")
        expected_rw = Decimal("1.00")
        expected_sme_factor = Decimal("0.7619")
        expected_rwa_before = Decimal("5000000")
        expected_rwa_after = Decimal("3809500")

        pytest.skip("Implementation not yet complete")

    # =========================================================================
    # CRR-F2: SME Retail with Supporting Factor
    # =========================================================================

    def test_crr_f2_sme_retail_factor(self, crr_config):
        """
        CRR-F2: SME retail gets both retail RW and SME factor.

        Hand calculation:
        - EAD: £500,000
        - Risk Weight: 75% (retail)
        - RWA before factor: £375,000
        - SME factor: 0.7619
        - RWA after factor: £375,000 × 0.7619 = £285,712.50
        - Effective RW: 75% × 0.7619 = 57.14%

        Regulatory Reference: CRR Art. 123 + Art. 501
        """
        expected_ead = Decimal("500000")
        expected_rw = Decimal("0.75")
        expected_sme_factor = Decimal("0.7619")
        expected_rwa_after = Decimal("285712.50")

        pytest.skip("Implementation not yet complete")

    # =========================================================================
    # CRR-F3: Infrastructure Supporting Factor
    # =========================================================================

    def test_crr_f3_infrastructure_factor(self, crr_config):
        """
        CRR-F3: Qualifying infrastructure gets 0.75 factor.

        Hand calculation:
        - EAD: £100,000,000 (infrastructure project)
        - Risk Weight: 100%
        - RWA before factor: £100,000,000
        - Infrastructure factor: 0.75
        - RWA after factor: £100,000,000 × 0.75 = £75,000,000

        Note: NOT available under Basel 3.1.

        Regulatory Reference: CRR Art. 501a
        """
        expected_ead = Decimal("100000000")
        expected_rw = Decimal("1.00")
        expected_infra_factor = Decimal("0.75")
        expected_rwa_after = Decimal("75000000")

        pytest.skip("Implementation not yet complete")

    # =========================================================================
    # CRR-F4: Large Corporate - No Factor
    # =========================================================================

    def test_crr_f4_large_corporate_no_factor(self, crr_config):
        """
        CRR-F4: Large corporate (turnover >= EUR 50m) gets no SME factor.

        Hand calculation:
        - EAD: £20,000,000
        - Turnover: £200,000,000 (> £44m threshold)
        - Risk Weight: 100%
        - SME factor: Not applicable
        - RWA: £20,000,000

        Regulatory Reference: CRR Art. 501
        """
        expected_ead = Decimal("20000000")
        expected_turnover = Decimal("200000000")
        expected_sme_factor = Decimal("1.00")  # No factor
        expected_rwa = Decimal("20000000")

        pytest.skip("Implementation not yet complete")


class TestCRRGroupF_EdgeCases:
    """Edge case tests for CRR supporting factors."""

    def test_crr_f_turnover_at_threshold(self):
        """
        Edge case: Turnover exactly at £44m threshold.

        Threshold is < EUR 50m, so exactly at threshold does NOT qualify.
        """
        turnover = Decimal("44000000")
        expected_factor_applies = False
        pytest.skip("Implementation not yet complete")

    def test_crr_f_turnover_just_below_threshold(self):
        """
        Edge case: Turnover at £43,999,999 qualifies for SME factor.
        """
        turnover = Decimal("43999999")
        expected_factor_applies = True
        pytest.skip("Implementation not yet complete")

    def test_crr_f_combined_factors_not_allowed(self):
        """
        Edge case: SME and infrastructure factors cannot be combined.
        Infrastructure takes precedence as it provides greater relief.
        """
        # If both apply, use infrastructure (0.75) not SME (0.7619)
        expected_factor = Decimal("0.75")
        pytest.skip("Implementation not yet complete")

    def test_crr_f_irb_with_sme_factor(self):
        """
        Edge case: IRB exposure can also receive SME supporting factor.
        """
        pytest.skip("Implementation not yet complete")
