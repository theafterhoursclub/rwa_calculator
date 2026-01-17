"""
Fixture loader - redirects to shared module.

This module is kept for backwards compatibility but delegates to
the shared fixture loader in workbooks/shared/fixture_loader.py.
"""

# Re-export from shared module
from workbooks.shared.fixture_loader import (
    FixtureData,
    load_fixtures,
    load_fixtures_eager,
    get_fixture_path,
)

__all__ = [
    "FixtureData",
    "load_fixtures",
    "load_fixtures_eager",
    "get_fixture_path",
]
