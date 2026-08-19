"""Basic import tests for bedi_lakehouse."""

import bedi_lakehouse


def test_package_import():
    """Test that the package can be imported."""
    assert bedi_lakehouse is not None


def test_package_metadata():
    """Test that package metadata is correctly set."""
    assert bedi_lakehouse.__version__ == "1.0.0"
    assert isinstance(bedi_lakehouse.__author__, str)
    assert len(bedi_lakehouse.__author__) > 0
    assert "@" in bedi_lakehouse.__email__  # Basic email validation
