"""Basic import tests for aas_doors_lakehouse."""

import aas_doors_lakehouse


def test_package_import():
    """Test that the package can be imported."""
    assert aas_doors_lakehouse is not None


def test_package_metadata():
    """Test that package metadata is correctly set."""
    assert aas_doors_lakehouse.__version__ == "1.0.0"
    assert isinstance(aas_doors_lakehouse.__author__, str)
    assert len(aas_doors_lakehouse.__author__) > 0
    assert "@" in aas_doors_lakehouse.__email__  # Basic email validation
