"""Unit tests for GovMove postcode normalisation and distance calculations.

Run locally:      pytest -q govmove/tests
Run in Databricks: see the 'Unit tests' cell of the GovMove data-preparation notebook.
Spark-based tests are skipped automatically when no Spark session is available.
"""
import math
import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from govmove_core import (  # noqa: E402
    calculate_straight_line_distance, get_postcode_coordinates, is_valid_postcode_format,
    metres_to_km, metres_to_miles, normalise_postcode, straight_line_distance_m,
)


# ----------------------------------------------------------------- postcode normalisation
@pytest.mark.parametrize("raw, expected", [
    ("sw1a2aa", "SW1A 2AA"),
    ("  SW1A   2AA ", "SW1A 2AA"),
    ("sw1a 2aa", "SW1A 2AA"),
    ("m11ae", "M1 1AE"),
    ("B338TH", "B33 8TH"),
    ("cr2 6xh", "CR2 6XH"),
    ("bt1 5gs", "BT1 5GS"),
    ("EC1A1BB", "EC1A 1BB"),
    ("\tdn55 1pt\n", "DN55 1PT"),
])
def test_normalise_postcode_valid(raw, expected):
    assert normalise_postcode(raw) == expected
    assert is_valid_postcode_format(normalise_postcode(raw))


@pytest.mark.parametrize("raw", [None, "", "   "])
def test_normalise_blank(raw):
    assert normalise_postcode(raw) == ""
    assert not is_valid_postcode_format(normalise_postcode(raw))


@pytest.mark.parametrize("raw", ["ABC", "12345", "SW1A2AAX1", "SW1A-2AA", "1AA 2BB", "SW1A 2A1"])
def test_invalid_postcode_format(raw):
    assert not is_valid_postcode_format(normalise_postcode(raw))


def test_normalise_too_short_or_long_returns_compact():
    assert normalise_postcode("ab c") == "ABC"
    assert normalise_postcode("sw1a2aax1") == "SW1A2AAX1"


def test_lookup_validation_without_spark():
    assert get_postcode_coordinates("   ")["lookup_status"] == "blank"
    assert get_postcode_coordinates("NOTAPOSTCODE")["lookup_status"] == "invalid_format"
    res = get_postcode_coordinates("sw1a2aa")  # valid format but no Spark session
    assert res["normalised_postcode"] == "SW1A 2AA" and res["lookup_status"] == "error"
    assert set(res) >= {"normalised_postcode", "easting", "northing", "latitude", "longitude",
                        "coordinate_reference_system", "lookup_status"}


# ----------------------------------------------------------------- distance (pure Python)
def test_distance_3_4_5_triangle():
    assert straight_line_distance_m(0, 0, 3000, 4000) == pytest.approx(5000.0)


def test_distance_zero_and_symmetric():
    assert straight_line_distance_m(530000, 180000, 530000, 180000) == 0.0
    a = straight_line_distance_m(530000, 180000, 451000, 206000)
    b = straight_line_distance_m(451000, 206000, 530000, 180000)
    assert a == pytest.approx(b)


def test_unit_conversions():
    assert metres_to_km(1609.344) == pytest.approx(1.609344)
    assert metres_to_miles(1609.344) == pytest.approx(1.0)
    assert metres_to_miles(16093.44) == pytest.approx(10.0)


def test_known_city_distance_london_to_birmingham():
    # Approximate BNG centres: Trafalgar Square (530000, 180400) and Birmingham New Street (406900, 286600).
    d = straight_line_distance_m(530000, 180400, 406900, 286600)
    assert 160000 < d < 165000  # ~162.6 km / ~101 miles straight-line
    assert math.isclose(metres_to_miles(d), d / 1609.344)


# ----------------------------------------------------------------- distance (Spark Column)
@pytest.fixture(scope="module")
def spark():
    try:
        from pyspark.sql import SparkSession
        s = SparkSession.getActiveSession()
        if s is None:
            from databricks.connect import DatabricksSession
            s = DatabricksSession.builder.serverless(True).getOrCreate()
        return s
    except Exception:
        pytest.skip("No Spark session available")


def test_spark_distance_matches_python(spark):
    rows = [("a", 3000.0, 4000.0), ("b", 0.0, 0.0), ("c", 406900.0, 286600.0)]
    df = spark.createDataFrame(rows, "id string, e double, n double")
    out = {r["id"]: r["d"] for r in df.select("id", calculate_straight_line_distance(0, 0, "e", "n").alias("d")).collect()}
    assert out["a"] == pytest.approx(5000.0)
    assert out["b"] == pytest.approx(0.0)
    assert out["c"] == pytest.approx(straight_line_distance_m(0, 0, 406900, 286600))


def test_spark_distance_casts_strings_and_longs(spark):
    df = spark.createDataFrame([("3000", 4000)], "e string, n long")
    d = df.select(calculate_straight_line_distance(0.0, 0.0, "e", "n").alias("d")).first()["d"]
    assert d == pytest.approx(5000.0)
