"""GovMove core logic: postcode normalisation, postcode lookup, straight-line distance and search.

WHY PYTHAGOREAN DISTANCE IS VALID HERE
  British National Grid (BNG, EPSG:27700) is a projected coordinate reference system: Easting and
  Northing are metres on a flat Transverse Mercator plane. On a plane, the straight-line distance
  between two points is simply sqrt(dE^2 + dN^2). The BNG projection scale factor varies only
  between about 0.9996 and 1.0007 across GB, so distances are accurate to within roughly 0.1%,
  which is far below the precision needed for 'nearest property' searches.

WHY HAVERSINE IS NOT REQUIRED
  Haversine computes great-circle distance from latitude/longitude on a sphere. We never use
  latitude/longitude in the distance calculation: the coordinates are already projected metres, so
  the projection has already handled the Earth's curvature. Applying Haversine to Easting/Northing
  would be meaningless, and applying Pythagoras to latitude/longitude would be wrong (degrees are
  not equal-length units).

WHY BOTH POINTS MUST USE THE SAME COORDINATE REFERENCE SYSTEM
  Easting/Northing values only mean something relative to their own grid origin. BNG and the Irish
  Grid (EPSG:29903) have different origins, projections and datums, so subtracting an Irish Grid
  Easting from a BNG Easting gives a meaningless number. Every property is therefore converted to
  BNG (or labelled as not comparable) in the data-prep notebook, and the postcode lookup returns BNG.

WHY THIS DIFFERS FROM DRIVING DISTANCE
  Straight-line ('as the crow flies') distance ignores roads, rivers, estuaries, ferries, one-way
  systems and terrain. Real travel distance is always equal to or longer than it, often 20-40%
  longer in towns and much more where there is water or no direct route.
"""
from __future__ import annotations

import math
import re
from dataclasses import asdict, dataclass
from typing import Optional

METRES_PER_KM = 1000.0
METRES_PER_MILE = 1609.344
BNG_CRS = "EPSG:27700"

# UK postcode format (outward code + space + inward code), e.g. SW1A 2AA, M1 1AE, BT1 5GS.
POSTCODE_PATTERN = re.compile(r"^(GIR 0AA|[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2})$")


# ---------------------------------------------------------------------------------------------
# Postcode normalisation
# ---------------------------------------------------------------------------------------------
def normalise_postcode(raw: Optional[str]) -> str:
    """Upper-case, remove all whitespace, and re-insert one space before the final three characters.

    Returns '' for blank input. If the postcode is too short or long to split (fewer than 5 or more
    than 7 characters without spaces) it is returned upper-cased without spaces.
    """
    if raw is None:
        return ""
    compact = re.sub(r"\s+", "", str(raw)).upper()
    if 5 <= len(compact) <= 7:
        return f"{compact[:-3]} {compact[-3:]}"
    return compact


def is_valid_postcode_format(postcode: str) -> bool:
    """True if an already-normalised postcode matches the UK postcode pattern."""
    return bool(postcode) and bool(POSTCODE_PATTERN.match(postcode))


# ---------------------------------------------------------------------------------------------
# Distance
# ---------------------------------------------------------------------------------------------
def straight_line_distance_m(e1: float, n1: float, e2: float, n2: float) -> float:
    """Pure-Python Pythagorean distance in metres between two points in the same projected CRS."""
    return math.sqrt((float(e2) - float(e1)) ** 2 + (float(n2) - float(n1)) ** 2)


def metres_to_km(metres: float) -> float:
    return metres / METRES_PER_KM


def metres_to_miles(metres: float) -> float:
    return metres / METRES_PER_MILE


def calculate_straight_line_distance(input_easting, input_northing, property_easting_column, property_northing_column):
    """Spark Column with the straight-line distance in metres.

    distance_metres = sqrt(pow(property_easting - input_easting, 2) + pow(property_northing - input_northing, 2))

    input_easting / input_northing: numeric BNG coordinates of the searched postcode.
    property_*_column: column names or Columns holding the property's BNG coordinates.
    Both inputs MUST be in the same projected CRS (BNG EPSG:27700); explicit double casts are applied.
    """
    from pyspark.sql import functions as F

    pe = F.col(property_easting_column) if isinstance(property_easting_column, str) else property_easting_column
    pn = F.col(property_northing_column) if isinstance(property_northing_column, str) else property_northing_column
    return F.sqrt(
        F.pow(pe.cast("double") - F.lit(float(input_easting)), 2)
        + F.pow(pn.cast("double") - F.lit(float(input_northing)), 2)
    )


# ---------------------------------------------------------------------------------------------
# Postcode lookup
# ---------------------------------------------------------------------------------------------
@dataclass
class PostcodeLookupResult:
    normalised_postcode: str
    easting: Optional[float] = None
    northing: Optional[float] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    coordinate_reference_system: Optional[str] = None
    lookup_status: str = "not_found"
    message: str = ""
    address_count: Optional[int] = None
    lookup_source: Optional[str] = None


def _in_range(value, rng) -> bool:
    return value is not None and rng[0] < float(value) <= rng[1]


def get_postcode_coordinates(postcode, spark=None, lookup_table=None, fallback_table=None,
                             bng_easting_range=(0.0, 700000.0), bng_northing_range=(0.0, 1300000.0)) -> dict:
    """Resolve a UK postcode to British National Grid (EPSG:27700) coordinates.

    Lookup order: the postcode centroid table built by the data-prep notebook (mean of current
    AddressBase Premium address points), then the raw AddressBase Premium table as a fallback.
    AddressBase Premium Easting/Northing are BNG for both GB and Northern Ireland addresses.

    Returns a dict with: normalised_postcode, easting, northing, latitude, longitude,
    coordinate_reference_system, lookup_status, message, address_count, lookup_source.
    lookup_status is one of: found, blank, invalid_format, not_found, missing_coordinates,
    crs_mismatch, error.
    """
    pc = normalise_postcode(postcode)
    if not pc:
        return asdict(PostcodeLookupResult(pc, lookup_status="blank", message="Please enter a postcode."))
    if not is_valid_postcode_format(pc):
        return asdict(PostcodeLookupResult(pc, lookup_status="invalid_format",
                      message=f"'{postcode}' is not a valid UK postcode format (for example SW1A 2AA)."))
    if spark is None:
        return asdict(PostcodeLookupResult(pc, lookup_status="error", message="No Spark session available for the postcode lookup."))

    from pyspark.sql import functions as F

    row, source = None, None
    try:
        if lookup_table:
            try:
                rows = (spark.table(lookup_table).filter(F.col("postcode") == pc)
                        .select("easting", "northing", "latitude", "longitude", "address_count").limit(1).collect())
                if rows:
                    row, source = rows[0], lookup_table
            except Exception as exc:  # centroid table missing or unreadable: fall back
                if not fallback_table:
                    raise exc
        if row is None and fallback_table:
            raw = spark.table(fallback_table).filter(F.upper(F.trim(F.col("postcode"))) == pc)
            rows = (raw.agg(F.avg(F.when(F.col("easting") > 0, F.col("easting").cast("double"))).alias("easting"),
                            F.avg(F.when(F.col("northing") > 0, F.col("northing").cast("double"))).alias("northing"),
                            F.avg(F.col("latitude").cast("double")).alias("latitude"),
                            F.avg(F.col("longitude").cast("double")).alias("longitude"),
                            F.count(F.lit(1)).alias("address_count")).collect())
            if rows and rows[0]["address_count"]:
                row, source = rows[0], fallback_table
    except Exception as exc:
        return asdict(PostcodeLookupResult(pc, lookup_status="error", message=f"The postcode lookup failed: {exc}"))

    if row is None:
        return asdict(PostcodeLookupResult(pc, lookup_status="not_found",
                      message=f"Postcode {pc} could not be found in the address reference data. Check it and try again."))
    res = PostcodeLookupResult(pc, easting=row["easting"], northing=row["northing"], latitude=row["latitude"],
                               longitude=row["longitude"], address_count=int(row["address_count"] or 0), lookup_source=source)
    if res.easting is None or res.northing is None or res.easting == 0 or res.northing == 0:
        res.lookup_status = "missing_coordinates"
        res.message = f"Postcode {pc} was found but has no usable Easting/Northing coordinates."
    elif not (_in_range(res.easting, bng_easting_range) and _in_range(res.northing, bng_northing_range)):
        res.lookup_status = "crs_mismatch"
        res.message = (f"Postcode {pc} coordinates ({res.easting:.0f}, {res.northing:.0f}) are outside the British National "
                       "Grid range, so they cannot be compared with property coordinates.")
    else:
        res.easting, res.northing = float(res.easting), float(res.northing)
        res.coordinate_reference_system = BNG_CRS
        res.lookup_status = "found"
    return asdict(res)


# ---------------------------------------------------------------------------------------------
# Property search (all filtering, distance, sorting and limiting happens in Spark)
# ---------------------------------------------------------------------------------------------
def valid_bng_condition(e_col, n_col, e_range=(0.0, 700000.0), n_range=(0.0, 1300000.0)):
    """Spark condition: coordinates not null, not zero and inside the BNG range."""
    from pyspark.sql import functions as F

    e, n = F.col(e_col).cast("double"), F.col(n_col).cast("double")
    return (e.isNotNull() & n.isNotNull() & (e != 0) & (n != 0)
            & (e > e_range[0]) & (e <= e_range[1]) & (n > n_range[0]) & (n <= n_range[1]))


def filter_properties(df, datasets=None, statuses=None, regions=None, local_authorities=None, min_area_sqm=None):
    """Apply the non-distance user filters to the cleaned property DataFrame."""
    from pyspark.sql import functions as F

    if datasets:
        df = df.filter(F.col("dataset").isin(list(datasets)))
    if statuses:
        df = df.filter(F.col("availability_status").isin(list(statuses)))
    if regions:
        df = df.filter(F.col("region").isin(list(regions)))
    if local_authorities:
        df = df.filter(F.col("local_authority").isin(list(local_authorities)))
    if min_area_sqm is not None and min_area_sqm > 0:
        df = df.filter(F.col("available_area_sqm").cast("double") >= float(min_area_sqm))
    return df


RESULT_COLUMNS = [
    "property_reference", "record_reference", "dataset", "property_name", "full_address", "postcode", "region",
    "local_authority", "availability_status", "available_area", "area_units", "available_start_date",
    "bng_easting", "bng_northing", "source_crs", "distance_miles", "distance_km", "map_latitude", "map_longitude",
]


def search_nearest(df, input_easting, input_northing, max_distance_miles=None, limit=10,
                   e_range=(0.0, 700000.0), n_range=(0.0, 1300000.0)):
    """Distance, distance filter, sort and limit in Spark. Returns (result Spark DataFrame, matching-count DataFrame).

    Only records whose coordinates are comparable in BNG are used; others are excluded and reported
    separately by the caller.
    """
    from pyspark.sql import functions as F

    comparable = df.filter(F.col("is_comparable") & valid_bng_condition("bng_easting", "bng_northing", e_range, n_range))
    dist_m = calculate_straight_line_distance(input_easting, input_northing, "bng_easting", "bng_northing")
    scored = (comparable.withColumn("distance_m", dist_m)
              .withColumn("_distance_miles", F.col("distance_m") / F.lit(METRES_PER_MILE))
              .withColumn("distance_miles", F.round(F.col("distance_m") / F.lit(METRES_PER_MILE), 2))
              .withColumn("distance_km", F.round(F.col("distance_m") / F.lit(METRES_PER_KM), 2)))
    if max_distance_miles is not None and max_distance_miles > 0:
        scored = scored.filter(F.col("_distance_miles") <= float(max_distance_miles))
    results = scored.orderBy(F.col("_distance_miles").asc(), F.col("property_reference").asc()).limit(int(limit)).select(*RESULT_COLUMNS)
    return results, scored
