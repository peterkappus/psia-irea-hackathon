"""Spark session, cached queries and location resolution shared by all GovMove views.

All aggregation, filtering and distance work runs in Spark; only small aggregated or limited
results are converted to pandas. SQL filter strings passed to helpers are defined in code, never
built from user input (user input is applied with the DataFrame API).
"""
import re

import pandas as pd
import streamlit as st

import config as cfg
from govmove_core import (METRES_PER_MILE, calculate_straight_line_distance, get_postcode_coordinates,
                          is_valid_postcode_format, normalise_postcode)

NOT_DUPLICATE = "NOT coalesce(is_duplicate_copy, false)"


@st.cache_resource(show_spinner="Connecting to Databricks compute...")
def get_spark():
    from databricks.connect import DatabricksSession

    if cfg.COMPUTE.strip().lower() == "serverless":
        return DatabricksSession.builder.serverless(True).getOrCreate()
    return DatabricksSession.builder.clusterId(cfg.COMPUTE.strip()).getOrCreate()


def F():
    from pyspark.sql import functions
    return functions


def table(name):
    return get_spark().table(name)


@st.cache_data(ttl=3600, show_spinner=False)
def group_count(table_name, col, where=None, regions=(), region_col="region_clean", top=15, value=None):
    """Count (or sum `value`) by `col`, optionally filtered by a code-defined `where` and by regions."""
    f = F()
    df = table(table_name)
    if where:
        df = df.filter(where)
    if regions:
        df = df.filter(f.col(region_col).isin(list(regions)))
    agg = f.count(f.lit(1)) if value is None else f.sum(f.col(value).cast("double"))
    label = f.coalesce(f.nullif(f.trim(f.col(col).cast("string")), f.lit("")), f.lit("Not recorded"))
    return (df.groupBy(label.alias("label")).agg(agg.alias("value"))
            .orderBy(f.desc("value")).limit(top).toPandas())


@st.cache_data(ttl=3600, show_spinner=False)
def scalar_aggs(table_name, exprs, where=None):
    """Evaluate a tuple of (alias, SQL expression) aggregates defined in code. Returns a dict."""
    f = F()
    df = table(table_name)
    if where:
        df = df.filter(where)
    row = df.agg(*[f.expr(e).alias(a) for a, e in exprs]).first()
    return row.asDict() if row else {}


@st.cache_data(ttl=3600, show_spinner=False)
def regions():
    f = F()
    return [r[0] for r in table(cfg.EVIDENCE_TABLE_FQ).select("region").where(f.col("region").isNotNull())
            .distinct().orderBy("region").collect()]


@st.cache_data(ttl=3600, show_spinner=False)
def map_points(flag_col=None, regions_sel=(), limit=20000):
    """Property points with valid latitude/longitude from the evidence table (map display only)."""
    f = F()
    df = table(cfg.EVIDENCE_TABLE_FQ).filter(f.col("latitude").isNotNull() & f.col("longitude").isNotNull())
    if flag_col:
        df = df.filter(f.col(flag_col) > 0)
    if regions_sel:
        df = df.filter(f.col("region").isin(list(regions_sel)))
    return (df.select("property_reference", "property_name", "town", "region", "latitude", "longitude",
                      "occupiers", "vacant_available_area", "surplus_records")
            .orderBy("property_reference").limit(limit).toPandas())


@st.cache_data(ttl=3600, show_spinner=False)
def lookup_postcode(postcode):
    return get_postcode_coordinates(postcode, get_spark(), cfg.POSTCODE_TABLE_FQ, cfg.POSTCODE_FALLBACK_TABLE,
                                    cfg.BNG_EASTING_RANGE, cfg.BNG_NORTHING_RANGE)


@st.cache_data(ttl=3600, show_spinner=False)
def resolve_location(text):
    """Resolve a postcode, property reference, town, local authority or region to a BNG point.

    Postcodes use the postcode lookup table; other locations use the mean BNG position of matching
    properties. Returns dict with status ('found' or an error status), message, kind, label,
    easting, northing, latitude, longitude.
    """
    t = (text or "").strip()
    if not t:
        return {"status": "blank", "message": "Please enter a location."}
    pc = normalise_postcode(t)
    if is_valid_postcode_format(pc):
        r = lookup_postcode(pc)
        if r["lookup_status"] == "found":
            return {"status": "found", "kind": "postcode", "label": pc, "easting": r["easting"],
                    "northing": r["northing"], "latitude": r["latitude"], "longitude": r["longitude"], "message": ""}
        return {"status": r["lookup_status"], "message": r["message"]}
    f = F()
    ev = table(cfg.EVIDENCE_TABLE_FQ).filter(f.col("has_bng"))
    if re.fullmatch(r"[Pp]\d{5,9}", t):
        rows = (ev.filter(f.col("property_reference") == t.upper())
                .select("property_name", "bng_easting", "bng_northing", "latitude", "longitude").limit(1).collect())
        if rows:
            r = rows[0]
            return {"status": "found", "kind": "property", "label": f"{t.upper()} {r['property_name'] or ''}".strip(),
                    "easting": r["bng_easting"], "northing": r["bng_northing"], "latitude": r["latitude"],
                    "longitude": r["longitude"], "message": ""}
    for col, kind in (("town", "town"), ("local_authority", "local authority"), ("region", "region")):
        r = (ev.filter(f.upper(f.trim(f.col(col))) == t.upper())
             .agg(f.avg("bng_easting").alias("e"), f.avg("bng_northing").alias("n"), f.avg("latitude").alias("lat"),
                  f.avg("longitude").alias("lon"), f.count(f.lit(1)).alias("c")).first())
        if r and r["c"]:
            return {"status": "found", "kind": kind, "label": t.title(), "easting": r["e"], "northing": r["n"],
                    "latitude": r["lat"], "longitude": r["lon"], "message": "", "matched_properties": int(r["c"])}
    return {"status": "not_found",
            "message": f"'{t}' did not match a postcode, property reference, town, local authority or region."}


@st.cache_data(ttl=3600, show_spinner=False)
def nearby_properties(easting, northing, radius_miles, limit=1000):
    """Properties within a straight-line radius (BNG Pythagorean distance), nearest first."""
    f = F()
    ev = table(cfg.EVIDENCE_TABLE_FQ).filter(f.col("has_bng"))
    d = calculate_straight_line_distance(easting, northing, "bng_easting", "bng_northing") / f.lit(METRES_PER_MILE)
    return (ev.withColumn("distance_miles", d).filter(f.col("distance_miles") <= float(radius_miles))
            .orderBy("distance_miles").limit(int(limit))
            .withColumn("distance_miles", f.round("distance_miles", 2)).toPandas())


@st.cache_data(ttl=3600, show_spinner=False)
def search_evidence(query, regions_sel=(), limit=200):
    """Property explorer search on reference, name, town or postcode (case-insensitive contains)."""
    f = F()
    df = table(cfg.EVIDENCE_TABLE_FQ)
    q = (query or "").strip().upper()
    if q:
        cond = f.lit(False)
        for c in ("property_reference", "property_name", "town", "postcode"):
            cond = cond | f.upper(f.coalesce(f.col(c), f.lit(""))).contains(q)
        df = df.filter(cond)
    if regions_sel:
        df = df.filter(f.col("region").isin(list(regions_sel)))
    return df.orderBy("property_reference").limit(int(limit)).toPandas()


@st.cache_data(ttl=3600, show_spinner=False)
def related_records(ref):
    """Source-level records for one property, joined only on exact Insite Property Reference."""
    f = F()
    ct = cfg.CLEAN_TABLES
    by_ref = lambda name: table(name).filter((f.col("insite_property_reference") == ref) & f.expr(NOT_DUPLICATE))
    out = {
        "Contracts": by_ref(ct["property"]).select("insite_contract_reference", "contract_name", "contract_organisation",
                                                   "tenure", "asset_type", "usage").distinct(),
        "Buildings": by_ref(ct["building"]).select("insite_building_reference", "building_name", "floor_area",
                                                   "floor_area_measurement", "construction_date", "usage",
                                                   "listed_building", "epc_band", "dec_band"),
        "Occupations": by_ref(ct["occupation"]).select("insite_occupation_reference", "occupation_name", "organisation",
                                                       "floor_area", "floor_area_measurement", "agreement_type", "usage"),
        "Land": by_ref(ct["land"]).select("insite_land_reference", "total_land_area", "land_area_hectares", "usage",
                                          "title_number", "conservation_area"),
    }
    loc = table(cfg.PROPERTY_TABLE_FQ).filter(f.col("property_reference") == ref)
    out["Vacancy"] = loc.filter(f.col("dataset") == "vacant_space").select(
        "record_reference", "availability_status", "available_area", "area_units", "available_start_date", "available_end_date")
    out["Surplus"] = loc.filter(f.col("dataset") == "surplus").select(
        "record_reference", "availability_status", "available_area", "area_units", "surplus_land_area_hectares")
    return {k: v.limit(500).toPandas() for k, v in out.items()}


def to_pdf(df) -> pd.DataFrame:
    return df.toPandas()
