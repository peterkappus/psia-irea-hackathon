"""LEGACY single-page GovMove postcode finder (standalone). The deployed app now runs main.py, where this
search lives under Estate Planning & Decision Support > Vacant space finder. Kept for reference; run with
`streamlit run app.py` if only the finder is wanted.

GovMove: find the nearest available government properties to a UK postcode.

Configuration values to replace before deployment are listed at the top of config.py and in README.md.
All filtering, distance calculation, sorting and limiting run in Spark (Databricks Connect); only the
final small result set is converted to pandas for display.
"""
import pandas as pd
import pydeck as pdk
import streamlit as st

import config as cfg
from govmove_core import (BNG_CRS, filter_properties, get_postcode_coordinates, normalise_postcode,
                          search_nearest, valid_bng_condition)

st.set_page_config(page_title="GovMove", layout="wide")

DISCLAIMER = ("Distances are straight-line ('as the crow flies') distances calculated from British National Grid "
              "(EPSG:27700) Easting and Northing. They are not road or travelling distances, which are usually longer.")


# ------------------------------------------------------------------------------ Spark and data
@st.cache_resource(show_spinner="Connecting to Databricks compute...")
def get_spark():
    from databricks.connect import DatabricksSession

    if cfg.COMPUTE.strip().lower() == "serverless":
        return DatabricksSession.builder.serverless(True).getOrCreate()
    return DatabricksSession.builder.clusterId(cfg.COMPUTE.strip()).getOrCreate()


@st.cache_resource(show_spinner="Loading property data...")
def get_property_frame():
    """Cleaned property DataFrame with explicit numeric casts, cached for repeated searches."""
    from pyspark.sql import functions as F

    df = (get_spark().table(cfg.PROPERTY_TABLE_FQ)
          .withColumn("bng_easting", F.col("bng_easting").cast("double"))
          .withColumn("bng_northing", F.col("bng_northing").cast("double"))
          .withColumn("available_area_sqm", F.col("available_area_sqm").cast("double")))
    try:
        cached = df.cache()
        cached.count()
        return cached
    except Exception:
        # Serverless compute does not support DataFrame.cache(); the Delta table is small and is
        # cached by the engine, and this DataFrame definition is reused across searches.
        return df


@st.cache_data(ttl=3600, show_spinner=False)
def get_filter_options() -> pd.DataFrame:
    """Distinct dataset/status/region/local-authority combinations (aggregated in Spark, small)."""
    return (get_property_frame().groupBy("dataset", "availability_status", "region", "local_authority").count()
            .toPandas())


@st.cache_data(ttl=3600, show_spinner=False)
def lookup_postcode(postcode: str) -> dict:
    return get_postcode_coordinates(postcode, get_spark(), cfg.POSTCODE_TABLE_FQ, cfg.POSTCODE_FALLBACK_TABLE,
                                    cfg.BNG_EASTING_RANGE, cfg.BNG_NORTHING_RANGE)


def dataset_label(key: str) -> str:
    return cfg.AVAILABILITY_RULES.get(key, {}).get("label", key)


# ------------------------------------------------------------------------------ UI: inputs
st.title("GovMove")
st.caption("Find the nearest available government properties to a UK postcode.")
st.info(DISCLAIMER)

try:
    options = get_filter_options()
except Exception as exc:
    st.error("GovMove could not load the property data. Check that the app's service principal can use "
             f"the configured compute and read {cfg.PROPERTY_TABLE_FQ}.\n\nDetails: {exc}")
    st.stop()

datasets_available = [d for d in cfg.AVAILABILITY_RULES if d in set(options["dataset"])] or sorted(options["dataset"].unique())

c1, c2, c3 = st.columns([2, 2, 2])
with c1:
    postcode_input = st.text_input("UK postcode (required)", placeholder="e.g. SW1A 2AA", max_chars=12)
    datasets = st.multiselect("Property dataset", datasets_available,
                              default=[cfg.DEFAULT_DATASET] if cfg.DEFAULT_DATASET in datasets_available else datasets_available[:1],
                              format_func=dataset_label)
with c2:
    status_options = sorted(options.loc[options["dataset"].isin(datasets), "availability_status"].dropna().unique())
    default_statuses = sorted({s for d in datasets for s in cfg.AVAILABILITY_RULES.get(d, {}).get("default_statuses", [])
                               if s in status_options})
    statuses = st.multiselect("Availability status", status_options, default=default_statuses,
                              help="Defaults to available properties only. Select other statuses explicitly to include them.")
    max_miles = st.number_input("Maximum distance (miles, optional)", min_value=0.0, value=None, step=5.0)
    min_area = st.number_input("Minimum available area (sqm, optional)", min_value=0.0, value=None, step=50.0)
with c3:
    region_options = sorted(options.loc[options["dataset"].isin(datasets), "region"].dropna().unique())
    regions = st.multiselect("Region (optional)", region_options)
    la_mask = options["dataset"].isin(datasets) & (options["region"].isin(regions) if regions else True)
    local_authorities = st.multiselect("Local authority (optional)", sorted(options.loc[la_mask, "local_authority"].dropna().unique()))
    n_results = st.number_input("Number of results", min_value=1, max_value=cfg.MAX_RESULTS, value=cfg.DEFAULT_RESULTS, step=1)

if st.button("Search", type="primary"):
    st.session_state["search"] = dict(postcode=postcode_input, datasets=datasets, statuses=statuses, max_miles=max_miles,
                                      min_area=min_area, regions=regions, local_authorities=local_authorities,
                                      n_results=int(n_results))

params = st.session_state.get("search")
if not params:
    st.stop()

# ------------------------------------------------------------------------------ Validation and lookup
if not normalise_postcode(params["postcode"]):
    st.error("Please enter a postcode.")
    st.stop()
if not params["datasets"]:
    st.error("Please select at least one property dataset.")
    st.stop()
if not params["statuses"]:
    st.error("Please select at least one availability status.")
    st.stop()

with st.spinner("Locating postcode..."):
    loc = lookup_postcode(params["postcode"])

status = loc["lookup_status"]
if status != "found":
    titles = {"blank": "Blank postcode", "invalid_format": "Invalid postcode", "not_found": "Postcode not found",
              "missing_coordinates": "Missing postcode coordinates", "crs_mismatch": "Coordinate reference system mismatch",
              "error": "Lookup error"}
    st.error(f"**{titles.get(status, 'Postcode problem')}.** {loc['message']}")
    st.stop()

# Both sides must be in the same CRS: property coordinates are normalised to BNG by the data-prep notebook.
if loc["coordinate_reference_system"] != BNG_CRS:
    st.error(f"**Coordinate reference system mismatch.** The postcode coordinates are in "
             f"{loc['coordinate_reference_system']}, but property coordinates are in {BNG_CRS}.")
    st.stop()

# ------------------------------------------------------------------------------ Search (Spark)
from pyspark.sql import functions as F  # noqa: E402

with st.spinner("Searching properties..."):
    try:
        filtered = filter_properties(get_property_frame(), params["datasets"], params["statuses"], params["regions"],
                                     params["local_authorities"], params["min_area"])
        bad = filtered.filter(~(F.col("is_comparable") & valid_bng_condition("bng_easting", "bng_northing",
                                                                              cfg.BNG_EASTING_RANGE, cfg.BNG_NORTHING_RANGE)))
        bad_pdf = (bad.select("record_reference", "property_name", "full_address", "availability_status", "coordinate_status",
                              "easting_raw", "northing_raw", "ni_easting_raw", "ni_northing_raw", "coordinate_note")
                   .limit(200).toPandas())
        bad_count = bad.count() if len(bad_pdf) >= 200 else len(bad_pdf)
        results_sdf, scored = search_nearest(filtered, loc["easting"], loc["northing"], params["max_miles"],
                                             params["n_results"], cfg.BNG_EASTING_RANGE, cfg.BNG_NORTHING_RANGE)
        results = results_sdf.toPandas()
        matching = scored.count()
    except Exception as exc:
        st.error(f"The property search failed. Details: {exc}")
        st.stop()

# ------------------------------------------------------------------------------ Summary
st.subheader("Search summary")
m1, m2, m3, m4 = st.columns(4)
m1.metric("Searched postcode", loc["normalised_postcode"])
m2.metric("Postcode Easting / Northing", f"{loc['easting']:,.0f} / {loc['northing']:,.0f}")
m3.metric("Matching properties", f"{matching:,}")
if len(results):
    m4.metric("Nearest property", f"{results.iloc[0]['distance_miles']:.2f} miles")
    st.markdown(f"Nearest: **{results.iloc[0]['property_name']}**, {results.iloc[0]['full_address']} - "
                f"{results.iloc[0]['distance_miles']:.2f} miles ({results.iloc[0]['distance_km']:.2f} km).")
else:
    m4.metric("Nearest property", "None")
st.caption(f"Postcode coordinates: {loc['coordinate_reference_system']} (British National Grid), mean of "
           f"{loc['address_count'] or 0:,} address points. {DISCLAIMER}")

if bad_count:
    st.warning(f"{bad_count:,} matching record(s) were excluded because their coordinates are missing, zero, outside the "
               "valid range, or in a coordinate system that cannot be compared reliably. See 'Records that could not be compared' below.")

if not len(results):
    within = f" within {params['max_miles']:g} miles" if params["max_miles"] else ""
    st.warning(f"No available properties were found{within} of {loc['normalised_postcode']} with the selected filters. "
               "Try a larger distance, fewer filters, or other availability statuses.")
else:
    display = results.rename(columns={
        "property_reference": "Property reference", "record_reference": "Record reference", "dataset": "Dataset",
        "property_name": "Property name", "full_address": "Full address", "postcode": "Property postcode",
        "region": "Region", "local_authority": "Local authority", "availability_status": "Availability status",
        "available_area": "Available area", "area_units": "Area units", "available_start_date": "Available start date",
        "bng_easting": "Easting", "bng_northing": "Northing", "source_crs": "Coordinate source",
        "distance_miles": "Distance (miles)", "distance_km": "Distance (km)"})
    display["Dataset"] = display["Dataset"].map(dataset_label)
    table_cols = ["Property reference", "Property name", "Full address", "Property postcode", "Region", "Local authority",
                  "Availability status", "Available area", "Area units", "Available start date", "Easting", "Northing",
                  "Distance (miles)", "Distance (km)", "Dataset", "Record reference", "Coordinate source"]
    st.subheader(f"Nearest {len(results)} of {matching:,} matching properties")
    st.dataframe(display[table_cols], hide_index=True, use_container_width=True,
                 column_config={"Easting": st.column_config.NumberColumn(format="%.0f"),
                                "Northing": st.column_config.NumberColumn(format="%.0f"),
                                "Distance (miles)": st.column_config.NumberColumn(format="%.2f"),
                                "Distance (km)": st.column_config.NumberColumn(format="%.2f")})
    st.download_button("Download results as CSV", display[table_cols].to_csv(index=False).encode("utf-8"),
                       file_name=f"govmove_{loc['normalised_postcode'].replace(' ', '')}.csv", mime="text/csv")

    # Map: latitude/longitude used for display only, never for distance.
    st.subheader("Map")
    pts = results.dropna(subset=["map_latitude", "map_longitude"]).copy()
    pts["label"] = pts["property_name"].fillna("") + " - " + pts["distance_miles"].map(lambda v: f"{v:.2f} miles")
    layers = [pdk.Layer("ScatterplotLayer", pts, get_position="[map_longitude, map_latitude]", get_radius=250,
                        radius_min_pixels=5, get_fill_color=[34, 114, 180, 200], pickable=True)]
    if loc["latitude"] is not None and loc["longitude"] is not None:
        origin = pd.DataFrame([{"lat": loc["latitude"], "lon": loc["longitude"], "label": f"Searched postcode {loc['normalised_postcode']}"}])
        layers.append(pdk.Layer("ScatterplotLayer", origin, get_position="[lon, lat]", get_radius=350,
                                radius_min_pixels=8, get_fill_color=[192, 57, 43, 230], pickable=True))
        centre_lat, centre_lon = loc["latitude"], loc["longitude"]
    else:
        centre_lat, centre_lon = pts["map_latitude"].mean(), pts["map_longitude"].mean()
    max_d = float(results["distance_miles"].max() or 1)
    zoom = 12 if max_d < 2 else 10 if max_d < 10 else 8 if max_d < 40 else 6
    st.pydeck_chart(pdk.Deck(layers=layers, map_style=None, tooltip={"text": "{label}"},
                             initial_view_state=pdk.ViewState(latitude=centre_lat, longitude=centre_lon, zoom=zoom)))
    st.caption("Red: searched postcode. Blue: matching properties. "
               f"{len(results) - len(pts)} result(s) have no map position." if len(results) != len(pts) else
               "Red: searched postcode. Blue: matching properties.")

if bad_count:
    with st.expander(f"Records that could not be compared ({bad_count:,})"):
        st.dataframe(bad_pdf, hide_index=True, use_container_width=True)
