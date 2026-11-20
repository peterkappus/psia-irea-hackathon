"""Estate Planning & Decision Support views (investigate capacity and planning options)."""
import pandas as pd
import pydeck as pdk
import streamlit as st

import config as cfg
import data_access as da
from estate_views import card, fmt, hbar, page_title, point_map, region_filter, render_colocation
from govmove_core import BNG_CRS, filter_properties, normalise_postcode, search_nearest, valid_bng_condition
from prioritisation_views import render_prioritisation

DISCLAIMER = ("Distances are straight-line ('as the crow flies') distances calculated from British National Grid "
              "(EPSG:27700) Easting and Northing. They are not road or travelling distances, which are usually longer.")

TASKS = [
    ("space", "Find space", "Create an evidence-led What If? scenario: vacant and surplus options near a location."),
    ("clusters", "Find co-location opportunities", "Properties with multiple occupiers and recorded vacant space nearby."),
    ("utilisation", "Building utilisation explorer", "Compare recorded building, occupied and vacant floor areas by property."),
    ("footprint", "Government footprint explorer", "Understand government presence in a town, city, region or postcode."),
    ("vacancy", "Vacant space finder", "Nearest available properties to a UK postcode by straight-line distance."),
    ("opportunities", "Estate opportunity explorer", "Properties ranked by the investigation priority score."),
    ("prioritisation", "Sustainability & investment prioritisation",
     "Explainable 100-point ranking for consolidation, vacancy, disposal, sustainability and data-quality review."),
]


def open_task(task_id, location=None):
    st.session_state["task"] = task_id
    if location:
        st.session_state["planning_location"] = location


# ------------------------------------------------------------------------------ landing
def render_landing():
    page_title("ESTATE PLANNING & DECISION SUPPORT", "Make Better Use of the Government Estate",
               "Identify capacity, utilisation, co-location and property planning opportunities using government estate data.")
    c1, c2 = st.columns([4, 1])
    with c1:
        loc = st.text_input("Search a town, city, region or postcode", key="landing_loc", label_visibility="collapsed",
                            placeholder="Search a town, city, region or postcode")
    with c2:
        if st.button("Explore footprint", type="primary", use_container_width=True) and loc:
            open_task("footprint", loc)
            st.rerun()
    st.caption("Try:")
    tc = st.columns(4)
    for col, ex in zip(tc, ["Glasgow", "Newcastle upon Tyne", "CF10 3NQ", "Wales"]):
        if col.button(ex, key=f"try_{ex}", use_container_width=True):
            open_task("footprint", ex)
            st.rerun()
    st.markdown("<div class='gm-kicker'>START WITH A TASK</div><h3>What do you need to do?</h3>", unsafe_allow_html=True)
    st.caption("Each task uses connected source records and preserves their original relationships.")
    for row in (TASKS[i:i + 3] for i in range(0, len(TASKS), 3)):
        cols = st.columns(3)
        for col, (tid, title, desc) in zip(cols, row):
            with col:
                with st.container(border=True):
                    st.markdown(f"**{title}**")
                    st.caption(desc)
                    if st.button("Open", key=f"task_{tid}", use_container_width=True):
                        open_task(tid)
                        st.rerun()
    with st.container(border=True):
        st.markdown("<div class='gm-kicker'>FLAGSHIP PLANNING TOOL</div><h3>What If?</h3>", unsafe_allow_html=True)
        st.caption("Create an evidence-led scenario using existing property, occupation and capacity records.")
        w1, w2 = st.columns(2)
        if w1.button("\u201cWe need office space in Glasgow\u201d", use_container_width=True):
            open_task("space", "Glasgow")
            st.rerun()
        if w2.button("\u201cWe want to understand government presence in Newcastle\u201d", use_container_width=True):
            open_task("footprint", "Newcastle upon Tyne")
            st.rerun()


# ------------------------------------------------------------------------------ Find space (What If?)
def render_find_space():
    page_title("WHAT IF? SCENARIO", "Find space", "Potential options for investigation \u2014 not a recommendation.")
    c1, c2, c3, c4 = st.columns([3, 2, 2, 1])
    with c1:
        loc_text = st.text_input("Location", value=st.session_state.get("planning_location", ""),
                                 placeholder="For example, Glasgow", key="fs_loc")
    with c2:
        min_area = st.number_input("Minimum area (sqm)", min_value=0.0, value=0.0, step=50.0, key="fs_area")
    with c3:
        radius = st.selectbox("Within", [5, 10, 25, 50, 100], index=2, key="fs_radius",
                              format_func=lambda m: f"{m} miles")
    with c4:
        st.write("")
        go = st.button("Find options", type="primary", key="fs_go")
    if go:
        st.session_state["fs_params"] = (loc_text, min_area, radius)
    params = st.session_state.get("fs_params")
    if not params:
        st.info("Enter a location to surface matching vacant space and surplus evidence.")
        return
    loc = da.resolve_location(params[0])
    if loc["status"] != "found":
        st.error(loc["message"])
        return
    statuses = sorted({s for r in cfg.AVAILABILITY_RULES.values() for s in r["default_statuses"]})
    base = filter_properties(da.table(cfg.PROPERTY_TABLE_FQ), list(cfg.AVAILABILITY_RULES), statuses,
                             min_area_sqm=params[1] or None)
    res_sdf, scored = search_nearest(base, loc["easting"], loc["northing"], params[2], 50)
    res = res_sdf.toPandas()
    near = da.nearby_properties(loc["easting"], loc["northing"], params[2])
    st.markdown(f"#### LOCATION EVIDENCE \u2014 within {params[2]} miles of {loc['label']} ({loc['kind']})")
    st.caption("Potential options for investigation based on connected records. " + DISCLAIMER)
    c = st.columns(4)
    for col, (lab, v) in zip(c, [("properties", len(near)), ("occupiers", int(near["occupiers"].sum()) if len(near) else 0),
                                 ("vacant area (sqm)", near["vacant_available_area"].sum() if len(near) else 0),
                                 ("surplus records", int(near["surplus_records"].sum()) if len(near) else 0)]):
        with col:
            card(lab.upper(), fmt(v))
    if res.empty:
        st.warning("No linked records match these filters. Try a broader location or lower area requirement.")
        return
    ev = near[["property_reference", "holding_department"]].drop_duplicates("property_reference") if len(near) else None
    if ev is not None:
        res = res.merge(ev, on="property_reference", how="left")
    st.markdown(f"#### {len(res)} option(s), nearest first (of {scored.count():,} matching)")
    for _, r in res.head(12).iterrows():
        with st.container(border=True):
            a, b, c_, d = st.columns([4, 2, 2, 2])
            a.markdown(f"**{r['property_name'] or r['property_reference']}**  \n{r['full_address'] or ''}")
            b.markdown(f"<div class='gm-card-label'>VACANT AREA</div>{fmt(r['available_area'])} sqm", unsafe_allow_html=True)
            c_.markdown(f"<div class='gm-card-label'>HOLDING ORGANISATION</div>{r.get('holding_department') or 'Not recorded'}",
                        unsafe_allow_html=True)
            d.markdown(f"<div class='gm-card-label'>DISTANCE</div>{r['distance_miles']:.2f} miles \u00b7 "
                       f"{'Vacant space' if r['dataset'] == 'vacant_space' else 'Surplus'} ({r['availability_status']})",
                       unsafe_allow_html=True)
    with st.expander("All options as a table"):
        st.dataframe(res, hide_index=True, use_container_width=True)
    point_map(res.rename(columns={"map_latitude": "latitude", "map_longitude": "longitude"}).dropna(subset=["latitude"]),
              (244, 119, 56), radius=300, origin=loc, zoom=9)


# ------------------------------------------------------------------------------ footprint
def render_footprint():
    page_title("GUIDED INVESTIGATION", "Government footprint explorer",
               "Understand government presence around a town, city, region or postcode.")
    c1, c2, c3 = st.columns([3, 2, 1])
    with c1:
        loc_text = st.text_input("Location", value=st.session_state.get("planning_location", ""),
                                 placeholder="Town, city, region or postcode", key="fp_loc")
    with c2:
        radius = st.selectbox("Within", [1, 5, 10, 25], index=1, key="fp_radius", format_func=lambda m: f"{m} miles")
    with c3:
        st.write("")
        st.button("Search", type="primary", key="fp_go")
    if not loc_text:
        st.info("Enter a location.")
        return
    loc = da.resolve_location(loc_text)
    if loc["status"] != "found":
        st.error(loc["message"])
        return
    near = da.nearby_properties(loc["easting"], loc["northing"], radius, limit=5000)
    if near.empty:
        st.warning("No properties with valid coordinates were found within this radius.")
        return
    if len(near) >= 10 and near["holding_department"].nunique() >= 3:
        st.success(f"GOVERNMENT CLUSTER IDENTIFIED \u2014 {len(near):,} properties from "
                   f"{near['holding_department'].nunique()} organisations within {radius} miles of {loc['label']}.")
    c = st.columns(4)
    for col, (lab, v) in zip(c, [("Properties", len(near)), ("Occupiers", near["occupiers"].sum()),
                                 ("Vacant space (sqm)", near["vacant_available_area"].sum()),
                                 ("Surplus assets", near["surplus_records"].sum())]):
        with col:
            card(lab, fmt(v))
    a, b = st.columns([3, 2])
    with a:
        point_map(near, (29, 112, 184), radius=120, origin=loc, zoom=12 if radius <= 1 else 10 if radius <= 10 else 9)
    with b:
        top = near.groupby(near["holding_department"].fillna("Not recorded")).size().sort_values(ascending=False).head(12)
        hbar(pd.DataFrame({"label": top.index, "value": top.values}), "Holding organisations", "Properties in radius")
    st.dataframe(near[["property_reference", "property_name", "town", "distance_miles", "holding_department", "buildings",
                       "occupiers", "vacant_available_area", "surplus_records"]], hide_index=True, use_container_width=True)
    st.caption(DISCLAIMER)


# ------------------------------------------------------------------------------ utilisation
def render_utilisation():
    page_title("GUIDED INVESTIGATION", "Building utilisation explorer",
               "Recorded building floor area compared with recorded occupation and available vacant area. Areas come "
               "from different extracts and measurement bases, so this is evidence for investigation, not a utilisation rate.")
    regs = region_filter("ut_regions")
    f = da.F()
    df = da.table(cfg.EVIDENCE_TABLE_FQ).filter((f.col("building_floor_area") > 0) &
                                                ((f.col("occupation_records") > 0) | (f.col("vacant_records") > 0)))
    if regs:
        df = df.filter(f.col("region").isin(list(regs)))
    pdf = (df.withColumn("vacant_share_pct", f.round(100 * f.coalesce(f.col("vacant_available_area"), f.lit(0)) /
                                                      f.col("building_floor_area"), 1))
           .select("property_reference", "property_name", "town", "region", "holding_department", "buildings",
                   "building_floor_area", "occupiers", "occupied_area", "vacant_available_area", "vacant_share_pct")
           .orderBy(f.desc("vacant_share_pct")).limit(500).toPandas())
    st.caption(f"{len(pdf):,} properties with building floor area and occupation or vacant space records"
               + (" (first 500)" if len(pdf) >= 500 else ""))
    st.dataframe(pdf, hide_index=True, use_container_width=True,
                 column_config={"vacant_share_pct": st.column_config.ProgressColumn("Vacant share of building area",
                                format="%.1f%%", min_value=0, max_value=100)})


# ------------------------------------------------------------------------------ opportunities
def render_opportunities():
    page_title("GUIDED INVESTIGATION", "Estate opportunity explorer",
               "Properties ranked by the investigation priority score (vacancy, vacancy duration, surplus, consolidation "
               "and reuse, scale and data uncertainty). A prompt for investigation, not a recommendation.")
    c1, c2 = st.columns(2)
    with c1:
        bands = st.multiselect("Priority band", ["Very High", "High", "Medium", "Low", "Very Low"],
                               default=["Very High", "High", "Medium"], key="op_bands")
    with c2:
        regs = region_filter("op_regions")
    f = da.F()
    df = da.table(cfg.PRIORITY_TABLE_FQ)
    if bands:
        df = df.filter(f.col("priority_band").isin(bands))
    if regs:
        df = df.filter(f.col("region").isin(list(regs)))
    pdf = (df.select("insite_property_reference", "property_name", "town", "region", "investigation_priority_score",
                     "priority_band", "total_vacant_area_sqm", "status_of_sale", "priority_explanation")
           .orderBy(f.desc("investigation_priority_score")).limit(500).toPandas())
    bc = pdf["priority_band"].value_counts()
    hbar(pd.DataFrame({"label": bc.index, "value": bc.values}), "Properties by priority band", "Current selection")
    st.dataframe(pdf, hide_index=True, use_container_width=True,
                 column_config={"investigation_priority_score": st.column_config.ProgressColumn(
                     "Priority score", format="%.1f", min_value=0, max_value=100)})


# ------------------------------------------------------------------------------ Vacant space finder (GovMove postcode search)
@st.cache_resource(show_spinner="Loading property data...")
def _finder_frame():
    f = da.F()
    df = (da.table(cfg.PROPERTY_TABLE_FQ)
          .withColumn("bng_easting", f.col("bng_easting").cast("double"))
          .withColumn("bng_northing", f.col("bng_northing").cast("double"))
          .withColumn("available_area_sqm", f.col("available_area_sqm").cast("double")))
    try:
        cached = df.cache()
        cached.count()
        return cached
    except Exception:
        return df  # serverless compute does not support cache(); the source table is small


@st.cache_data(ttl=3600, show_spinner=False)
def _finder_options():
    return _finder_frame().groupBy("dataset", "availability_status", "region", "local_authority").count().toPandas()


def _label(key):
    return cfg.AVAILABILITY_RULES.get(key, {}).get("label", key)


def render_vacancy_finder():
    page_title("GUIDED INVESTIGATION", "Vacant space finder",
               "Find the nearest available government properties to a UK postcode.")
    st.info(DISCLAIMER)
    options = _finder_options()
    available = [d for d in cfg.AVAILABILITY_RULES if d in set(options["dataset"])] or sorted(options["dataset"].unique())
    c1, c2, c3 = st.columns(3)
    with c1:
        postcode_input = st.text_input("UK postcode (required)", placeholder="e.g. SW1A 2AA", max_chars=12, key="vf_pc")
        datasets = st.multiselect("Property dataset", available, key="vf_ds", format_func=_label,
                                  default=[cfg.DEFAULT_DATASET] if cfg.DEFAULT_DATASET in available else available[:1])
    with c2:
        status_opts = sorted(options.loc[options["dataset"].isin(datasets), "availability_status"].dropna().unique())
        default_st = sorted({s for d in datasets for s in cfg.AVAILABILITY_RULES.get(d, {}).get("default_statuses", [])
                             if s in status_opts})
        statuses = st.multiselect("Availability status", status_opts, default=default_st, key="vf_st",
                                  help="Defaults to available properties only. Select other statuses explicitly to include them.")
        max_miles = st.number_input("Maximum distance (miles, optional)", min_value=0.0, value=None, step=5.0, key="vf_mi")
        min_area = st.number_input("Minimum available area (sqm, optional)", min_value=0.0, value=None, step=50.0, key="vf_ar")
    with c3:
        reg_opts = sorted(options.loc[options["dataset"].isin(datasets), "region"].dropna().unique())
        regions = st.multiselect("Region (optional)", reg_opts, key="vf_rg")
        mask = options["dataset"].isin(datasets) & (options["region"].isin(regions) if regions else True)
        las = st.multiselect("Local authority (optional)", sorted(options.loc[mask, "local_authority"].dropna().unique()), key="vf_la")
        n_results = st.number_input("Number of results", min_value=1, max_value=cfg.MAX_RESULTS, value=cfg.DEFAULT_RESULTS,
                                    step=1, key="vf_n")
    if st.button("Search", type="primary", key="vf_go"):
        st.session_state["vf_params"] = dict(postcode=postcode_input, datasets=datasets, statuses=statuses, max_miles=max_miles,
                                             min_area=min_area, regions=regions, las=las, n=int(n_results))
    p = st.session_state.get("vf_params")
    if not p:
        return
    if not normalise_postcode(p["postcode"]):
        st.error("Please enter a postcode.")
        return
    if not p["datasets"]:
        st.error("Please select at least one property dataset.")
        return
    if not p["statuses"]:
        st.error("Please select at least one availability status.")
        return
    loc = da.lookup_postcode(p["postcode"])
    if loc["lookup_status"] != "found":
        titles = {"blank": "Blank postcode", "invalid_format": "Invalid postcode", "not_found": "Postcode not found",
                  "missing_coordinates": "Missing postcode coordinates", "crs_mismatch": "Coordinate reference system mismatch",
                  "error": "Lookup error"}
        st.error(f"**{titles.get(loc['lookup_status'], 'Postcode problem')}.** {loc['message']}")
        return
    if loc["coordinate_reference_system"] != BNG_CRS:
        st.error(f"**Coordinate reference system mismatch.** Postcode is in {loc['coordinate_reference_system']}, "
                 f"properties are in {BNG_CRS}.")
        return
    f = da.F()
    with st.spinner("Searching properties..."):
        filtered = filter_properties(_finder_frame(), p["datasets"], p["statuses"], p["regions"], p["las"], p["min_area"])
        bad = filtered.filter(~(f.col("is_comparable") & valid_bng_condition("bng_easting", "bng_northing",
                                                                              cfg.BNG_EASTING_RANGE, cfg.BNG_NORTHING_RANGE)))
        bad_pdf = bad.select("record_reference", "property_name", "full_address", "availability_status", "coordinate_status",
                             "coordinate_note").limit(200).toPandas()
        bad_count = bad.count() if len(bad_pdf) >= 200 else len(bad_pdf)
        res_sdf, scored = search_nearest(filtered, loc["easting"], loc["northing"], p["max_miles"], p["n"],
                                         cfg.BNG_EASTING_RANGE, cfg.BNG_NORTHING_RANGE)
        res = res_sdf.toPandas()
        matching = scored.count()
    st.markdown("#### Search summary")
    m = st.columns(4)
    with m[0]:
        card("Searched postcode", loc["normalised_postcode"])
    with m[1]:
        card("Easting / Northing", f"{loc['easting']:,.0f} / {loc['northing']:,.0f}", BNG_CRS)
    with m[2]:
        card("Matching properties", f"{matching:,}")
    with m[3]:
        card("Nearest property", f"{res.iloc[0]['distance_miles']:.2f} miles" if len(res) else "None",
             res.iloc[0]["property_name"] if len(res) else "")
    if bad_count:
        st.warning(f"{bad_count:,} matching record(s) were excluded because their coordinates are missing, zero, outside the "
                   "valid range, or in a coordinate system that cannot be compared reliably.")
    if res.empty:
        within = f" within {p['max_miles']:g} miles" if p["max_miles"] else ""
        st.warning(f"No available properties were found{within} of {loc['normalised_postcode']} with the selected filters.")
    else:
        out = res.rename(columns={
            "property_reference": "Property reference", "record_reference": "Record reference", "dataset": "Dataset",
            "property_name": "Property name", "full_address": "Full address", "postcode": "Property postcode",
            "region": "Region", "local_authority": "Local authority", "availability_status": "Availability status",
            "available_area": "Available area", "area_units": "Area units", "available_start_date": "Available start date",
            "bng_easting": "Easting", "bng_northing": "Northing", "source_crs": "Coordinate source",
            "distance_miles": "Distance (miles)", "distance_km": "Distance (km)"})
        out["Dataset"] = out["Dataset"].map(_label)
        cols = ["Property reference", "Property name", "Full address", "Property postcode", "Region", "Local authority",
                "Availability status", "Available area", "Area units", "Available start date", "Easting", "Northing",
                "Distance (miles)", "Distance (km)", "Dataset", "Record reference", "Coordinate source"]
        st.dataframe(out[cols], hide_index=True, use_container_width=True,
                     column_config={"Easting": st.column_config.NumberColumn(format="%.0f"),
                                    "Northing": st.column_config.NumberColumn(format="%.0f"),
                                    "Distance (miles)": st.column_config.NumberColumn(format="%.2f"),
                                    "Distance (km)": st.column_config.NumberColumn(format="%.2f")})
        st.download_button("Download results as CSV", out[cols].to_csv(index=False).encode("utf-8"),
                           file_name=f"govmove_{loc['normalised_postcode'].replace(' ', '')}.csv", mime="text/csv")
        pts = res.dropna(subset=["map_latitude", "map_longitude"]).rename(
            columns={"map_latitude": "latitude", "map_longitude": "longitude"})
        pts["town"] = pts["distance_miles"].map(lambda v: f"{v:.2f} miles")
        max_d = float(res["distance_miles"].max() or 1)
        point_map(pts, (29, 112, 184), radius=250, zoom=12 if max_d < 2 else 10 if max_d < 10 else 8 if max_d < 40 else 6,
                  origin={"latitude": loc["latitude"], "longitude": loc["longitude"],
                          "label": f"Searched postcode {loc['normalised_postcode']}"})
        st.caption("Red: searched postcode. Blue: matching properties. " + DISCLAIMER)
    if bad_count:
        with st.expander(f"Records that could not be compared ({bad_count:,})"):
            st.dataframe(bad_pdf, hide_index=True, use_container_width=True)


TASK_VIEWS = {
    "space": render_find_space,
    "clusters": lambda: render_colocation(key="pl"),
    "utilisation": render_utilisation,
    "footprint": render_footprint,
    "vacancy": render_vacancy_finder,
    "opportunities": render_opportunities,
    "prioritisation": render_prioritisation,
}
