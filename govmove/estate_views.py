"""Estate Intelligence views (explore and understand the estate)."""
import pandas as pd
import plotly.express as px
import pydeck as pdk
import streamlit as st

import config as cfg
import data_access as da

BLUE, GREEN, AMBER, NAVY = "#1d70b8", "#00703c", "#f47738", "#0b0c0c"
CT = cfg.CLEAN_TABLES
LOC = cfg.PROPERTY_TABLE_FQ
EV = cfg.EVIDENCE_TABLE_FQ

DATASETS = [
    ("property", "Property and Contracts", "The primary entity and contractual interests at a location."),
    ("buildings", "Buildings", "Physical buildings associated with a property."),
    ("land", "Land", "Land associated with a property and land-only assets."),
    ("occupation", "Occupation", "Organisations occupying space at a property."),
    ("vacant", "Vacant Space", "Recorded available space within the estate."),
    ("surplus", "Surplus Property", "Property and land identified through the surplus process."),
]


# ------------------------------------------------------------------------------ shared widgets
def page_title(kicker, title, text=None):
    st.markdown(f"<div class='gm-kicker'>{kicker}</div><h2 class='gm-title'>{title}</h2>", unsafe_allow_html=True)
    if text:
        st.markdown(f"<p class='gm-lead'>{text}</p>", unsafe_allow_html=True)


def card(label, value, sub=""):
    st.markdown(f"<div class='gm-card'><div class='gm-card-label'>{label}</div>"
                f"<div class='gm-card-value'>{value}</div><div class='gm-card-sub'>{sub}</div></div>",
                unsafe_allow_html=True)


def fmt(v, dp=0):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    return f"{v:,.{dp}f}"


def hbar(df, title, subtitle="", color=BLUE, height=None, value_label="Records"):
    st.markdown(f"**{title}**" + (f"  \n<span class='gm-muted'>{subtitle}</span>" if subtitle else ""),
                unsafe_allow_html=True)
    if df is None or df.empty:
        st.info("No records for the current selection.")
        return
    fig = px.bar(df, x="value", y="label", orientation="h", labels={"value": value_label, "label": ""})
    fig.update_traces(marker_color=color)
    fig.update_layout(height=height or max(220, 26 * len(df) + 60), margin=dict(l=0, r=10, t=10, b=10),
                      yaxis={"categoryorder": "total ascending"}, plot_bgcolor="white", paper_bgcolor="white")
    st.plotly_chart(fig, use_container_width=True)


def point_map(pdf, color=(29, 112, 184), radius=600, origin=None, zoom=None, tooltip="{property_name}\n{town}"):
    """Latitude/longitude are used for display only."""
    if pdf is None or pdf.empty:
        st.info("No records with map positions for the current selection.")
        return
    layers = [pdk.Layer("ScatterplotLayer", pdf, get_position="[longitude, latitude]", get_radius=radius,
                        radius_min_pixels=3, get_fill_color=list(color) + [180], pickable=True)]
    lat, lon, z = 54.5, -3.0, 4.6
    if origin and origin.get("latitude") is not None:
        o = pd.DataFrame([{"latitude": origin["latitude"], "longitude": origin["longitude"],
                           "property_name": origin.get("label", "Search location"), "town": ""}])
        layers.append(pdk.Layer("ScatterplotLayer", o, get_position="[longitude, latitude]", get_radius=radius * 1.5,
                                radius_min_pixels=8, get_fill_color=[212, 53, 28, 230], pickable=True))
        lat, lon, z = origin["latitude"], origin["longitude"], zoom or 10
    st.pydeck_chart(pdk.Deck(layers=layers, map_style=None, tooltip={"text": tooltip},
                             initial_view_state=pdk.ViewState(latitude=lat, longitude=lon, zoom=z)))


def region_filter(key):
    return tuple(st.multiselect("Region", da.regions(), key=key, placeholder="All regions"))


# ------------------------------------------------------------------------------ overview
def render_overview():
    page_title("CONNECTED ESTATE DATA", "Government Estate Intelligence Platform",
               "Understand government property, occupation, space utilisation and portfolio opportunities "
               "through connected estate records.")
    k = da.scalar_aggs(EV, (("properties", "count(*)"), ("buildings", "sum(buildings)"), ("land", "sum(land_records)"),
                            ("occupation", "sum(occupation_records)"), ("vacant", "sum(vacant_records)"),
                            ("surplus", "sum(surplus_records)"), ("departments", "count(DISTINCT holding_department)"),
                            ("vacant_area", "sum(vacant_available_area)"), ("mapped", "count_if(has_bng)")))
    occ = da.scalar_aggs(CT["occupation"], (("orgs", "count(DISTINCT organisation)"),), da.NOT_DUPLICATE)
    st.success("All six Government Property Estate source datasets are connected. Related records are joined "
               "only on exact Insite Property Reference.")
    st.markdown("<div class='gm-kicker'>SOURCE DATASETS</div>", unsafe_allow_html=True)
    cols = st.columns(6)
    keys = ["properties", "buildings", "land", "occupation", "vacant", "surplus"]
    for c, (_, name, desc), key in zip(cols, DATASETS, keys):
        with c:
            card(name, fmt(k.get(key)), desc)
    c1, c2, c3 = st.columns(3)
    with c1:
        card("Contracted organisations", fmt(k.get("departments")), "Distinct holding (parent) organisations")
    with c2:
        card("Occupying organisations", fmt(occ.get("orgs")), "Distinct Organisation values in Occupation")
    with c3:
        card("Available vacant area", f"{fmt(k.get('vacant_area'))} sqm", "Vacant Space Status = Available")

    st.markdown("### National estate map")
    st.caption("United Kingdom → Region → Local Authority → Town → Property → Building")
    m1, m2 = st.columns([1, 3])
    with m1:
        layer = st.radio("Layer", ["Properties", "Occupation", "Vacant space", "Surplus"], key="ov_layer")
        regs = region_filter("ov_regions")
    flag = {"Properties": None, "Occupation": "occupation_records", "Vacant space": "vacant_records",
            "Surplus": "surplus_records"}[layer]
    colour = {"Properties": (29, 112, 184), "Occupation": (0, 112, 60), "Vacant space": (244, 119, 56),
              "Surplus": (11, 12, 12)}[layer]
    pts = da.map_points(flag, regs)
    with m2:
        point_map(pts, colour, radius=500 if flag is None else 1500)
        st.caption(f"Records displayed: {len(pts):,}" + (" (first 20,000 by reference)" if len(pts) >= 20000 else "")
                   + ". Sampled source records with valid UK coordinates.")
    a, b, c = st.columns(3)
    with a:
        hbar(da.group_count(EV, "region", regions=regs, region_col="region"), "Records by region", "Properties")
    with b:
        hbar(da.group_count(EV, "usage", regions=regs, region_col="region"), "Records by usage", "Values as recorded in source")
    with c:
        hbar(da.group_count(EV, "holding_department", regions=regs, region_col="region"), "Records by organisation",
             "Contract Parent Organisation")

    st.markdown("### Connected estate data model")
    st.caption("Property is the primary entity; related records retain their source-level granularity.")
    st.markdown("`Property` → 1 : many → `Contracts` · `Buildings` · `Land` · `Occupations` · `Vacant Space` · "
                "`Surplus Property` (related by property)")


# ------------------------------------------------------------------------------ property explorer
def render_property_explorer():
    page_title("ESTATE INTELLIGENCE", "Property explorer",
               "Property Reference is used to relate property records to Buildings, Land, Occupation, Vacant Space "
               "and Surplus Property records. Repeated locations may represent distinct government interests.")
    c1, c2 = st.columns([3, 2])
    with c1:
        q = st.text_input("Search", placeholder="Search property reference, name, town or postcode", key="pe_q",
                          value=st.session_state.get("header_search", ""))
    with c2:
        regs = region_filter("pe_regions")
    res = da.search_evidence(q, regs)
    st.caption(f"Showing {len(res):,} propert{'y' if len(res) == 1 else 'ies'}" + (" (first 200)" if len(res) >= 200 else ""))
    cols = ["property_reference", "property_name", "town", "postcode", "region", "holding_department", "buildings",
            "occupiers", "vacant_records", "land_records", "surplus_records"]
    st.dataframe(res[cols], hide_index=True, use_container_width=True, height=320)
    if res.empty:
        return
    ref = st.selectbox("Select a connected property to inspect source-level records", res["property_reference"],
                       format_func=lambda r: f"{r} — {res.loc[res.property_reference == r, 'property_name'].iloc[0]}",
                       key="pe_ref")
    render_property_detail(res.loc[res.property_reference == ref].iloc[0])


def render_property_detail(p):
    st.markdown(f"### {p['property_name'] or p['property_reference']}")
    st.caption(f"{p['property_reference']} · {p['full_address'] or ''} · {p['region'] or ''}")
    cols = st.columns(5)
    for c, (lab, key) in zip(cols, [("Buildings", "buildings"), ("Occupations", "occupation_records"),
                                     ("Vacancy", "vacant_records"), ("Land", "land_records"), ("Surplus", "surplus_records")]):
        with c:
            card(lab, fmt(p[key]))
    st.markdown(f"**Holding Department:** {p['holding_department'] or 'Not recorded'} · **Occupying organisations:** "
                f"{p['occupier_names'] or 'None recorded'}")
    recs = da.related_records(p["property_reference"])
    tabs = st.tabs(list(recs))
    for t, (name, df) in zip(tabs, recs.items()):
        with t:
            if df.empty:
                st.info(f"No {name.lower()} records for this property.")
            else:
                st.dataframe(df, hide_index=True, use_container_width=True)


# ------------------------------------------------------------------------------ dataset analytics
def render_buildings():
    page_title("ESTATE INTELLIGENCE", "Building analytics")
    regs = region_filter("bd_regions")
    t, w = CT["building"], da.NOT_DUPLICATE
    k = da.scalar_aggs(t, (("n", "count(*)"), ("area", "sum(try_cast(floor_area AS DOUBLE))"),
                           ("listed", "count_if(listed_building_flag)"), ("epc", "count(epc_band)")), w)
    c = st.columns(4)
    for col, (lab, v) in zip(c, [("Buildings", k.get("n")), ("Recorded floor area", k.get("area")),
                                 ("Listed buildings", k.get("listed")), ("With an EPC band", k.get("epc"))]):
        with col:
            card(lab, fmt(v))
    a, b = st.columns(2)
    with a:
        hbar(da.group_count(t, "region_clean", w, regs), "Buildings by region")
        hbar(da.group_count(t, "epc_band", w, regs), "EPC band", "EPC band field", GREEN)
    with b:
        hbar(da.group_count(t, "usage", w, regs), "Buildings by usage", "Usage field")
        hbar(da.group_count(t, "listed_building", w, regs), "Listed building status", "Listed Building field", AMBER)


def render_land():
    page_title("ESTATE INTELLIGENCE", "Land analytics")
    regs = region_filter("ld_regions")
    t, w = CT["land"], da.NOT_DUPLICATE
    k = da.scalar_aggs(t, (("n", "count(*)"), ("ha", "sum(try_cast(land_area_hectares AS DOUBLE))")), w)
    c1, c2 = st.columns(2)
    with c1:
        card("Land records", fmt(k.get("n")))
    with c2:
        card("Total land area", f"{fmt(k.get('ha'))} ha", "Derived land_area_hectares")
    a, b = st.columns(2)
    with a:
        hbar(da.group_count(t, "region_clean", w, regs), "Land records by region")
        hbar(da.group_count(t, "conservation_area", w, regs), "Conservation area", "Conservation Area field", GREEN)
    with b:
        hbar(da.group_count(t, "usage", w, regs), "Land usage", "Usage field")
        hbar(da.group_count(t, "region_clean", w, regs, value="land_area_hectares"), "Land area by region",
             "Hectares", AMBER, value_label="Hectares")


def render_occupation():
    page_title("ESTATE INTELLIGENCE", "Occupation analytics",
               "Follow recorded relationships from an organisation to the property holding structure. Source relationships only.")
    regs = region_filter("oc_regions")
    t, w = CT["occupation"], da.NOT_DUPLICATE
    a, b = st.columns(2)
    with a:
        hbar(da.group_count(t, "organisation", w, regs), "Occupation records by organisation", "Top Organisation values")
        hbar(da.group_count(t, "agreement_type", w, regs), "Occupation agreement type", "Agreement Type field", GREEN)
    with b:
        hbar(da.group_count(t, "region_clean", w, regs), "Occupation records by region", "Region field")
        hbar(da.group_count(t, "usage", w, regs), "Occupation usage", "Usage field", AMBER)
    st.markdown("### Multi-occupier property evidence")
    st.caption("Real records from the Occupation extract: properties with more than one occupying organisation.")
    f = da.F()
    df = da.table(EV).filter(f.col("occupiers") > 1)
    if regs:
        df = df.filter(f.col("region").isin(list(regs)))
    multi = (df.select("property_reference", "property_name", "town", "region", "holding_department", "occupiers",
                       "occupier_names", "occupied_area", "vacant_available_area").orderBy(f.desc("occupiers")).limit(200).toPandas())
    st.dataframe(multi, hide_index=True, use_container_width=True)
    st.markdown("### Occupation footprint")
    point_map(da.map_points("occupation_records", regs), (0, 112, 60), radius=1500)


def _area_band(col):
    return (f"CASE WHEN {col} IS NULL THEN 'Not recorded' WHEN {col} < 100 THEN 'Under 100 sqm' "
            f"WHEN {col} < 500 THEN '100–499 sqm' WHEN {col} < 1000 THEN '500–999 sqm' "
            f"WHEN {col} < 5000 THEN '1,000–4,999 sqm' ELSE '5,000 sqm and over' END")


def _with_department(dataset, regs):
    """Vacant/surplus records joined (exact reference) to the property holding department."""
    f = da.F()
    df = da.table(LOC).filter(f.col("dataset") == dataset)
    if regs:
        df = df.filter(f.col("region").isin(list(regs)))
    ev = da.table(EV).select(f.col("property_reference"), "holding_department")
    return df.join(ev, "property_reference", "left")


@st.cache_data(ttl=3600, show_spinner=False)
def _dataset_breakdowns(dataset, regs):
    f = da.F()
    df = _with_department(dataset, regs).withColumn("area_band", f.expr(_area_band("available_area_sqm")))
    out = {}
    for key in ("region", "holding_department", "availability_status", "area_band"):
        lab = f.coalesce(f.col(key), f.lit("Not recorded"))
        out[key] = (df.groupBy(lab.alias("label")).agg(f.count(f.lit(1)).alias("value"))
                    .orderBy(f.desc("value")).limit(15).toPandas())
    pts = (df.filter(f.col("map_latitude").isNotNull())
           .select("property_name", "town", "availability_status", f.col("map_latitude").alias("latitude"),
                   f.col("map_longitude").alias("longitude")).toPandas())
    tot = df.agg(f.count(f.lit(1)).alias("n"), f.sum("available_area_sqm").alias("area")).first().asDict()
    return out, pts, tot


def render_vacant():
    page_title("ESTATE INTELLIGENCE", "Vacant space analytics", "Available government space and regional concentrations.")
    regs = region_filter("vs_regions")
    out, pts, tot = _dataset_breakdowns("vacant_space", regs)
    c1, c2 = st.columns(2)
    with c1:
        card("Vacant space records", fmt(tot["n"]))
    with c2:
        card("Recorded vacant area", f"{fmt(tot['area'])} sqm", "All statuses; IPMS basis assumed sqm")
    st.markdown("### Vacant space map")
    point_map(pts, (244, 119, 56), radius=2000, tooltip="{property_name}\n{town}\n{availability_status}")
    a, b = st.columns(2)
    with a:
        hbar(out["region"], "Vacant records by region")
        hbar(out["availability_status"], "Vacant space status", "Vacant Space Status field", GREEN)
    with b:
        hbar(out["holding_department"], "Vacant space by organisation", "Contract Parent Organisation of the property")
        hbar(out["area_band"], "Vacant space by area band", "Derived from Vacant Area", AMBER)
    st.caption("The Vacant Space extract does not contain a building type field. Building usage is therefore not "
               "assigned to vacant records because a property can contain multiple buildings and such a join may be ambiguous.")


def render_surplus():
    page_title("ESTATE INTELLIGENCE", "Surplus property analytics", "Geographic distribution of source surplus records.")
    regs = region_filter("sp_regions")
    out, pts, tot = _dataset_breakdowns("surplus", regs)
    c1, c2 = st.columns(2)
    with c1:
        card("Surplus records", fmt(tot["n"]))
    with c2:
        card("Surplus floor area", f"{fmt(tot['area'])} sqm", "Total Surplus Floor Area (assumed sqm)")
    st.markdown("### Surplus property map")
    point_map(pts, (11, 12, 12), radius=2000, tooltip="{property_name}\n{town}\n{availability_status}")
    a, b = st.columns(2)
    with a:
        hbar(out["region"], "Surplus records by region")
        hbar(out["availability_status"], "Status of sale", "Status of Sale field", GREEN)
    with b:
        hbar(out["holding_department"], "Surplus records by organisation", "Contract Parent Organisation of the property")
        hbar(out["area_band"], "Surplus floor area band", "Derived from Total Surplus Floor Area", AMBER)


# ------------------------------------------------------------------------------ co-location
def render_colocation(key="cl"):
    page_title("ESTATE INTELLIGENCE", "Co-location opportunity explorer",
               "Evidence, not a recommendation. Potential opportunities require validation against operational, "
               "contractual and organisational requirements.")
    c1, c2, c3 = st.columns([3, 2, 1])
    with c1:
        loc_text = st.text_input("Location", placeholder="Town, postcode or property reference", key=f"{key}_loc")
    with c2:
        radius = st.radio("Radius", [1, 5, 10, 25], index=1, horizontal=True, key=f"{key}_radius",
                          format_func=lambda m: f"{m} mile" + ("s" if m > 1 else ""))
    with c3:
        st.write("")
        apply = st.button("Apply", type="primary", key=f"{key}_apply")
    if apply:
        st.session_state[f"{key}_params"] = (loc_text, radius)
    params = st.session_state.get(f"{key}_params")
    if not params:
        st.info("Enter a town, postcode or property reference and select Apply.")
        return
    loc = da.resolve_location(params[0])
    if loc["status"] != "found":
        st.error(loc["message"])
        return
    near = da.nearby_properties(loc["easting"], loc["northing"], params[1])
    multi = near[near.occupiers > 1]
    vac = near[near.vacant_available_records > 0]
    both = near[(near.occupiers > 0) & (near.vacant_available_records > 0)]
    st.markdown(f"#### Evidence flags within {params[1]} miles of {loc['label']} ({loc['kind']})")
    st.caption("Generated from exact connected records. Straight-line distance on British National Grid.")
    c = st.columns(4)
    for col, (lab, v) in zip(c, [("Properties in radius", len(near)), ("Multiple occupiers recorded", len(multi)),
                                 ("Vacant space recorded", len(vac)), ("Potential for further investigation", len(both))]):
        with col:
            card(lab, fmt(v))
    st.markdown("#### Occupied versus available space")
    st.caption("Whole-radius totals; not a recommendation or a like-for-like utilisation measure.")
    o1, o2 = st.columns(2)
    with o1:
        card("Occupation Floor Area", f"{fmt(near['occupied_area'].sum())}")
    with o2:
        card("Vacant Area (available)", f"{fmt(near['vacant_available_area'].sum())} sqm")
    st.markdown("#### Properties with occupation and vacant space records")
    st.caption("Evidence joined only on exact Insite Property Reference.")
    cols = ["property_reference", "property_name", "town", "distance_miles", "holding_department", "occupiers",
            "occupier_names", "occupied_area", "vacant_available_area", "surplus_records"]
    show = near[(near.occupiers > 0) | (near.vacant_records > 0)]
    st.dataframe(show[cols], hide_index=True, use_container_width=True)
    point_map(near, (29, 112, 184), radius=150, origin=loc, zoom=11 if params[1] <= 5 else 9)


# ------------------------------------------------------------------------------ data quality
QUALITY_CHECKS = {
    "Property and Contracts": (CT["property"], da.NOT_DUPLICATE, "insite_property_reference", "has_valid_coordinates",
                               "try_cast(floor_area AS DOUBLE) > 0", "usage"),
    "Buildings": (CT["building"], da.NOT_DUPLICATE, "insite_property_reference", "coalesce(latitude, 0) <> 0",
                  "try_cast(floor_area AS DOUBLE) > 0", "usage"),
    "Land": (CT["land"], da.NOT_DUPLICATE, "insite_property_reference", "has_valid_coordinates",
             "try_cast(land_area_hectares AS DOUBLE) > 0", "usage"),
    "Occupation": (CT["occupation"], da.NOT_DUPLICATE, "insite_property_reference", "has_valid_coordinates",
                   "try_cast(floor_area AS DOUBLE) > 0", "usage"),
    "Vacant Space": (LOC, "dataset = 'vacant_space'", "property_reference", "is_comparable", "available_area_sqm > 0", None),
    "Surplus Property": (LOC, "dataset = 'surplus'", "property_reference", "is_comparable", "available_area_sqm > 0", None),
}


def render_quality():
    page_title("ESTATE INTELLIGENCE", "Data quality workspace", "Across 6 connected extracts.")
    rows = []
    for name, (tbl, where, ref, loc, area, usage) in QUALITY_CHECKS.items():
        ref_unique = "record_reference" if tbl != LOC else "record_reference"
        exprs = (("records", "count(*)"), ("ref", f"avg(CASE WHEN {ref} IS NOT NULL THEN 1 ELSE 0 END)"),
                 ("loc", f"avg(CASE WHEN {loc} THEN 1 ELSE 0 END)"), ("area", f"avg(CASE WHEN {area} THEN 1 ELSE 0 END)"),
                 ("unique", f"count(DISTINCT {ref_unique}) / count(*)"))
        if usage:
            exprs += (("usage", f"avg(CASE WHEN nullif(trim({usage}), '') IS NOT NULL THEN 1 ELSE 0 END)"),)
        r = da.scalar_aggs(tbl, exprs, where)
        checks = [r["ref"], r["loc"], r["area"], r["unique"]] + ([r["usage"]] if usage else [])
        rows.append({"Dataset": name, "Connection status": "Connected", "Records": int(r["records"]),
                     "Property Reference %": 100 * r["ref"], "Valid location %": 100 * r["loc"],
                     "Area recorded %": 100 * r["area"], "Usage recorded %": (100 * r["usage"]) if usage else None,
                     "Unique source reference %": 100 * r["unique"],
                     "Completeness %": 100 * sum(checks) / len(checks), "Dataset refresh date": "Not supplied in extract"})
    df = pd.DataFrame(rows)
    st.markdown("### Data completeness by dataset")
    st.caption("Calculated across Property Reference, location, area, usage and unique source reference checks. "
               "Usage is not available in the Vacant Space and Surplus extracts.")
    pct = {c: st.column_config.ProgressColumn(c, format="%.1f%%", min_value=0, max_value=100)
           for c in df.columns if c.endswith("%")}
    st.dataframe(df, hide_index=True, use_container_width=True, column_config=pct)
    st.markdown("#### Government Property Data Standard")
    st.caption("The workspace preserves Property as the primary entity, exposes source-level related records, and makes "
               "missing identifiers and relationship quality visible rather than inferring unverified links.")


VIEWS = {
    "Estate overview": render_overview,
    "Property explorer": render_property_explorer,
    "Buildings": render_buildings,
    "Land": render_land,
    "Occupation": render_occupation,
    "Vacant space": render_vacant,
    "Surplus property": render_surplus,
    "Co-location": render_colocation,
    "Data quality": render_quality,
}
