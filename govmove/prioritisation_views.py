"""Sustainability & investment prioritisation (Estate Planning & Decision Support task).

Transparent 100-point core property score (components A-F) built from the property-level priority
results table, which links Building, PropertyContract, Occupation, Land, VacantSpace and Surplus on the
exact Insite Property Reference. Proximity to a home postcode is calculated separately and never added
to the core score unless the user asks for a combined ranking.

Privacy: the home postcode is looked up without caching and is not written anywhere. Outputs show only
the postcode district.
"""
import pandas as pd
import streamlit as st

import config as cfg
import data_access as da
import prioritisation_core as pc
from estate_views import card, fmt, hbar, page_title, point_map, region_filter
from govmove_core import BNG_CRS, get_postcode_coordinates, normalise_postcode

PT = cfg.PRIORITY_TABLE_FQ
LOC = cfg.PROPERTY_TABLE_FQ
DISTANCE_NOTE = ("Distances are approximate straight-line distances (Haversine on latitude/longitude, or British "
                 "National Grid easting/northing). They are not road distance, travel time or commuting time.")

CAT_COLS = {
    "Consolidation opportunity": "cat_consolidation",
    "Vacant-space intervention": "cat_vacant",
    "Surplus-property review": "cat_surplus",
    "Disposal acceleration": "cat_disposal",
    "Sustainability investigation": "cat_sustainability",
    "Contract and duplication review": "cat_contract",
    "Data-quality investigation": "cat_dq",
}
NEXT_STEP = {
    "Disposal acceleration": "Confirm disposal route and blockers with the holding organisation; check why the surplus asset is not on the market.",
    "Surplus-property review": "Confirm surplus scope (whole site, building or parcel) and current disposal status.",
    "Consolidation opportunity": "Test whether existing occupiers could consolidate into the recorded vacant space (check usage, security classification, floor-area standard and availability).",
    "Vacant-space intervention": "Validate the vacant space record (area, basis, dates) and agree reuse, letting or exit options.",
    "Sustainability investigation": "Obtain metered energy, EPC/DEC and operating data to test whether unused space could be closed down.",
    "Contract and duplication review": "Review the multiple contracts at this property for duplication or upcoming break/expiry opportunities.",
    "Data-quality investigation": "Ask the data owner to resolve the flagged conflicts before relying on this record.",
    "Local investigation opportunity": "Candidate for a site visit from the home office.",
}


# ------------------------------------------------------------------------------ Spark scoring
def _pts(*pairs):
    """Chained when(condition, points)...otherwise(0)."""
    f = da.F()
    expr = None
    for cond, val in pairs:
        expr = f.when(cond, f.lit(val)) if expr is None else expr.when(cond, f.lit(val))
    return expr.otherwise(f.lit(0))


def scored_frame():
    """Property-level Spark DataFrame with core score components A-F, confidence and category flags."""
    f = da.F()
    c = f.col
    z = lambda n: f.coalesce(c(n).cast("double"), f.lit(0.0))
    b = lambda n: f.coalesce(c(n), f.lit(False))

    # Vacancy records still 'Available' after their availability end date (exact Insite Property Reference join).
    outdated = (da.table(LOC).filter((c("dataset") == "vacant_space") & (c("availability_status") == "Available"))
                .groupBy(c("property_reference").alias("insite_property_reference"))
                .agg(f.sum(f.when(c("available_end_date") < f.current_date(), 1).otherwise(0)).alias("past_end_available_records"),
                     f.max("available_end_date").alias("latest_available_end_date")))
    df = da.table(PT).join(outdated, "insite_property_reference", "left")

    vac, days = z("total_vacant_area_sqm"), z("max_vacancy_days")
    ratio = f.when(c("vacant_area_ratio").isNotNull() & ~b("inconsistent_area_basis_flag"), c("vacant_area_ratio"))
    occ, orgs, contracts = z("occupation_record_count"), z("occupying_organisation_count"), z("contract_count")
    surplus = b("surplus_flag")
    not_mkt, on_mkt, uo = z("surplus_not_on_market_count"), z("surplus_on_market_count"), z("surplus_under_offer_count")
    sfa, sla = z("total_surplus_floor_area_sqm"), z("total_surplus_land_area_hectares")
    dq, past_end = z("dq_issue_count"), z("past_end_available_records")
    possible_conflict = surplus & (occ > 0) & (z("vacant_available_count") > 0)
    has_vac = vac > 0

    df = df.withColumns({
        # A. Vacancy and underutilisation (0-30)
        "a_scale": _pts((vac >= 10000, 12), (vac >= 2500, 9), (vac >= 500, 6), (has_vac, 3)),
        "a_proportion": _pts((ratio >= 0.5, 8), (ratio >= 0.25, 6), (ratio >= 0.1, 4), (ratio > 0, 2)),
        "a_duration": _pts((has_vac & (days >= 1825), 6), (has_vac & (days >= 730), 4), (has_vac & (days >= 365), 2)),
        "a_whole": _pts((has_vac & ((ratio >= 0.9) | b("large_vacant_space_flag")), 4)),
        # B. Surplus and disposal position (0-25)
        "b_surplus": _pts((surplus, 10)),
        "b_not_market": _pts((surplus & (not_mkt > 0), 6)),
        "b_area": _pts((surplus & ((sfa >= 1000) | (sla >= 1)), 5), (surplus & ((sfa > 0) | (sla > 0)), 2)),
        "b_no_progress": _pts((surplus & (on_mkt == 0) & (uo == 0), 4)),
        # C. Consolidation opportunity (0-20)
        "c_occupations": _pts((occ >= 5, 6), (occ >= 2, 4)),
        "c_fragmented": _pts((orgs >= 3, 4), (orgs >= 2, 2)),
        "c_vacant_with_occ": _pts(((occ > 0) & ((vac >= 500) | b("consolidation_opportunity_flag")), 6),
                                  ((occ > 0) & has_vac, 3)),
        "c_contracts": _pts((contracts > 1, 4)),
        # D. Property and contract review (0-10)
        "d_contracts": _pts((contracts >= 3, 4), (contracts > 1, 2)),
        "d_duplicate_ref": _pts((b("property_duplicate_ref_flag"), 3)),
        "d_not_in_register": _pts((~f.coalesce(c("property_in_register"), f.lit(True)), 3)),
        # E. Sustainability opportunity indicator (0-10) - proxy only
        "e_unused_area": _pts((vac >= 1000, 5), (vac >= 250, 3), (has_vac, 1)),
        "e_consolidate_close": _pts(((occ > 0) & has_vac, 3)),
        "e_long_unused": _pts((has_vac & (days >= 365), 2)),
        # F. Data-quality risk (0-5)
        "f_raw": (_pts((dq >= 3, 3), (dq >= 1, 2))
                  + _pts((b("inconsistent_area_basis_flag") | b("location_conflict_flag"), 1))
                  + _pts((past_end > 0, 1)) + _pts((possible_conflict, 1))),
        "possible_status_conflict": possible_conflict,
        "past_end_available_records": past_end,
    })
    df = df.withColumns({
        "score_a": c("a_scale") + c("a_proportion") + c("a_duration") + c("a_whole"),
        "score_b": c("b_surplus") + c("b_not_market") + c("b_area") + c("b_no_progress"),
        "score_c": c("c_occupations") + c("c_fragmented") + c("c_vacant_with_occ") + c("c_contracts"),
        "score_d": c("d_contracts") + c("d_duplicate_ref") + c("d_not_in_register"),
        "score_e": c("e_unused_area") + c("e_consolidate_close") + c("e_long_unused"),
        "score_f": f.least(c("f_raw"), f.lit(5)),
    })
    has_loc = (c("latitude_clean").isNotNull() & c("longitude_clean").isNotNull()) | (c("grid_reference_status") == "gb_valid")
    low = (~has_loc | b("inconsistent_area_basis_flag") | (z("vacant_area_unit_unknown_count") > 0) | (dq >= 3)
           | possible_conflict)
    medium = ((dq >= 1) | (c("vacant_area_ratio_basis") == "property_floor_area_fallback")
              | (z("vacant_missing_start_date_count") > 0) | (past_end > 0) | b("location_conflict_flag"))
    return df.withColumns({
        "core_score": c("score_a") + c("score_b") + c("score_c") + c("score_d") + c("score_e") + c("score_f"),
        "confidence": f.when(low, "Low").when(medium, "Medium").otherwise("High"),
        "cat_consolidation": c("score_c") >= 10,
        "cat_vacant": c("score_a") >= 15,
        "cat_surplus": surplus,
        "cat_disposal": surplus & (not_mkt > 0),
        "cat_sustainability": c("score_e") >= 5,
        "cat_contract": c("score_d") >= 4,
        "cat_dq": (c("score_f") >= 3) | possible_conflict,
    })


def with_distance(df, home, bands, core_weight, combined):
    """Add straight-line distance from the home postcode centroid, proximity band/points and ranking score."""
    f = da.F()
    c = f.col
    lat_ok = c("latitude_clean").isNotNull() & c("longitude_clean").isNotNull() & f.lit(home.get("latitude") is not None)
    bng_ok = (c("grid_reference_status") == "gb_valid") & f.lit(home.get("easting") is not None)
    if home.get("latitude") is not None:
        hlat, hlon = f.radians(f.lit(float(home["latitude"]))), f.radians(f.lit(float(home["longitude"])))
        plat, plon = f.radians(c("latitude_clean")), f.radians(c("longitude_clean"))
        a = (f.pow(f.sin((plat - hlat) / 2), 2) + f.cos(hlat) * f.cos(plat) * f.pow(f.sin((plon - hlon) / 2), 2))
        hav = f.lit(2 * pc.EARTH_RADIUS_MILES) * f.asin(f.sqrt(a))
    else:
        hav = f.lit(None).cast("double")
    if home.get("easting") is not None:
        bng = f.sqrt(f.pow(c("easting_clean") - f.lit(float(home["easting"])), 2)
                     + f.pow(c("northing_clean") - f.lit(float(home["northing"])), 2)) / f.lit(pc.METRES_PER_MILE)
    else:
        bng = f.lit(None).cast("double")
    dist = f.when(lat_ok, hav).when(bng_ok, bng)
    l1, l2, l3 = bands
    prox = (f.when(dist.isNull(), f.lit(None)).when(dist <= l1, 10).when(dist <= l2, 7).when(dist <= l3, 4).otherwise(1))
    df = df.withColumns({
        "distance_miles": f.round(dist, 1),
        "distance_method": f.when(lat_ok, "Haversine (latitude/longitude)")
                            .when(bng_ok, "British National Grid easting/northing").otherwise(pc.UNAVAILABLE),
        "proximity_points": prox,
    })
    w = float(core_weight)
    rank = (c("core_score") * w + f.coalesce(c("proximity_points"), f.lit(0)) * 10 * (1 - w)) if combined else c("core_score")
    return df.withColumn("ranking_score", f.round(rank, 1))


DISPLAY_COLS = [
    "insite_property_reference", "property_name", "town", "postcode", "postcode_outward",
    "postcode_area", "region", "local_authority", "organisation", "parent_organisation", "contract_references",
    "contract_count", "asset_type", "usage", "land_usage", "security_classification", "area_bases",
    "inconsistent_area_basis_flag", "total_vacant_area_sqm", "total_occupied_area_sqm", "property_floor_area_sqm",
    "total_building_floor_area_sqm", "vacant_area_ratio", "vacant_area_ratio_basis", "max_vacancy_days",
    "earliest_vacant_available_date", "latest_available_end_date", "past_end_available_records", "vacant_space_status",
    "vacant_available_count", "occupation_record_count", "occupying_organisation_count", "surplus_flag",
    "status_of_sale", "surplus_not_on_market_count", "total_surplus_floor_area_sqm", "total_surplus_land_area_hectares",
    "linked_land_area_hectares", "has_energy_data", "worst_epc_band", "dq_issue_list", "vacant_area_unit_unknown_count",
    "location_conflict_flag", "property_duplicate_ref_flag", "possible_status_conflict", "latitude_clean",
    "longitude_clean", "map_latitude", "map_longitude",
    "a_scale", "a_proportion", "a_duration", "a_whole", "b_surplus", "b_not_market", "b_area", "b_no_progress",
    "c_occupations", "c_fragmented", "c_vacant_with_occ", "c_contracts", "d_contracts", "d_duplicate_ref",
    "d_not_in_register", "e_unused_area", "e_consolidate_close", "e_long_unused",
    "score_a", "score_b", "score_c", "score_d", "score_e", "score_f", "core_score", "confidence",
    "distance_miles", "distance_method", "proximity_points", "ranking_score",
] + list(CAT_COLS.values())


def _filtered(regions, categories):
    f = da.F()
    df = scored_frame()
    if regions:
        df = df.filter(f.col("region").isin(list(regions)))
    cats = [CAT_COLS[c] for c in categories if c in CAT_COLS]
    if cats:
        cond = f.lit(False)
        for col in cats:
            cond = cond | f.col(col)
        df = df.filter(cond)
    return df


# ------------------------------------------------------------------------------ pandas enrichment
def _list(v):
    if v is None:
        return []
    try:
        return [str(x) for x in list(v)]
    except TypeError:
        return [str(v)]


def _num(v):
    return None if v is None or (isinstance(v, float) and pd.isna(v)) else float(v)


def enrich(pdf, bands, office, home_parts, local_threshold):
    """Add categories, explanation, warnings, evidence, proximity band and recommended next step."""
    if pdf.empty:
        return pdf
    rows = []
    for _, r in pdf.iterrows():
        bases = ", ".join(_list(r["area_bases"])) or "measurement basis not recorded"
        vac, occ_a = _num(r["total_vacant_area_sqm"]), _num(r["total_occupied_area_sqm"])
        dist = _num(r["distance_miles"])
        cats = [k for k, col in CAT_COLS.items() if bool(r[col])]
        if dist is not None and dist <= local_threshold and r["core_score"] >= 40:
            cats.append("Local investigation opportunity")

        why = []
        if r["score_a"]:
            ratio = _num(r["vacant_area_ratio"])
            bits = [f"{fmt(vac)} sqm vacant"]
            if r["a_proportion"] and ratio is not None:
                bits.append(f"{ratio:.0%} of comparable area (basis: {r['vacant_area_ratio_basis']})")
            if r["a_duration"]:
                bits.append(f"longest vacancy {int(r['max_vacancy_days']):,} days")
            why.append(f"A {int(r['score_a'])}/30: " + "; ".join(bits))
        if r["score_b"]:
            bits = ["recorded as surplus", f"sale status: {r['status_of_sale'] or 'not recorded'}"]
            if r["b_not_market"]:
                bits.append(f"{int(r['surplus_not_on_market_count'])} record(s) not on the market")
            why.append(f"B {int(r['score_b'])}/25: " + "; ".join(bits))
        if r["score_c"]:
            why.append(f"C {int(r['score_c'])}/20: {int(r['occupation_record_count'] or 0)} occupation record(s), "
                       f"{int(r['occupying_organisation_count'] or 0)} organisation(s), {int(r['contract_count'] or 0)} contract(s)"
                       + (", with recorded vacant space" if r["c_vacant_with_occ"] else ""))
        if r["score_d"]:
            bits = []
            if r["d_contracts"]:
                bits.append(f"{int(r['contract_count'])} contracts")
            if r["d_duplicate_ref"]:
                bits.append("duplicate property reference")
            if r["d_not_in_register"]:
                bits.append("not found in the property register")
            why.append(f"D {int(r['score_d'])}/10: " + "; ".join(bits))
        if r["score_e"]:
            why.append(f"E {int(r['score_e'])}/10: unused operational floor area is a proxy for avoidable building operation "
                       "(not a measured saving)")
        if r["score_f"]:
            why.append(f"F {int(r['score_f'])}/5: data-quality risk (see warnings)")

        warn = []
        if r["dq_issue_list"]:
            warn.append(str(r["dq_issue_list"]))
        if r["inconsistent_area_basis_flag"]:
            warn.append(f"mixed area measurement bases ({bases}); vacancy proportion not scored")
        if r["vacant_area_ratio_basis"] == "property_floor_area_fallback":
            warn.append("vacancy proportion uses property floor area as the denominator (fallback)")
        if (r["vacant_area_unit_unknown_count"] or 0) > 0:
            warn.append("vacant area unit unknown on some records")
        if (r["past_end_available_records"] or 0) > 0:
            warn.append(f"{int(r['past_end_available_records'])} vacancy record(s) still 'Available' after their end date "
                        "(potentially outdated)")
        if r["possible_status_conflict"]:
            warn.append("possible status conflict: occupied, available-vacant and surplus records at once (scope not established)")
        if r["location_conflict_flag"]:
            warn.append("location attributes conflict with other records")
        if r["distance_method"] == pc.UNAVAILABLE:
            warn.append("no comparable coordinates; proximity unavailable")

        missing = ["energy consumption, carbon, cost and condition data" if not r["has_energy_data"]
                   else "energy consumption, carbon and cost data (only EPC/DEC ratings present)",
                   "contract start/end dates", "occupancy, attendance and desk utilisation"]

        same = []
        if home_parts[1] and r["postcode_outward"] == home_parts[1]:
            same.append("same postcode district")
        elif home_parts[0] and r["postcode_area"] == home_parts[0]:
            same.append("same postcode area")
        if office and office.get("kind") in ("town", "local authority", "region"):
            col = {"town": "town", "local authority": "local_authority", "region": "region"}[office["kind"]]
            if str(r[col] or "").strip().upper() == office["label"].upper():
                same.append(f"same {office['kind']} as home office")

        land = []
        if _num(r["total_surplus_land_area_hectares"]):
            land.append(f"{r['total_surplus_land_area_hectares']:,.2f} ha surplus land")
        if _num(r["linked_land_area_hectares"]):
            land.append(f"{r['linked_land_area_hectares']:,.2f} ha linked land")

        rows.append({
            "Priority category": "; ".join(cats) or "None triggered",
            "Why prioritised": " | ".join(why) or "No scoring factors triggered",
            "Recommended next investigation": " ".join(NEXT_STEP[c] for c in cats[:2]) or "No action indicated.",
            "Data-quality warnings": "; ".join(warn) or "None flagged",
            "Missing evidence": "; ".join(missing),
            "Floor area (recorded)": (f"Vacant {fmt(vac)} sqm; occupied {fmt(occ_a)} sqm; surplus "
                                      f"{fmt(_num(r['total_surplus_floor_area_sqm']))} sqm \u2014 basis: {bases}"),
            "Land area": "; ".join(land) or "Not recorded",
            "Occupation status": (f"Occupied ({int(r['occupation_record_count'])} record(s), "
                                  f"{int(r['occupying_organisation_count'] or 0)} organisation(s))"
                                  if (r["occupation_record_count"] or 0) > 0 else "No occupation records"),
            "Vacancy status": r["vacant_space_status"] or "No vacant space records",
            "Surplus status": "Surplus" if r["surplus_flag"] else "Not recorded as surplus",
            "Sale status": r["status_of_sale"] or ("Not recorded" if r["surplus_flag"] else "n/a"),
            "Core priority": pc.core_priority_label(r["core_score"]),
            "Proximity band": pc.proximity_band(dist, bands),
            "Same area as home": "; ".join(same) or "\u2014",
        })
    return pd.concat([pdf.reset_index(drop=True), pd.DataFrame(rows)], axis=1)


# ------------------------------------------------------------------------------ portfolio aggregates (no home data)
@st.cache_data(ttl=3600, show_spinner=False)
def portfolio(regions):
    f = da.F()
    df = _filtered(regions, [])
    band = (f.when(f.col("core_score") >= 75, "Very high").when(f.col("core_score") >= 60, "High")
            .when(f.col("core_score") >= 40, "Medium").otherwise("Lower"))
    bands = df.groupBy(band.alias("label")).agg(f.count(f.lit(1)).alias("value")).toPandas()
    cats = df.agg(*[f.sum(f.col(col).cast("int")).alias(name) for name, col in CAT_COLS.items()]).first().asDict()
    top_regions = (df.filter(f.col("core_score") >= 40)
                   .groupBy(f.coalesce(f.col("region"), f.lit("Not recorded")).alias("label"))
                   .agg(f.count(f.lit(1)).alias("value")).orderBy(f.desc("value")).limit(12).toPandas())
    vac_by_basis = (df.filter(f.col("total_vacant_area_sqm") > 0)
                    .groupBy(f.coalesce(f.concat_ws(" + ", "area_bases"), f.lit("Not recorded")).alias("Measurement basis"))
                    .agg(f.count(f.lit(1)).alias("Properties"),
                         f.round(f.sum("total_vacant_area_sqm"), 0).alias("Vacant area (sqm)"))
                    .orderBy(f.desc("Vacant area (sqm)")).toPandas())
    dq = (df.filter(f.col("dq_issue_list").isNotNull())
          .select(f.explode(f.split("dq_issue_list", "; ")).alias("label"))
          .groupBy("label").agg(f.count(f.lit(1)).alias("value")).orderBy(f.desc("value")).limit(12).toPandas())
    totals = df.agg(f.count(f.lit(1)).alias("properties"),
                    f.sum(f.col("possible_status_conflict").cast("int")).alias("possible_conflicts"),
                    f.sum(f.col("past_end_available_records")).alias("past_end_records"),
                    f.sum(f.coalesce(f.col("inconsistent_area_basis_flag"), f.lit(False)).cast("int")).alias("mixed_basis"),
                    f.sum(f.coalesce(f.col("has_energy_data"), f.lit(False)).cast("int")).alias("with_energy"),
                    f.sum(((f.col("latitude_clean").isNull()) & (f.col("grid_reference_status") != "gb_valid")).cast("int"))
                    .alias("no_location"),
                    f.sum((f.col("core_score") >= 60).cast("int")).alias("high_or_above")).first().asDict()
    return {"bands": bands, "cats": cats, "regions": top_regions, "vac_by_basis": vac_by_basis, "dq": dq, "totals": totals}


# ------------------------------------------------------------------------------ inputs and location checks
def _home_lookup(postcode):
    """Uncached lookup so the home postcode is not retained in the app's shared cache."""
    return get_postcode_coordinates(postcode, da.get_spark(), cfg.POSTCODE_TABLE_FQ, cfg.POSTCODE_FALLBACK_TABLE,
                                    cfg.BNG_EASTING_RANGE, cfg.BNG_NORTHING_RANGE)


def _office_check(office, home, local_threshold):
    """Compare the home-office location with the home postcode; returns (message, is_conflict)."""
    if not office or office.get("status") != "found":
        return ("Home-office location not supplied or not recognised; postcode used for all calculations.", False)
    if office["kind"] == "region":
        return (f"Home-office location resolved to a region ({office['label']}); this is a broad indicator only and "
                "cannot be checked precisely against the postcode.", False)
    d = pc.bng_miles(office.get("easting"), office.get("northing"), home.get("easting"), home.get("northing"))
    if d is None:
        return ("Home-office location could not be compared with the postcode.", False)
    if d > local_threshold:
        return (f"Inconsistency: the home-office location ({office['label']}, {office['kind']}) is about {d:,.0f} miles "
                "(straight line) from the home postcode. The postcode has been used for all distance calculations.", True)
    return (f"Home-office location ({office['label']}) is consistent with the postcode (about {d:,.1f} miles apart).", False)


def _inputs():
    q = st.text_area("Your question", key="sp_q", height=80,
                     placeholder="For example: Which surplus or under-used properties near our Leeds office should we "
                                 "investigate first for consolidation or disposal?")
    interp = pc.interpret_question(q)
    c1, c2, c3 = st.columns(3)
    with c1:
        office = st.text_input("Home-office location", key="sp_office", placeholder="Town, local authority, region or postcode")
        postcode = st.text_input("Home postcode", key="sp_pc", max_chars=12, placeholder="e.g. LS1 4AP",
                                 help="Used only to calculate approximate straight-line distance from the postcode centroid. "
                                      "Not stored.")
    with c2:
        b1 = st.number_input("Local up to (miles)", min_value=1.0, value=10.0, step=1.0, key="sp_b1")
        b2 = st.number_input("Accessible up to (miles)", min_value=2.0, value=25.0, step=1.0, key="sp_b2")
        b3 = st.number_input("Extended up to (miles)", min_value=3.0, value=50.0, step=5.0, key="sp_b3")
    with c3:
        cats = st.multiselect("Focus categories (from your question; edit as needed)", list(CAT_COLS),
                              default=[c for c in interp["categories"] if c in CAT_COLS], key=f"sp_cats_{hash(q)}")
        regions = region_filter("sp_regions")
        min_score = st.slider("Minimum core score shown", 0, 100, 40, 5, key="sp_min")
    l1, l2 = st.columns(2)
    with l1:
        combined = st.checkbox("Prioritise local options (combined ranking)", value=interp["local_focus"],
                               key=f"sp_comb_{hash(q)}",
                               help="Ranks by a weighted mix of the core score and the proximity score. Core and proximity "
                                    "scores are always shown separately.")
    with l2:
        weight = st.slider("Core property weight in combined ranking", 50, 100, 85, 5, key="sp_w",
                           disabled=not combined, help="Default 85% core / 15% proximity (an adjustable analytical assumption).")
    return dict(question=q, office=office, postcode=postcode, bands=pc.validate_bands((b1, b2, b3)),
                bands_valid=(b1 < b2 < b3), categories=tuple(cats), regions=tuple(regions), min_score=min_score,
                combined=combined, weight=weight / 100, interp=interp)


# ------------------------------------------------------------------------------ page
def render_prioritisation():
    page_title("SUSTAINABILITY & INVESTMENT PRIORITISATION", "Prioritise properties for investigation",
               "Transparent, evidence-based ranking of properties for utilisation, consolidation, vacant-space, disposal, "
               "sustainability and data-quality investigation. A prompt for investigation, not a decision.")
    p = _inputs()
    if st.button("Run prioritisation", type="primary", key="sp_go"):
        st.session_state["sp_params"] = p
    p = st.session_state.get("sp_params")
    if not p:
        st.info("Enter your question, home-office location and home postcode, then run the prioritisation.")
        return
    if not p["bands_valid"]:
        st.warning("Proximity thresholds must increase (Local < Accessible < Extended). Default bands 10/25/50 miles used.")
    bands, local_t = p["bands"], p["bands"][0]

    # Home postcode (used for calculation) and home-office location (checked for consistency).
    home, home_msg = {}, "No home postcode supplied; proximity unavailable."
    pc_norm = normalise_postcode(p["postcode"] or "")
    if pc_norm:
        lk = _home_lookup(pc_norm)
        if lk["lookup_status"] == "found" and lk["coordinate_reference_system"] == BNG_CRS:
            home = {k: lk[k] for k in ("easting", "northing", "latitude", "longitude")}
            home_msg = "Postcode-centroid coordinates (approximate)."
        else:
            home_msg = f"Home postcode could not be used: {lk.get('message', 'lookup failed')} Proximity unavailable."
    home_parts = pc.postcode_parts(pc_norm) if home else ("", "")
    office = da.resolve_location(p["office"]) if (p["office"] or "").strip() else None
    office_msg, office_conflict = (_office_check(office, home, local_t) if home else
                                   ("Home-office location not compared (no usable home postcode).", False))
    if office and office.get("status") == "found" and office["kind"] == "postcode":
        office = None  # postcode-type office: no town/LA/region comparison

    with st.spinner("Scoring properties..."):
        f = da.F()
        base = _filtered(p["regions"], p["categories"])
        scored = with_distance(base, home, bands, p["weight"], p["combined"] and bool(home))
        top = (scored.filter(f.col("core_score") >= p["min_score"])
               .orderBy(f.desc("ranking_score"), f.desc("core_score"), "insite_property_reference")
               .select(*DISPLAY_COLS).limit(20).toPandas())
        n_eligible = scored.filter(f.col("core_score") >= p["min_score"]).count()
        near_high = pd.DataFrame()
        local_cands = pd.DataFrame()
        near_consol = pd.DataFrame()
        if home:
            near_high = (scored.filter((f.col("core_score") >= 60) & f.col("distance_miles").isNotNull())
                         .orderBy("distance_miles").select(*DISPLAY_COLS).limit(10).toPandas())
            local_cands = (scored.filter((f.col("core_score") >= 40) & (f.col("distance_miles") <= local_t))
                           .orderBy(f.desc("core_score")).select(*DISPLAY_COLS).limit(10).toPandas())
            near_consol = (scored.filter((f.col("distance_miles") <= bands[1]) &
                                         (f.col("cat_consolidation") | (f.col("total_vacant_area_sqm") > 0)))
                           .orderBy("distance_miles").select(*DISPLAY_COLS).limit(15).toPandas())
        port = portfolio(p["regions"])
    top = enrich(top, bands, office, home_parts, local_t)
    near_high = enrich(near_high, bands, office, home_parts, local_t)
    local_cands = enrich(local_cands, bands, office, home_parts, local_t)
    near_consol = enrich(near_consol, bands, office, home_parts, local_t)

    # 1. Executive summary
    st.markdown("#### 1. Executive summary")
    m = st.columns(4)
    with m[0]:
        card("Properties assessed", fmt(port["totals"]["properties"]), "after region filter")
    with m[1]:
        card("Meeting minimum score", fmt(n_eligible), f"core score \u2265 {p['min_score']}" +
             (" in focus categories" if p["categories"] else ""))
    with m[2]:
        card("High or very high", fmt(port["totals"]["high_or_above"]), "core score \u2265 60 (all categories)")
    with m[3]:
        card("Nearest high priority", f"{near_high.iloc[0]['distance_miles']:.1f} miles" if len(near_high) else "\u2014",
             (near_high.iloc[0]["property_name"] or "") if len(near_high) else home_msg[:60])
    if top.empty:
        st.warning("No properties meet the minimum score for the selected focus. Lower the minimum score or widen the focus.")
    else:
        lead = top.iloc[0]
        cat_counts = pd.Series([c for s in top["Priority category"] for c in s.split("; ")]).value_counts()
        st.markdown(
            f"The strongest property-based opportunity is **{lead['property_name'] or lead['insite_property_reference']}** "
            f"({lead['insite_property_reference']}, {lead['town'] or 'town not recorded'}) with a core score of "
            f"**{lead['core_score']:.0f}/100** ({lead['Core priority'].lower()}, {lead['confidence'].lower()} confidence). "
            f"Of the {len(top)} properties listed, the most common themes are "
            + ", ".join(f"{k.lower()} ({v})" for k, v in cat_counts.head(3).items()) + ". "
            + (f"Ranking uses a combined score ({p['weight']:.0%} core / {1 - p['weight']:.0%} proximity). "
               if p["combined"] and home else "Ranking uses the core property score only; proximity is shown separately. ")
            + ("Fewer than 20 properties met the evidence threshold, so fewer are shown." if len(top) < 20 else ""))
    if office_conflict:
        st.warning(office_msg)

    # 2. Input and scope
    with st.expander("2. Input and scope", expanded=False):
        st.markdown(
            f"* **Question:** {p['question'] or 'Not supplied'}\n"
            f"* **Interpreted focus:** {', '.join(p['interp']['categories']) or 'all categories'}"
            f"{' (local options explicitly prioritised)' if p['interp']['local_focus'] else ''}\n"
            f"* **Datasets used:** Property/PropertyContract, Building, Occupation, Land, VacantSpace and Surplus, via the "
            f"property-level table `{PT}` (all records linked on exact Insite Property Reference; Land linked via the "
            f"property prefix of the Insite Land Reference), plus vacancy/surplus records in `{LOC}` for availability end dates.\n"
            f"* **Home-office location:** {(p['office'] or 'Not supplied')} \u2014 {office_msg}\n"
            f"* **Home postcode:** district {home_parts[1] or 'n/a'} (full postcode not displayed or stored)\n"
            f"* **Proximity method:** {home_msg} Property side: Haversine where latitude/longitude exist, otherwise "
            f"British National Grid easting/northing. Bands: Local \u2264{bands[0]:g}, Accessible \u2264{bands[1]:g}, "
            f"Extended \u2264{bands[2]:g}, Distant >{bands[2]:g} miles (indicative, not policy).\n"
            "* **Material assumptions:** areas are shown in square metres as normalised by the data-prep notebook with "
            "their recorded IPMS basis; vacancy proportion is only scored where measurement bases are consistent; "
            "vacant space does not by itself mean energy use can be eliminated.")

    # 3. Ranked table
    st.markdown(f"#### 3. Ranked properties ({len(top)})")
    if not top.empty:
        out = top.copy()
        out.insert(0, "Rank", range(1, len(out) + 1))
        cols = {"Rank": "Rank", "core_score": "Core score", "Core priority": "Core priority",
                "proximity_points": "Proximity score", "ranking_score": "Ranking score",
                "property_name": "Property name", "insite_property_reference": "Insite Property Reference",
                "contract_references": "Insite Contract Reference(s)", "town": "Town", "postcode": "Postcode",
                "region": "Region", "local_authority": "Local authority", "organisation": "Contract organisation",
                "Floor area (recorded)": "Floor area (recorded)", "Land area": "Land area",
                "Occupation status": "Occupation status", "Vacancy status": "Vacancy status",
                "Surplus status": "Surplus status", "Sale status": "Sale status",
                "distance_miles": "Distance (miles, straight line)", "Proximity band": "Proximity band",
                "distance_method": "Distance method", "Same area as home": "Same area as home",
                "Priority category": "Priority category", "Why prioritised": "Why prioritised",
                "Recommended next investigation": "Recommended next investigation",
                "Data-quality warnings": "Data-quality warnings", "Missing evidence": "Missing evidence",
                "confidence": "Confidence",
                "score_a": "A Vacancy /30", "score_b": "B Surplus /25", "score_c": "C Consolidation /20",
                "score_d": "D Contract /10", "score_e": "E Sustainability /10", "score_f": "F Data quality /5"}
        table = out[list(cols)].rename(columns=cols)
        st.dataframe(table, hide_index=True, use_container_width=True,
                     column_config={"Core score": st.column_config.ProgressColumn("Core score", format="%.0f",
                                                                                    min_value=0, max_value=100)})
        st.download_button("Download ranked properties as CSV", table.to_csv(index=False).encode("utf-8"),
                           file_name="govmove_prioritisation.csv", mime="text/csv", key="sp_dl")
        for _, r in out.head(5).iterrows():
            with st.container(border=True):
                a, b_, c_, d = st.columns([4, 2, 2, 2])
                a.markdown(f"**{int(r['Rank'])}. {r['property_name'] or r['insite_property_reference']}**  \n"
                           f"{r['insite_property_reference']} \u00b7 {r['town'] or ''} {r['postcode'] or ''}  \n"
                           f"<span class='gm-muted'>{r['Priority category']}</span>", unsafe_allow_html=True)
                b_.markdown(f"<div class='gm-card-label'>CORE SCORE</div>{r['core_score']:.0f}/100 \u00b7 {r['Core priority']}",
                            unsafe_allow_html=True)
                c_.markdown(f"<div class='gm-card-label'>CONFIDENCE</div>{r['confidence']}", unsafe_allow_html=True)
                dist = r["distance_miles"]
                d.markdown(f"<div class='gm-card-label'>DISTANCE (STRAIGHT LINE)</div>"
                           f"{'%.1f miles' % dist if pd.notna(dist) else 'Unavailable'} \u00b7 {r['Proximity band']}",
                           unsafe_allow_html=True)
                with st.expander("Evidence"):
                    st.markdown(
                        f"**Facts (recorded):** {r['Floor area (recorded)']}. {r['Occupation status']}. Vacancy: "
                        f"{r['Vacancy status']}. {r['Surplus status']}; sale status {r['Sale status']}. "
                        f"Contracts: {r['contract_references'] or 'not recorded'}. Usage: {r['usage'] or 'not recorded'}; "
                        f"security classification: {r['security_classification'] or 'not recorded'}.  \n"
                        f"**Calculated values:** core score {r['core_score']:.0f}/100 (A {r['score_a']}, B {r['score_b']}, "
                        f"C {r['score_c']}, D {r['score_d']}, E {r['score_e']}, F {r['score_f']}); distance "
                        f"{'%.1f miles' % dist if pd.notna(dist) else 'unavailable'} ({r['distance_method']}).  \n"
                        f"**Reasonable indicators:** {r['Why prioritised']}  \n"
                        f"**Missing evidence:** {r['Missing evidence']}.  \n"
                        f"**Data-quality warnings:** {r['Data-quality warnings']}.  \n"
                        f"**Recommended next investigation:** {r['Recommended next investigation']}")

    # 4. Nearest high-priority opportunities
    st.markdown("#### 4. Nearest high-priority opportunities")
    if not home:
        st.info(home_msg)
    else:
        st.caption(DISTANCE_NOTE + " Proximity does not establish suitability.")
        short = ["insite_property_reference", "property_name", "town", "region", "distance_miles", "Proximity band",
                 "core_score", "Core priority", "Priority category", "confidence"]
        st.markdown("**High-priority properties (core \u2265 60) by distance**")
        st.dataframe(near_high[short] if len(near_high) else near_high, hide_index=True, use_container_width=True)
        st.markdown(f"**Candidates for local investigation or site visits (core \u2265 40, within {local_t:g} miles)**")
        if local_cands.empty:
            st.caption("None within the local band.")
        else:
            st.dataframe(local_cands[short], hide_index=True, use_container_width=True)
        st.markdown(f"**Nearby consolidation options (within {bands[1]:g} miles)**")
        st.caption("Before any consolidation, confirm property type, occupation requirement, security classification, "
                   "floor-area standard and availability. Shown for investigation only.")
        if near_consol.empty:
            st.caption("No nearby properties with consolidation signals or recorded vacant space.")
        else:
            st.dataframe(near_consol[["insite_property_reference", "property_name", "town", "distance_miles",
                                      "asset_type", "usage", "security_classification", "total_vacant_area_sqm",
                                      "Floor area (recorded)", "Vacancy status", "earliest_vacant_available_date",
                                      "latest_available_end_date", "core_score", "confidence"]],
                         hide_index=True, use_container_width=True)
        pts = pd.concat([near_high, local_cands]).drop_duplicates("insite_property_reference")
        pts = pts.dropna(subset=["map_latitude", "map_longitude"]).rename(
            columns={"map_latitude": "latitude", "map_longitude": "longitude"})
        point_map(pts, (7, 134, 107), radius=400, zoom=8,
                  origin={"latitude": home["latitude"], "longitude": home["longitude"],
                          "label": f"Home postcode district {home_parts[1]}"})
        st.caption("Red: home postcode centroid (approximate). Green: high-priority and local candidates.")

    # 5. Method
    with st.expander("5. Scoring and prioritisation method"):
        st.markdown(
            "Core score (0-100) is the sum of six components, each shown separately:\n\n"
            "| Component | Max | Points awarded |\n| --- | --- | --- |\n"
            "| A. Vacancy and underutilisation | 30 | Vacant area \u226510,000 / 2,500 / 500 / >0 sqm: 12/9/6/3; vacant "
            "proportion \u226550/25/10/>0%: 8/6/4/2 (only where measurement bases are consistent); longest vacancy "
            "\u22655/2/1 years: 6/4/2; whole-building or large vacancy: 4 |\n"
            "| B. Surplus and disposal position | 25 | Recorded surplus: 10; surplus not on the market: 6; surplus floor "
            "\u22651,000 sqm or land \u22651 ha: 5 (any area: 2); no record on the market or under offer: 4 |\n"
            "| C. Consolidation opportunity | 20 | Occupation records \u22655/\u22652: 6/4; organisations \u22653/\u22652: "
            "4/2; occupation with \u2265500 sqm vacant or a consolidation flag: 6 (any vacant: 3); multiple contracts: 4 |\n"
            "| D. Property and contract review | 10 | Contracts \u22653/>1: 4/2; duplicate property reference: 3; not in "
            "the property register: 3 |\n"
            "| E. Sustainability opportunity indicator | 10 | Vacant \u22651,000/250/>0 sqm: 5/3/1; occupied and vacant "
            "space together: 3; vacant \u22651 year: 2 |\n"
            "| F. Data-quality risk | 5 | Data-quality issues \u22653/\u22651: 3/2; mixed bases or location conflict: 1; "
            "outdated availability: 1; possible status conflict: 1 (capped at 5) |\n\n"
            "Interpretation: 75-100 very high, 60-74 high, 40-59 medium, below 40 lower priority (indicative).\n\n"
            f"Proximity score (separate): Local 10, Accessible 7, Extended 4, Distant 1, unavailable: no score. Combined "
            f"ranking (only when selected) = core \u00d7 weight + proximity \u00d7 10 \u00d7 (1 \u2212 weight); default 85/15. "
            "Proximity never reduces the core score.\n\n"
            "Confidence: **Low** where coordinates are missing, area bases are mixed, area units are unknown, three or more "
            "data-quality issues exist or a possible status conflict is present; **Medium** where some issues, fallback "
            "denominators, missing start dates or outdated availability exist; otherwise **High**.")

    # 6. Portfolio patterns
    st.markdown("#### 6. Key portfolio-wide patterns")
    a, b_ = st.columns(2)
    with a:
        hbar(port["bands"], "Properties by core priority", "All properties in the selected regions", value_label="Properties")
    with b_:
        cats = pd.DataFrame({"label": list(port["cats"]), "value": [v or 0 for v in port["cats"].values()]})
        hbar(cats, "Properties by priority category", "A property can appear in several categories",
             value_label="Properties")
    a, b_ = st.columns(2)
    with a:
        hbar(port["regions"], "Regions with most medium-or-higher properties", "Core score \u2265 40", value_label="Properties")
    with b_:
        st.markdown("**Recorded vacant area by measurement basis**  \n<span class='gm-muted'>Bases are not combined "
                    "into a single total because they are not directly comparable.</span>", unsafe_allow_html=True)
        st.dataframe(port["vac_by_basis"], hide_index=True, use_container_width=True)

    # 7. Data quality
    st.markdown("#### 7. Data-quality issues")
    t = port["totals"]
    m = st.columns(4)
    for col, (lab, v, sub) in zip(m, [
            ("Possible status conflicts", t["possible_conflicts"], "occupied + available vacant + surplus"),
            ("Outdated availability records", t["past_end_records"], "'Available' past end date"),
            ("Mixed area bases", t["mixed_basis"], "properties"),
            ("No comparable location", t["no_location"], "proximity unavailable")]):
        with col:
            card(lab, fmt(v or 0), sub)
    hbar(port["dq"], "Most frequent data-quality issues", "Properties affected", color="#f47738", value_label="Properties")

    # 8-10
    with st.expander("8. Recommended next actions", expanded=True):
        st.markdown(
            "* Validate the top-ranked records with holding organisations before any decision (areas, bases, dates, status).\n"
            "* For surplus properties not on the market, confirm disposal blockers and timetable.\n"
            "* For consolidation candidates, test compatibility of usage, security classification, floor-area standard and "
            "availability against the occupation requirement.\n"
            "* Ask data owners to resolve possible status conflicts and availability records past their end dates.\n"
            "* Use the local candidates list to plan site visits from the home office.")
    with st.expander("9. Additional data needed for a stronger sustainability assessment"):
        st.markdown(
            f"Only {fmt(t['with_energy'] or 0)} of {fmt(t['properties'])} properties have any EPC/DEC rating. Needed: "
            "metered energy consumption (electricity, gas, heat), tariffs and energy cost, emissions factors, "
            "floor/zone-level operating hours and servicing, building condition and maintenance cost, actual occupancy, "
            "attendance and desk utilisation, retrofit cost estimates, and disposal values. Without these, energy, cost "
            "and carbon savings cannot be calculated: reducing unused operational floor area may reduce avoidable energy "
            "use, but vacant area does not automatically mean the associated energy use can be eliminated.")
    with st.expander("10. Methodology limitations"):
        st.markdown(
            "* Scores are indicative and based only on the supplied records; thresholds are analytical assumptions.\n"
            "* Distances are approximate straight-line values from the postcode centroid, not road distance or travel time.\n"
            "* Vacancy, occupation and surplus records may relate to different parts of a site; where scope cannot be "
            "established, conflicts are labelled as possible.\n"
            "* Contract start/end dates are not available in the linked property table, so upcoming contract "
            "opportunities cannot be scored.\n"
            "* The question is interpreted by keyword matching; review the focus categories before relying on results.\n"
            "* Sustainability scoring is a proxy based on unused floor area, not measured energy or carbon.")
    st.caption(DISTANCE_NOTE)
