# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Government Property Sustainability and Investment Prioritisation
# MAGIC
# MAGIC **Decision-support prototype.** This notebook builds a transparent, rules-based prioritisation of government properties that may warrant further investigation for sustainability improvements, retrofit or energy-efficiency investment, consolidation of occupations, reuse of vacant space, and release or disposal of surplus property or land. It is **not** a property valuation or an investment decision, and no machine-learning model or AI calculates any score.
# MAGIC
# MAGIC **Sources** – six managed Delta tables landed by the **GovMove Setup** notebook in `manual_sandbox_dev.property_prioritisation`: `property`, `building`, `occupation`, `vacant_space`, `surplus`, `land`. Original Excel workbooks are optionally searched for in a configurable volume path.
# MAGIC
# MAGIC **Location is central** – every original location field (property name/number, street, town, postcode, region, local authority, Easting/Northing, NI Easting/Northing, latitude/longitude) is preserved alongside cleaned versions, `full_address`, `location_key`, `has_valid_coordinates` and `location_quality_score/notes`. Every record keeps `source_dataset` and `source_row_id`.
# MAGIC
# MAGIC **Outputs** (Delta, in `TARGET_SCHEMA`): `property_master`, `property_clean`, `building_clean`, `occupation_clean`, `land_clean`, `surplus_clean`, `vacant_space_clean`, `property_location_matches`, `property_priority_results`, `property_data_quality_issues`, `column_mapping` (plus `stg_*` intermediate tables).
# MAGIC
# MAGIC **Sections**: 1 Configuration · 2 File discovery · 3 Excel ingestion · 4 Schema inspection · 5 Cleaning and standardisation · 6 Location preparation · 7 Property matching · 8 Area conversion · 9 Feature engineering · 10 Priority scoring · 11 Explanations · 12 Visualisations · 13 Data-quality report · 14 Validation · 15 Export and next steps

# COMMAND ----------

# DBTITLE 1,1. Configuration
# MAGIC %md
# MAGIC ## 1. Configuration
# MAGIC All sources, output schema, documented assumptions, thresholds and score weights. Change values here only.
# MAGIC
# MAGIC **Area-unit assumptions** (flagged in outputs and in the data-quality report):
# MAGIC * `Floor Area Measurement` / `Vacant Area Units` hold IPMS measurement *bases* (IPMS 1 GFA, IPMS 2 GIA, IPMS 3 NIA), not units. IPMS figures are reported in square metres by convention, so `ASSUME_IPMS_IS_SQM = True`. A null basis gives `unit_unknown`: the raw value is kept but excluded from sqm totals.
# MAGIC * `land.Total Land Area`, `property.Total Land Area` and `surplus.Total Surplus Land Area` have no unit field and are assumed to be **hectares**; `surplus.Total Surplus Floor Area` is assumed to be **square metres**.

# COMMAND ----------

# DBTITLE 1,Configuration settings
from pyspark.sql import functions as F
from pyspark.sql import Window as W
from functools import reduce
import re, difflib

# --- Sources ---------------------------------------------------------------
SOURCE_CATALOG = "manual_sandbox_dev"
SOURCE_SCHEMA = "property_prioritisation"   # GovMove landing schema (loaded by the GovMove Setup notebook)
SOURCE_TABLES = ["property", "building", "occupation", "vacant_space", "surplus", "land"]
# Optional folders searched for the original .xlsx workbooks (informational; absence is not an error)
SOURCE_FILE_PATHS = ["/Volumes/manual_sandbox_dev/property_prioritisation"]

# --- Outputs ---------------------------------------------------------------
TARGET_SCHEMA = "manual_sandbox_dev.property_prioritisation"
FALLBACK_TARGET_SCHEMA = None   # e.g. "manual_sandbox_dev.<your_schema>" if TARGET_SCHEMA cannot be created
DROP_STAGING_TABLES = False     # set True to drop the stg_* intermediate tables in Section 15

# --- Documented area-unit assumptions ----------------------------------------
ASSUME_IPMS_IS_SQM = True
ASSUME_LAND_AREA_IS_HECTARES = True
ASSUME_SURPLUS_LAND_AREA_IS_HECTARES = True
ASSUME_SURPLUS_FLOOR_AREA_IS_SQM = True

# --- Coordinate validation ---------------------------------------------------
UK_LAT_MIN, UK_LAT_MAX = 49.8, 60.9
UK_LON_MIN, UK_LON_MAX = -8.7, 1.8
GB_E_MIN, GB_E_MAX = 0, 700000        # British National Grid; 0 is treated as 'not supplied'
GB_N_MIN, GB_N_MAX = 0, 1300000
NI_E_MIN, NI_E_MAX = 100000, 400000   # Irish Grid (Northern Ireland); 0 = not supplied
NI_N_MIN, NI_N_MAX = 250000, 550000

# --- Secondary property matching ---------------------------------------------
MATCH_NEAR_M = 25    # within this distance (and postcode/name agreeing) a proximity match can be Medium
MATCH_MAX_M = 100    # proximity candidates further than this are ignored

# --- Feature thresholds ------------------------------------------------------
LONG_TERM_VACANCY_DAYS = 365
VACANCY_DURATION_FULL_SCORE_DAYS = 1095   # 3 years or more = full duration score
LARGE_VACANT_PERCENTILE = 0.75            # 'large' = at/above regional 75th percentile of vacant area
LARGE_VACANT_MIN_SQM = 100
MIN_COMPARABLE_GROUP = 5                  # regions with fewer vacant properties use the national percentile
SIGNIFICANT_OCCUPIED_SQM = 100
SIGNIFICANT_VACANT_SQM = 100
SIGNIFICANT_VACANT_RATIO = 0.20
MULTIPLE_OCCUPATION_MIN = 2
NEARBY_RADII_KM = [1, 5, 10]
LOCAL_CLUSTER_RADIUS_KM = 1               # must be one of NEARBY_RADII_KM
H3_CLUSTER_RESOLUTION = 7                 # ~5 km2 hexagons for cluster ids
EPC_MAX_VALIDITY_YEARS = 10

# --- Score weights (sum to 1) and bands -------------------------------------
WEIGHTS = {"vacancy_score": 0.30, "vacancy_duration_score": 0.15, "surplus_score": 0.20,
           "consolidation_reuse_score": 0.20, "scale_score": 0.10, "data_uncertainty_score": 0.05}
assert abs(sum(WEIGHTS.values()) - 1.0) < 1e-9, "Weights must sum to 1"
assert LOCAL_CLUSTER_RADIUS_KM in NEARBY_RADII_KM
PRIORITY_BANDS = [(80, "Very High"), (60, "High"), (40, "Medium"), (20, "Low"), (0, "Very Low")]

ENERGY_ABSENT_SENTENCE = ("Energy-performance data is not currently available, so the score identifies "
                          "property-utilisation and estate-management opportunities rather than confirmed retrofit need.")

# --- Target schema with fallback ---------------------------------------------
def _schema_writable(schema):
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {schema}.zz_write_check (x INT)")
        spark.sql(f"DROP TABLE IF EXISTS {schema}.zz_write_check")
        return True
    except Exception as e:
        print(f"Cannot write to {schema}: {str(e)[:300]}")
        return False

if not _schema_writable(TARGET_SCHEMA):
    if FALLBACK_TARGET_SCHEMA and _schema_writable(FALLBACK_TARGET_SCHEMA):
        TARGET_SCHEMA = FALLBACK_TARGET_SCHEMA
    else:
        raise PermissionError("Cannot create/write TARGET_SCHEMA. Set FALLBACK_TARGET_SCHEMA to a schema you can write to.")

def T(name):
    return f"{TARGET_SCHEMA}.{name}"

def save_delta(df, name, comment=None):
    """Overwrite a Delta table in TARGET_SCHEMA and return it re-read from storage."""
    full = T(name)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full)
    if comment:
        safe = comment.replace("'", "''")
        spark.sql(f"COMMENT ON TABLE {full} IS '{safe}'")
    out = spark.table(full)
    print(f"Wrote {full}: {out.count():,} rows")
    return out

settings = {"TARGET_SCHEMA": TARGET_SCHEMA, "ASSUME_IPMS_IS_SQM": ASSUME_IPMS_IS_SQM,
            "ASSUME_LAND_AREA_IS_HECTARES": ASSUME_LAND_AREA_IS_HECTARES,
            "ASSUME_SURPLUS_LAND_AREA_IS_HECTARES": ASSUME_SURPLUS_LAND_AREA_IS_HECTARES,
            "ASSUME_SURPLUS_FLOOR_AREA_IS_SQM": ASSUME_SURPLUS_FLOOR_AREA_IS_SQM,
            "UK bounding box": f"lat {UK_LAT_MIN}..{UK_LAT_MAX}, lon {UK_LON_MIN}..{UK_LON_MAX}",
            "GB grid": f"E {GB_E_MIN}..{GB_E_MAX}, N {GB_N_MIN}..{GB_N_MAX}",
            "NI grid": f"E {NI_E_MIN}..{NI_E_MAX}, N {NI_N_MIN}..{NI_N_MAX}",
            "Match distances (m)": f"near {MATCH_NEAR_M}, max {MATCH_MAX_M}",
            "LONG_TERM_VACANCY_DAYS": LONG_TERM_VACANCY_DAYS, "NEARBY_RADII_KM": NEARBY_RADII_KM,
            "WEIGHTS": WEIGHTS, "PRIORITY_BANDS": PRIORITY_BANDS, "As-of date": "current_date()"}
print("Output schema:", TARGET_SCHEMA)
display(spark.createDataFrame([(k, str(v)) for k, v in settings.items()], "setting string, value string"))

# COMMAND ----------

# DBTITLE 1,2. File discovery
# MAGIC %md
# MAGIC ## 2. File discovery
# MAGIC The sources are managed Delta tables created by the file-upload UI, so they are discovered from `information_schema`. The optional volume search lists any original Excel workbooks and their sheet names, and does not fail if none exist.

# COMMAND ----------

# DBTITLE 1,Discover source tables and optional workbooks
table_list = ", ".join(f"'{t}'" for t in SOURCE_TABLES)
tables_df = spark.sql(f"""
    SELECT table_catalog, table_schema, table_name, table_type, data_source_format, created, last_altered
    FROM {SOURCE_CATALOG}.information_schema.tables
    WHERE table_schema = '{SOURCE_SCHEMA}' AND table_name IN ({table_list})
    ORDER BY table_name""")
display(tables_df)

found = {r.table_name for r in tables_df.collect()}
missing_tables = [t for t in SOURCE_TABLES if t not in found]
SOURCE_TABLES_FOUND = [t for t in SOURCE_TABLES if t in found]
if missing_tables:
    print("WARNING - source tables not found (later steps continue without them):", missing_tables)
print("Source tables found:", SOURCE_TABLES_FOUND)

# Optional: look for original Excel workbooks (informational only)
excel_files = []
def _walk(path, depth=0):
    try:
        for f in dbutils.fs.ls(path):
            if f.name.lower().endswith((".xlsx", ".xlsm", ".xls")):
                excel_files.append((f.path, f.size))
            elif f.name.endswith("/") and depth < 3:
                _walk(f.path, depth + 1)
    except Exception as e:
        print(f"Path not searchable ({path}): {str(e)[:150]}")

# Expand schema-level paths (/Volumes/<catalog>/<schema>) into the volumes they contain
search_paths = []
for p in SOURCE_FILE_PATHS:
    parts = [x for x in p.split("/") if x]
    if len(parts) == 3 and parts[0] == "Volumes":
        try:
            vols = spark.sql(f"SELECT volume_name FROM {parts[1]}.information_schema.volumes WHERE volume_schema = '{parts[2]}'").collect()
            search_paths += [f"/Volumes/{parts[1]}/{parts[2]}/{v.volume_name}" for v in vols]
        except Exception as e:
            print(f"Could not list volumes for {p}: {str(e)[:150]}")
    else:
        search_paths.append(p)
print("Volume paths searched:", search_paths or "(none found)")
for p in search_paths:
    _walk(p)

if excel_files:
    rows = []
    for path, size in excel_files:
        try:
            import openpyxl
            wb = openpyxl.load_workbook(path.replace("dbfs:", ""), read_only=True)
            rows += [(path, size, s) for s in wb.sheetnames]
        except Exception as e:
            rows.append((path, size, f"(sheets not readable: {str(e)[:80]})"))
    display(spark.createDataFrame(rows, "path string, size_bytes long, sheet_name string"))
else:
    print("No Excel workbooks found in", SOURCE_FILE_PATHS, "- the Delta tables above are the authoritative sources.")

# COMMAND ----------

# DBTITLE 1,3. Excel ingestion
# MAGIC %md
# MAGIC ## 3. Excel ingestion (from the uploaded Delta tables)
# MAGIC Each table is loaded without assuming identical columns. Every record gets `source_dataset`, a deterministic `source_row_id` (`<dataset>-NNNNNN`, ordered by a content hash) and `source_row_hash` (SHA-256 of all data columns, used for exact-duplicate detection). Column names are converted to lower-case snake_case; known variants are renamed (e.g. `In Site Disposal Reference` → `insite_disposal_reference`). Where two source columns collapse to the same name (property has both `SecurityClassification` and `Security Classification`) the second is kept as `<name>__variant_2`. `_rescued_data` (0 non-null rows) is kept but excluded from analysis.

# COMMAND ----------

# DBTITLE 1,Load tables, add lineage, snake_case columns
def to_snake(name):
    if name == "_rescued_data":
        return name
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name.strip())
    s = re.sub(r"[^0-9A-Za-z]+", "_", s).strip("_").lower()
    return re.sub(r"_+", "_", s)

VARIANT_RENAMES = {"in_site_disposal_reference": "insite_disposal_reference",
                   "in_site_vacant_space_reference": "insite_vacant_space_reference",
                   "in_site_occupation_ref": "insite_occupation_reference"}

raw, rename_log = {}, []
for ds in SOURCE_TABLES_FOUND:
    src = spark.table(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{ds}")
    data_cols = [c for c in src.columns if c != "_rescued_data"]
    df = src.withColumn("source_row_hash", F.sha2(F.to_json(F.struct(*[F.col(f"`{c}`") for c in data_cols])), 256))
    df = df.withColumn("_rn", F.row_number().over(W.orderBy("source_row_hash")))
    df = (df.withColumn("source_dataset", F.lit(ds))
            .withColumn("source_row_id", F.concat(F.lit(f"{ds}-"), F.lpad(F.col("_rn").cast("string"), 6, "0"))))
    seen, sel = set(), []
    for c in src.columns:
        s = to_snake(c)
        s2 = VARIANT_RENAMES.get(s, s)
        method = "unchanged" if s2 == c else ("snake_case" if s2 == s else "snake_case + variant rename")
        base, k = s2, 2
        while s2 in seen:
            s2 = f"{base}__variant_{k}"
            k += 1
        if s2 != base:
            method += " (name collision: kept as variant)"
        seen.add(s2)
        rename_log.append((ds, c, s2, method))
        sel.append(F.col(f"`{c}`").alias(s2))
    raw[ds] = df.select("source_dataset", "source_row_id", "source_row_hash", *sel)

rename_df = spark.createDataFrame(rename_log, "dataset string, original_column string, snake_case_column string, rename_method string")
display(rename_df.filter("rename_method != 'snake_case'"))
print(f"{rename_df.count()} columns standardised across {len(raw)} datasets")

# COMMAND ----------

# DBTITLE 1,4. Schema inspection
# MAGIC %md
# MAGIC ## 4. Schema inspection
# MAGIC Detected schema, row count and a sample of each dataset, then the mapping of each dataset's columns to the canonical columns used downstream. Where a canonical column is absent, the closest reasonable variant is used and documented; if none exists, a null column is created and noted in the data-quality report.

# COMMAND ----------

# DBTITLE 1,Row counts, schemas and samples
summary_rows, schema_rows = [], []
for ds, df in raw.items():
    n = df.count()
    rescued = df.filter(F.col("_rescued_data").isNotNull()).count() if "_rescued_data" in df.columns else None
    summary_rows.append((ds, f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{ds}", n, len(df.columns) - 3, rescued))
    schema_rows += [(ds, f.name, f.dataType.simpleString()) for f in df.schema.fields]

display(spark.createDataFrame(summary_rows, "dataset string, source_table string, row_count long, source_column_count int, rescued_data_non_null long"))
display(spark.createDataFrame(schema_rows, "dataset string, column_name string, data_type string"))
for ds, df in raw.items():
    print(f"Sample rows: {ds}")
    display(df.limit(5))

# COMMAND ----------

# DBTITLE 1,Map columns to canonical names
LOC = ["property_name", "property_number", "street_name", "town", "postcode", "region", "local_authority",
       "easting", "northing", "ni_easting", "ni_northing", "latitude", "longitude"]
COMMON_CANON = {c: [c] for c in LOC}
COMMON_CANON.update({
    "insite_property_reference": ["insite_property_reference", "insite_property_ref"],
    "contract_sector": ["contract_sector", "sector"],
    "contract_parent_organisation": ["contract_parent_organisation", "parent_organisation"],
    "contract_organisation": ["contract_organisation", "organisation"],
    "sector": ["sector", "contract_sector"],
    "parent_organisation": ["parent_organisation", "contract_parent_organisation"],
    "organisation": ["organisation", "contract_organisation"],
    "security_classification": ["security_classification"],
})
DATASET_CANON = {
    "property": {"insite_contract_reference": ["insite_contract_reference"], "floor_area": ["floor_area"],
                 "floor_area_measurement": ["floor_area_measurement"], "total_land_area": ["total_land_area"],
                 "land_usage": ["land_usage"], "usage": ["usage"], "tenure": ["tenure"], "asset_type": ["asset_type"],
                 "country": ["country"], "acquisition_date": ["acquisition_date"]},
    "building": {"insite_building_reference": ["insite_building_reference"], "floor_area": ["floor_area"],
                 "floor_area_measurement": ["floor_area_measurement"], "construction_date": ["construction_date"],
                 "listed_building": ["listed_building"], "breeam_rating_type": ["breeam_rating_type"],
                 "epc_property_rating": ["epc_property_rating"], "epc_property_rating_value": ["epc_property_rating_value"],
                 "epc_certificate_issue_date": ["epc_certificate_issue_date"], "epc_expiry_date": ["epc_expiry_date"],
                 "dec_operational_rating": ["dec_operational_rating"], "dec_operating_rating_value": ["dec_operating_rating_value", "dec_operational_rating_value"],
                 "dec_certificate_issue_date": ["dec_certificate_issue_date"], "dec_expiry_date": ["dec_expiry_date"]},
    "occupation": {"insite_occupation_reference": ["insite_occupation_reference", "insite_occupation_ref"],
                   "insite_contract_reference": ["insite_contract_reference"], "occupation_name": ["occupation_name"],
                   "floor_area": ["floor_area"], "floor_area_measurement": ["floor_area_measurement"],
                   "usage": ["usage"], "tenure": ["tenure"]},
    "vacant_space": {"insite_vacant_space_reference": ["insite_vacant_space_reference"],
                     "insite_contract_reference": ["insite_contract_reference"], "vacant_area": ["vacant_area"],
                     "vacant_area_units": ["vacant_area_units", "vacant_area_unit"], "vacant_space_status": ["vacant_space_status"],
                     "vacant_space_available_start_date": ["vacant_space_available_start_date"],
                     "vacant_space_availability_end_date": ["vacant_space_availability_end_date", "vacant_space_available_end_date"],
                     "vacation_date": ["vacation_date"]},
    "surplus": {"insite_disposal_reference": ["insite_disposal_reference"], "insite_contract_reference": ["insite_contract_reference"],
                "surplus_status": ["surplus_status"], "status_of_sale": ["status_of_sale"],
                "total_surplus_land_area": ["total_surplus_land_area"], "total_surplus_floor_area": ["total_surplus_floor_area"],
                "land_usage": ["land_usage"]},
    "land": {"insite_land_reference": ["insite_land_reference"], "total_land_area": ["total_land_area"], "usage": ["usage"]},
}
CANON_TYPES = {"easting": "bigint", "northing": "bigint", "ni_easting": "bigint", "ni_northing": "bigint",
               "latitude": "double", "longitude": "double", "floor_area": "double", "vacant_area": "double",
               "total_land_area": "double", "total_surplus_land_area": "double", "total_surplus_floor_area": "double",
               "surplus_status": "boolean", "construction_date": "date", "acquisition_date": "date",
               "epc_certificate_issue_date": "date", "epc_expiry_date": "date", "dec_certificate_issue_date": "date",
               "dec_expiry_date": "date", "epc_property_rating_value": "bigint", "dec_operating_rating_value": "bigint"}
# Derived canonical columns (business rule supplied by the data owner):
# the first 8 characters of the Insite Land Reference (e.g. P0000362-L0013128 -> P0000362) are the Insite Property Reference.
DERIVED_CANON = {("land", "insite_property_reference"): ("insite_land_reference",
    lambda c: F.when(F.substring(F.col(c), 1, 8).rlike(r"^P[0-9]{7}$"), F.substring(F.col(c), 1, 8)),
    "first 8 characters of Insite Land Reference (business rule from data owner)")}
LINEAGE_COLS = ["source_dataset", "source_row_id", "source_row_hash"]
orig_lookup = {(d, s): o for d, o, s, _ in rename_log}
norm = lambda s: s.replace("_", "")

std, mapping_rows = {}, []
for ds, df in raw.items():
    cols = [c for c in df.columns if c not in LINEAGE_COLS + ["_rescued_data"]]
    reqs = {**COMMON_CANON, **DATASET_CANON.get(ds, {})}
    out = df
    for canon, cands in reqs.items():
        variants = sorted(c for c in cols if c.startswith(canon + "__variant_"))
        alts = [c for c in cands if c != canon and c in cols]
        if canon in cols and variants:
            out = out.withColumn(canon, F.coalesce(F.col(canon), *[F.col(v) for v in variants]))
            src, method = ", ".join([canon] + variants), "coalesce name variants"
            note = "Two source columns with the same meaning; first non-null used, originals retained"
        elif canon in cols:
            src, method, note = canon, "exact", None
        elif (ds, canon) in DERIVED_CANON and DERIVED_CANON[(ds, canon)][0] in cols:
            src_c, fn, desc = DERIVED_CANON[(ds, canon)]
            out = out.withColumn(canon, fn(src_c))
            src, method, note = src_c, "derived", f"'{canon}' derived as {desc}"
        elif alts:
            out = out.withColumn(canon, F.col(alts[0]))
            src, method, note = alts[0], "variant / fallback column", f"'{canon}' not in {ds}; populated from '{alts[0]}'"
        else:
            close = difflib.get_close_matches(norm(canon), [norm(c) for c in cols], n=1, cutoff=0.9)
            if close:
                src = next(c for c in cols if norm(c) == close[0])
                out = out.withColumn(canon, F.col(src))
                method, note = "fuzzy name match", f"Closest column '{src}' used for '{canon}' - review"
            else:
                out = out.withColumn(canon, F.lit(None).cast(CANON_TYPES.get(canon, "string")))
                src, method, note = None, "missing - created as null", f"No reliable source column for '{canon}' in {ds}; created as null"
        first = src.split(", ")[0] if src else None
        mapping_rows.append((ds, canon, src, orig_lookup.get((ds, first)) if first else None, method, note))
    std[ds] = out

column_mapping_df = spark.createDataFrame(mapping_rows, "dataset string, canonical_column string, source_column string, source_column_original string, mapping_method string, note string")
column_mapping_df = save_delta(column_mapping_df, "column_mapping", "Mapping of source columns to canonical snake_case columns")
display(column_mapping_df.filter("mapping_method != 'exact'").orderBy("dataset", "canonical_column"))

# COMMAND ----------

# DBTITLE 1,5. Cleaning and standardisation
# MAGIC %md
# MAGIC ## 5. Cleaning and standardisation
# MAGIC Original values are never overwritten: cleaned or parsed values go into new columns, and questionable records are flagged, not deleted.
# MAGIC * **Record flags (all datasets):** `exact_duplicate_row_flag` / `is_duplicate_copy` (identical content; the copies are excluded from area totals), `reference_duplicate_flag` (the dataset's own reference repeats, e.g. one property row per contract), `missing_property_reference_flag`.
# MAGIC * **Land:** `insite_property_reference` is derived from the first 8 characters of `insite_land_reference` (rule supplied by the data owner).
# MAGIC * **Vacant space:** `dd/MM/yyyy HH:mm` strings are parsed to dates, with `*_invalid_flag` columns.
# MAGIC * **Surplus:** `status_of_sale_clean` and a boolean `surplus_status_clean`.
# MAGIC * **Property:** `country_clean` standardises values such as 'UK' and 'This record is within England'.
# MAGIC * **Building:** EPC/DEC bands (A+ to G), expiry flags and implausible-date flags.

# COMMAND ----------

# DBTITLE 1,Cleaning helpers and dataset-specific cleaning
def clean_text(c):
    t = F.regexp_replace(F.trim(F.col(c).cast("string")), r"\s+", " ")
    return F.when(t != "", t)

def match_text(c):
    """Upper-case, punctuation-free, single-spaced text for matching."""
    t = F.trim(F.regexp_replace(F.regexp_replace(F.upper(F.col(c).cast("string")), r"[^A-Z0-9&]+", " "), r"\s+", " "))
    return F.when(t != "", t)

def jset(c):
    return F.array_join(F.array_sort(F.collect_set(F.col(c).cast("string"))), "; ")

def safe_bool(e):
    return F.coalesce(e, F.lit(False))

DATE_FORMATS = ["dd/MM/yyyy HH:mm", "d/M/yyyy H:mm", "dd/MM/yyyy", "d/M/yyyy", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd"]
def parse_date_str(c):
    return F.coalesce(*[F.expr(f"try_to_timestamp(trim(CAST(`{c}` AS STRING)), '{f}')") for f in DATE_FORMATS]).cast("date")

REF = "insite_property_reference"
REF_COL = {"property": REF, "building": "insite_building_reference", "occupation": "insite_occupation_reference",
           "vacant_space": "insite_vacant_space_reference", "surplus": "insite_disposal_reference", "land": "insite_land_reference"}
EPC_BANDS = ["A+", "A", "B", "C", "D", "E", "F", "G"]
BAND_RANK = F.create_map(*[x for i, b in enumerate(EPC_BANDS) for x in (F.lit(b), F.lit(i))])
def band_of(c):
    b = F.regexp_extract(F.upper(F.trim(F.col(c).cast("string"))), r"^(A\+|[A-G])", 1)
    return F.when(b != "", b)

def add_record_flags(df, ds):
    rc = REF_COL[ds]
    return (df
        .withColumn("record_reference", F.col(rc).cast("string"))
        .withColumn("exact_duplicate_count", F.count(F.lit(1)).over(W.partitionBy("source_row_hash")))
        .withColumn("exact_duplicate_row_flag", F.col("exact_duplicate_count") > 1)
        .withColumn("is_duplicate_copy", F.row_number().over(W.partitionBy("source_row_hash").orderBy("source_row_id")) > 1)
        .withColumn("missing_record_reference_flag", F.col(rc).isNull())
        .withColumn("reference_duplicate_count", F.when(F.col(rc).isNotNull(), F.count(F.lit(1)).over(W.partitionBy(rc))))
        .withColumn("reference_duplicate_flag", safe_bool(F.col("reference_duplicate_count") > 1))
        .withColumn("missing_property_reference_flag", F.col(REF).isNull()))

cleaned = {ds: add_record_flags(df, ds) for ds, df in std.items()}

if "property" in cleaned:
    cu = F.upper(F.trim(F.col("country")))
    cleaned["property"] = (cleaned["property"]
        .withColumn("country_clean", F.when(cu.isNull() | (cu == ""), F.lit(None))
            .when(cu.contains("NORTHERN IRELAND"), "Northern Ireland").when(cu.contains("ENGLAND"), "England")
            .when(cu.contains("SCOTLAND"), "Scotland").when(cu.contains("WALES"), "Wales")
            .when(cu.isin("UK", "U.K.", "UNITED KINGDOM", "GB", "GREAT BRITAIN"), "United Kingdom")
            .otherwise(F.initcap(F.trim(F.col("country")))))
        .withColumn("acquisition_date_implausible_flag",
                    safe_bool((F.col("acquisition_date") > F.current_date()) | (F.year("acquisition_date") < 1066))))

if "vacant_space" in cleaned:
    v = cleaned["vacant_space"]
    for rc, new in [("vacant_space_available_start_date", "vacant_available_start_date"),
                    ("vacant_space_availability_end_date", "vacant_availability_end_date"),
                    ("vacation_date", "vacation_date_parsed")]:
        v = (v.withColumn(new, parse_date_str(rc))
              .withColumn(f"{new}_invalid_flag", safe_bool((clean_text(rc).isNotNull() & F.col(new).isNull())
                          | (F.year(new) < 1900) | (F.col(new) > F.add_months(F.current_date(), 600)))))
    cleaned["vacant_space"] = (v.withColumn("vacant_space_status_clean", F.initcap(clean_text("vacant_space_status")))
                                .withColumn("vacant_missing_start_date_flag", F.col("vacant_available_start_date").isNull()))

if "surplus" in cleaned:
    sl = F.lower(F.trim(F.col("status_of_sale")))
    cleaned["surplus"] = (cleaned["surplus"]
        .withColumn("status_of_sale_clean", F.when(sl.isNull() | (sl == ""), F.lit(None))
            .when(sl.rlike(r"not\s+on\s+(the\s+)?market"), "Not on the Market")
            .when(sl.contains("under offer"), "Under Offer")
            .when(sl.rlike(r"on\s+(the\s+)?market"), "On the Market")
            .when(sl.contains("sold"), "Sold")
            .otherwise(F.initcap(F.trim(F.col("status_of_sale")))))
        .withColumn("surplus_status_clean", F.col("surplus_status").cast("boolean")))

if "building" in cleaned:
    b = cleaned["building"]
    for kind, rating in [("epc", "epc_property_rating"), ("dec", "dec_operational_rating")]:
        issue, expiry = F.col(f"{kind}_certificate_issue_date"), F.col(f"{kind}_expiry_date")
        b = (b.withColumn(f"{kind}_band", band_of(rating))
              .withColumn(f"{kind}_band_rank", F.when(F.col(f"{kind}_band").isNotNull(), BAND_RANK[F.col(f"{kind}_band")]))
              .withColumn(f"{kind}_expired_flag", safe_bool(expiry < F.current_date()))
              .withColumn(f"{kind}_implausible_date_flag", safe_bool(
                  (issue > F.current_date()) | (expiry > F.add_months(F.current_date(), 12 * (EPC_MAX_VALIDITY_YEARS + 1)))
                  | (expiry < issue) | (F.year(issue) < 2007))))
    cleaned["building"] = (b
        .withColumn("construction_date_implausible_flag", safe_bool((F.col("construction_date") > F.current_date()) | (F.year("construction_date") < 1000)))
        .withColumn("listed_building_flag", safe_bool(F.col("listed_building").isNotNull() & (F.upper(F.col("listed_building")) != "NOT LISTED")))
        .withColumn("has_energy_certificate", F.col("epc_band").isNotNull() | F.col("dec_band").isNotNull()))

if "land" in cleaned:
    cleaned["land"] = cleaned["land"].withColumn("property_reference_derived_flag", F.col(REF).isNotNull())

for ds, df in cleaned.items():
    save_delta(df, f"stg_{ds}_clean")

summary = reduce(lambda a, b: a.unionByName(b), [
    spark.table(T(f"stg_{ds}_clean")).agg(
        F.lit(ds).alias("dataset"), F.count("*").alias("rows"),
        F.sum(F.col("is_duplicate_copy").cast("int")).alias("exact_duplicate_copies"),
        F.sum(F.col("reference_duplicate_flag").cast("int")).alias("rows_with_repeated_reference"),
        F.sum(F.col("missing_record_reference_flag").cast("int")).alias("missing_record_reference"),
        F.sum(F.col("missing_property_reference_flag").cast("int")).alias("missing_property_reference"))
    for ds in cleaned])
display(summary)
if "vacant_space" in cleaned:
    display(spark.table(T("stg_vacant_space_clean")).agg(
        F.sum(F.col("vacant_available_start_date").isNotNull().cast("int")).alias("start_dates_parsed"),
        F.sum(F.col("vacant_available_start_date_invalid_flag").cast("int")).alias("start_dates_invalid"),
        F.sum(F.col("vacant_missing_start_date_flag").cast("int")).alias("start_dates_missing"),
        F.min("vacant_available_start_date").alias("earliest_start"), F.max("vacant_available_start_date").alias("latest_start")))

# COMMAND ----------

# DBTITLE 1,6. Location preparation
# MAGIC %md
# MAGIC ## 6. Location preparation
# MAGIC Cleaned versions are added and the originals kept: `property_name_clean`, `street_name_clean`, `town_clean`, `postcode_clean`, `region_clean`, `local_authority_clean`, `latitude_clean`, `longitude_clean`, `easting_clean`/`northing_clean`, `ni_easting_clean`/`ni_northing_clean`.
# MAGIC * **Postcodes:** trimmed, upper-cased, and given the standard space before the last three characters when the format is valid.
# MAGIC * **Coordinates:** `coordinate_status` is one of `valid_uk`, `valid_international` (region = International), `zero`, `missing`, `possibly_swapped` or `out_of_range`. `has_valid_coordinates` = `valid_uk` or `valid_international`.
# MAGIC * **Grid references:** a zero Easting/Northing means 'not supplied'; non-zero values outside the grid are flagged as invalid.
# MAGIC * **`full_address`** = Property Number + Street Name, Town, Postcode (null parts skipped).
# MAGIC * **`location_key`** uses the strongest available basis, in order: Insite Property Reference, postcode + property name, full address (only if a street is present), rounded lat/lon. Town, postcode, region or local authority alone never form a key.
# MAGIC * **Conflicts** are checked against the most common town, region and local authority per postcode, and the most common region per local authority, across all datasets.
# MAGIC * **`location_quality_score`** runs 0–100: coordinates 30, postcode 20, name 10, street 10, town 5, region 5, local authority 5, grid reference 10, no conflict 5. `location_quality_notes` explains any missing points.

# COMMAND ----------

# DBTITLE 1,Prepare and score locations
def haversine_m(lat1, lon1, lat2, lon2):
    a = (F.pow(F.sin(F.radians(lat2 - lat1) / 2), 2)
         + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.pow(F.sin(F.radians(lon2 - lon1) / 2), 2))
    return F.lit(2 * 6371008.8) * F.asin(F.sqrt(F.least(F.lit(1.0), a)))

OFFSETS = F.array(*[F.struct(F.lit(i).alias("dy"), F.lit(j).alias("dx")) for i in (-1, 0, 1) for j in (-1, 0, 1)])
def grid_neighbour_join(left, right, l_y, l_x, r_y, r_x, cell_y, cell_x):
    """Spatial bucketing join: pairs every left row with right rows in its own and the 8 neighbouring grid cells."""
    L = (left.withColumn("_o", F.explode(OFFSETS))
             .withColumn("_cy", F.floor(F.col(l_y) / cell_y) + F.col("_o.dy"))
             .withColumn("_cx", F.floor(F.col(l_x) / cell_x) + F.col("_o.dx")).drop("_o"))
    R = right.withColumn("_cy", F.floor(F.col(r_y) / cell_y)).withColumn("_cx", F.floor(F.col(r_x) / cell_x))
    return L.join(R, ["_cy", "_cx"]).drop("_cy", "_cx")

# Known-answer test: 1 degree of latitude is ~111.2 km
_d = spark.range(1).select(haversine_m(F.lit(51.0), F.lit(-1.0), F.lit(52.0), F.lit(-1.0)).alias("d")).first()["d"]
assert abs(_d - 111195) < 100, _d

def prepare_location(df):
    pc_compact = F.regexp_replace(F.upper(F.col("postcode").cast("string")), r"[^A-Z0-9]", "")
    pc_valid = safe_bool(pc_compact.rlike(r"^[A-Z]{1,2}[0-9][A-Z0-9]?[0-9][A-Z]{2}$"))
    pc_fmt = F.concat(pc_compact.substr(F.lit(1), F.length(pc_compact) - 3), F.lit(" "), pc_compact.substr(F.length(pc_compact) - 2, F.lit(3)))
    pc_raw = F.regexp_replace(F.upper(F.trim(F.col("postcode").cast("string"))), r"\s+", " ")
    lat, lon = F.col("latitude").cast("double"), F.col("longitude").cast("double")
    in_uk = lat.between(UK_LAT_MIN, UK_LAT_MAX) & lon.between(UK_LON_MIN, UK_LON_MAX)
    intl = safe_bool(F.upper(F.col("region")).contains("INTERNATIONAL"))
    swapped = lon.between(UK_LAT_MIN, UK_LAT_MAX) & lat.between(UK_LON_MIN, UK_LON_MAX)
    coord_status = (F.when(lat.isNull() | lon.isNull(), "missing").when(lat == 0, "zero").when(in_uk, "valid_uk")
                     .when(intl & lat.between(-90, 90) & lon.between(-180, 180), "valid_international")
                     .when(swapped, "possibly_swapped").otherwise("out_of_range"))
    def grid_status(e_c, n_c, emin, emax, nmin, nmax):
        e, n = F.col(e_c).cast("double"), F.col(n_c).cast("double")
        return (F.when(e.isNull() | n.isNull(), "not_supplied").when((e == 0) & (n == 0), "not_supplied")
                 .when((e > emin) & (e <= emax) & (n > nmin) & (n <= nmax), "valid").otherwise("out_of_range"))
    rg = F.upper(F.col("region"))
    df = (df
        .withColumn("property_name_clean", match_text("property_name"))
        .withColumn("property_number_clean", clean_text("property_number"))
        .withColumn("street_name_clean", match_text("street_name"))
        .withColumn("town_clean", match_text("town"))
        .withColumn("region_clean", match_text("region"))
        .withColumn("local_authority_clean", match_text("local_authority"))
        .withColumn("postcode_valid_format_flag", pc_valid)
        .withColumn("postcode_clean", F.when(pc_valid, pc_fmt).when(pc_raw != "", pc_raw))
        .withColumn("postcode_outward", F.when(pc_valid, pc_compact.substr(F.lit(1), F.length(pc_compact) - 3)))
        .withColumn("postcode_area", F.when(pc_valid, F.regexp_extract(pc_compact, r"^([A-Z]{1,2})", 1)))
        .withColumn("coordinate_status", coord_status)
        .withColumn("has_valid_coordinates", F.col("coordinate_status").isin("valid_uk", "valid_international"))
        .withColumn("latitude_clean", F.when(F.col("has_valid_coordinates"), lat))
        .withColumn("longitude_clean", F.when(F.col("has_valid_coordinates"), lon))
        .withColumn("gb_grid_status", grid_status("easting", "northing", GB_E_MIN, GB_E_MAX, GB_N_MIN, GB_N_MAX))
        .withColumn("ni_grid_status", grid_status("ni_easting", "ni_northing", NI_E_MIN, NI_E_MAX, NI_N_MIN, NI_N_MAX))
        .withColumn("easting_clean", F.when(F.col("gb_grid_status") == "valid", F.col("easting").cast("bigint")))
        .withColumn("northing_clean", F.when(F.col("gb_grid_status") == "valid", F.col("northing").cast("bigint")))
        .withColumn("ni_easting_clean", F.when(F.col("ni_grid_status") == "valid", F.col("ni_easting").cast("bigint")))
        .withColumn("ni_northing_clean", F.when(F.col("ni_grid_status") == "valid", F.col("ni_northing").cast("bigint")))
        .withColumn("grid_reference_status", F.when(F.col("gb_grid_status") == "valid", "gb_valid")
                    .when(F.col("ni_grid_status") == "valid", "ni_valid")
                    .when((F.col("gb_grid_status") == "out_of_range") | (F.col("ni_grid_status") == "out_of_range"), "out_of_range")
                    .otherwise("not_supplied"))
        .withColumn("invalid_easting_northing_flag", (F.col("gb_grid_status") == "out_of_range") | (F.col("ni_grid_status") == "out_of_range"))
        .withColumn("uk_nation", F.when(rg.contains("SCOTLAND"), "Scotland").when(rg.contains("WALES"), "Wales")
                    .when(rg.contains("NORTHERN IRELAND"), "Northern Ireland").when(rg.contains("INTERNATIONAL"), "International")
                    .when(rg.isNotNull(), "England")))
    num_street = F.concat_ws(" ", clean_text("property_number"), clean_text("street_name"))
    fa = F.concat_ws(", ", F.when(num_street != "", num_street), clean_text("town"), F.col("postcode_clean"))
    df = df.withColumn("full_address", F.when(fa != "", fa))
    df = df.withColumn("full_address_clean", match_text("full_address"))
    ref = F.trim(F.col(REF).cast("string"))
    df = df.withColumn("location_key_basis",
        F.when(ref.isNotNull() & (ref != ""), "insite_property_reference")
         .when(F.col("postcode_valid_format_flag") & F.col("property_name_clean").isNotNull(), "postcode_and_property_name")
         .when(F.col("street_name_clean").isNotNull() & F.col("full_address_clean").isNotNull(), "full_address")
         .when(F.col("has_valid_coordinates"), "rounded_lat_lon").otherwise("none"))
    b = F.col("location_key_basis")
    return df.withColumn("location_key",
        F.when(b == "insite_property_reference", F.concat(F.lit("REF:"), ref))
         .when(b == "postcode_and_property_name", F.concat(F.lit("PCN:"), F.col("postcode_clean"), F.lit("|"), F.col("property_name_clean")))
         .when(b == "full_address", F.concat(F.lit("ADDR:"), F.col("full_address_clean")))
         .when(b == "rounded_lat_lon", F.concat(F.lit("LL:"), F.format_string("%.4f,%.4f", F.col("latitude_clean"), F.col("longitude_clean")))))

located0 = {ds: prepare_location(spark.table(T(f"stg_{ds}_clean"))) for ds in SOURCE_TABLES_FOUND}

# --- Cross-dataset location conflicts ----------------------------------------
CONFLICT_CHECKS = [
    ("postcode_clean", "town_clean", "postcode_town_conflict_flag", "town differs from the most common town for this postcode"),
    ("postcode_clean", "region_clean", "postcode_region_conflict_flag", "region differs from the most common region for this postcode"),
    ("postcode_clean", "local_authority_clean", "postcode_local_authority_conflict_flag", "local authority differs from the most common local authority for this postcode"),
    ("local_authority_clean", "region_clean", "local_authority_region_conflict_flag", "region differs from the most common region for this local authority")]
u_cols = ["postcode_clean", "postcode_valid_format_flag", "town_clean", "region_clean", "local_authority_clean"]
loc_union = reduce(lambda a, b: a.unionByName(b), [d.select(*u_cols) for d in located0.values()])
mode_parts = []
for key, attr, flag, _ in CONFLICT_CHECKS:
    base = loc_union.filter(F.col(key).isNotNull() & F.col(attr).isNotNull())
    if key == "postcode_clean":
        base = base.filter("postcode_valid_format_flag")
    cnt = base.groupBy(key, attr).agg(F.count("*").alias("_n"))
    w = W.partitionBy(key).orderBy(F.desc("_n"), F.col(attr))
    mode_parts.append(cnt.withColumn("_rk", F.row_number().over(w)).withColumn("n_values", F.count("*").over(W.partitionBy(key)))
                        .filter("_rk = 1").select(F.lit(flag).alias("check_flag"), F.col(key).alias("key_value"),
                                                  F.col(attr).alias("mode_value"), "n_values"))
modes_all = save_delta(reduce(lambda a, b: a.unionByName(b), mode_parts), "stg_location_modes")

def apply_conflicts(df):
    note_cols = []
    for i, (key, attr, flag, msg) in enumerate(CONFLICT_CHECKS):
        m = modes_all.filter(F.col("check_flag") == flag).select(F.col("key_value").alias(f"_k{i}"), F.col("mode_value").alias(f"_m{i}"), F.col("n_values").alias(f"_nv{i}"))
        df = df.join(m, F.col(key) == F.col(f"_k{i}"), "left")
        cond = (F.col(f"_nv{i}") > 1) & F.col(attr).isNotNull() & (F.col(attr) != F.col(f"_m{i}"))
        if key == "postcode_clean":
            cond = cond & F.col("postcode_valid_format_flag")
        df = (df.withColumn(flag, safe_bool(cond))
                .withColumn(f"_note{i}", F.when(F.col(flag), F.concat(F.lit(msg + " ("), F.col(attr), F.lit(" vs "), F.col(f"_m{i}"), F.lit(")"))))
                .drop(f"_k{i}", f"_m{i}", f"_nv{i}"))
        note_cols.append(f"_note{i}")
    if "country_clean" in df.columns:
        cr = F.col("country_clean").isin("England", "Scotland", "Wales", "Northern Ireland") & F.col("uk_nation").isNotNull() & (F.col("country_clean") != F.col("uk_nation"))
        df = (df.withColumn("country_region_conflict_flag", safe_bool(cr))
                .withColumn("_note9", F.when(F.col("country_region_conflict_flag"), F.concat(F.lit("country ("), F.col("country_clean"), F.lit(") conflicts with region-derived nation ("), F.col("uk_nation"), F.lit(")")))))
        note_cols.append("_note9")
    else:
        df = df.withColumn("country_region_conflict_flag", F.lit(False))
    flags = [c[2] for c in CONFLICT_CHECKS] + ["country_region_conflict_flag"]
    notes = F.concat_ws("; ", *[F.col(c) for c in note_cols])
    return (df.withColumn("location_conflict_flag", reduce(lambda a, b: a | b, [F.col(f) for f in flags]))
              .withColumn("location_conflict_notes", F.when(notes != "", notes)).drop(*note_cols))

def add_location_quality(df):
    comps = [
        (F.col("has_valid_coordinates"), 30, F.concat(F.lit("no valid coordinates ("), F.col("coordinate_status"), F.lit(")"))),
        (F.col("postcode_valid_format_flag"), 20, F.when(F.col("postcode_clean").isNull(), F.lit("postcode missing")).otherwise(F.lit("postcode format invalid"))),
        (F.col("property_name_clean").isNotNull(), 10, F.lit("property name missing")),
        (F.col("street_name_clean").isNotNull(), 10, F.lit("street name missing")),
        (F.col("town_clean").isNotNull(), 5, F.lit("town missing")),
        (F.col("region_clean").isNotNull(), 5, F.lit("region missing")),
        (F.col("local_authority_clean").isNotNull(), 5, F.lit("local authority missing")),
        (F.col("grid_reference_status").isin("gb_valid", "ni_valid"), 10, F.concat(F.lit("no valid grid reference ("), F.col("grid_reference_status"), F.lit(")"))),
        (~F.col("location_conflict_flag"), 5, F.lit("location attributes conflict with other records"))]
    score = reduce(lambda a, b: a + b, [F.when(c, F.lit(p)).otherwise(F.lit(0)) for c, p, _ in comps])
    notes = F.concat_ws("; ", *[F.when(~c, n) for c, _, n in comps])
    return (df.withColumn("location_quality_score", score)
              .withColumn("location_quality_notes", F.when(notes != "", notes).otherwise(F.lit("no location issues detected"))))

for ds, df in located0.items():
    save_delta(add_location_quality(apply_conflicts(df)), f"stg_{ds}_located")

LOCATION_COLS = LOC + [
    "property_name_clean", "property_number_clean", "street_name_clean", "town_clean", "postcode_clean",
    "postcode_valid_format_flag", "postcode_outward", "postcode_area", "region_clean", "local_authority_clean", "uk_nation",
    "latitude_clean", "longitude_clean", "coordinate_status", "has_valid_coordinates",
    "easting_clean", "northing_clean", "ni_easting_clean", "ni_northing_clean", "gb_grid_status", "ni_grid_status",
    "grid_reference_status", "invalid_easting_northing_flag", "full_address", "full_address_clean",
    "location_key", "location_key_basis", "location_quality_score", "location_quality_notes",
    "location_conflict_flag", "location_conflict_notes"] + [c[2] for c in CONFLICT_CHECKS] + ["country_region_conflict_flag"]

loc_summary = reduce(lambda a, b: a.unionByName(b), [
    spark.table(T(f"stg_{ds}_located")).groupBy(F.lit(ds).alias("dataset"), "coordinate_status").agg(
        F.count("*").alias("rows"),
        F.sum(F.col("postcode_valid_format_flag").cast("int")).alias("valid_postcodes"),
        F.sum(F.col("location_conflict_flag").cast("int")).alias("location_conflicts"),
        F.sum(F.col("invalid_easting_northing_flag").cast("int")).alias("invalid_grid_refs"),
        F.round(F.avg("location_quality_score"), 1).alias("avg_location_quality"))
    for ds in SOURCE_TABLES_FOUND])
display(loc_summary.orderBy("dataset", "coordinate_status"))
display(spark.table(T("stg_property_located")).select(REF, "property_name", "postcode", "postcode_clean", "full_address", "location_key", "location_key_basis", "coordinate_status", "location_quality_score", "location_quality_notes").limit(10))

# COMMAND ----------

# DBTITLE 1,7. Property matching
# MAGIC %md
# MAGIC ## 7. Property matching
# MAGIC **Property base:** one row per Insite Property Reference. The register row with the best location quality supplies the location; contract-level attributes are aggregated. References found only in occupation, vacant-space, surplus or building records are added with `property_in_register = false`.
# MAGIC
# MAGIC **Primary match, Exact:** on Insite Property Reference. For land, the reference is the first 8 characters of `insite_land_reference` (method `insite_land_reference_prefix`).
# MAGIC
# MAGIC **Secondary matching** applies only to records with no reference, or whose reference is not in the property base:
# MAGIC * **Postcode + property name:** High if unique; Medium if the nearest candidate is within 25 m and the next is further than 25 m; otherwise Low.
# MAGIC * **Full address** (street required): High if unique; otherwise as above.
# MAGIC * **Lat/lon proximity** (within 100 m): Medium if within 25 m, unique there, and the postcode or name agrees; otherwise Low.
# MAGIC * **Easting/Northing proximity** (GB or NI grid): the same rule as lat/lon.
# MAGIC * **Upgrade:** a match moves up one level (at most to High) when a text-based method and a coordinate-based method point to the same property.
# MAGIC
# MAGIC Only Exact, High and Medium matches populate `linked_property_reference` and are used in property-level totals. Low matches are kept as `candidate_property_reference` with `review_required_flag = true`. No method ever links records on postcode, town, region or local authority alone.

# COMMAND ----------

# DBTITLE 1,Build one-row-per-property base
P = spark.table(T("stg_property_located"))
w_best = W.partitionBy(REF).orderBy(F.desc("location_quality_score"), F.desc("has_valid_coordinates"), "source_row_id")
best = P.withColumn("_rk", F.row_number().over(w_best)).filter("_rk = 1")
reg_agg = P.groupBy(REF).agg(
    F.count("*").alias("property_register_row_count"),
    F.countDistinct("insite_contract_reference").alias("contract_count"),
    jset("insite_contract_reference").alias("contract_references"),
    jset("contract_sector").alias("contract_sector"),
    jset("parent_organisation").alias("parent_organisation"),
    jset("organisation").alias("organisation"),
    F.countDistinct("organisation").alias("organisation_count"),
    jset("tenure").alias("tenure"), jset("asset_type").alias("asset_type"), jset("usage").alias("usage"),
    jset("land_usage").alias("land_usage"), jset("security_classification").alias("security_classification"),
    jset("country_clean").alias("country_clean"), jset("uprn").alias("uprn"),
    F.countDistinct(F.round("latitude_clean", 4), F.round("longitude_clean", 4)).alias("distinct_coordinate_count"),
    F.array_sort(F.collect_set("source_row_id")).alias("property_source_row_ids"))
reg_base = (best.select(REF, *LOCATION_COLS, F.col("source_row_id").alias("location_source_row_id"),
                        F.col("source_dataset").alias("location_source_dataset"))
                .withColumn("property_in_register", F.lit(True))
                .join(reg_agg, REF, "left"))

# References that appear only in other datasets (not in the property register)
OTHER_REF_DATASETS = [d for d in ["occupation", "vacant_space", "surplus", "building"] if d in SOURCE_TABLES_FOUND]
oth = reduce(lambda a, b: a.unionByName(b), [
    spark.table(T(f"stg_{d}_located")).filter(F.col(REF).isNotNull())
         .select(REF, *LOCATION_COLS, "source_row_id", "source_dataset", "contract_sector", "parent_organisation", "organisation", "security_classification")
    for d in OTHER_REF_DATASETS]).join(reg_base.select(REF), REF, "left_anti")
w2 = W.partitionBy(REF).orderBy(F.desc("location_quality_score"), F.desc("has_valid_coordinates"), "source_dataset", "source_row_id")
oth_best = oth.withColumn("_rk", F.row_number().over(w2)).filter("_rk = 1")
oth_agg = oth.groupBy(REF).agg(
    jset("contract_sector").alias("contract_sector"), jset("parent_organisation").alias("parent_organisation"),
    jset("organisation").alias("organisation"), F.countDistinct("organisation").alias("organisation_count"),
    jset("security_classification").alias("security_classification"),
    F.countDistinct(F.round("latitude_clean", 4), F.round("longitude_clean", 4)).alias("distinct_coordinate_count"),
    jset("source_dataset").alias("non_register_source_datasets"))
extra_base = (oth_best.select(REF, *LOCATION_COLS, F.col("source_row_id").alias("location_source_row_id"),
                              F.col("source_dataset").alias("location_source_dataset"))
                      .withColumn("property_in_register", F.lit(False)).join(oth_agg, REF, "left"))

property_base = (reg_base.unionByName(extra_base, allowMissingColumns=True)
    .withColumn("property_register_row_count", F.coalesce("property_register_row_count", F.lit(0)))
    .withColumn("property_duplicate_ref_flag", F.col("property_register_row_count") > 1)
    .withColumn("property_conflicting_coordinates_flag", F.coalesce(F.col("distinct_coordinate_count") > 1, F.lit(False)))
    .withColumn("property_match_method", F.when(F.col("property_in_register"), F.lit("insite_property_reference (property register)"))
                .otherwise(F.lit("insite_property_reference (non-register sources only)")))
    .withColumn("property_match_confidence", F.lit("Exact"))
    .withColumn("property_match_notes", F.when(F.col("property_in_register"),
                    F.concat(F.lit("Property register reference; location from "), F.col("location_source_row_id"),
                             F.when(F.col("property_duplicate_ref_flag"), F.concat(F.lit("; "), F.col("property_register_row_count").cast("string"), F.lit(" register rows (one per contract) combined"))).otherwise(F.lit(""))))
                .otherwise(F.concat(F.lit("Reference not in property register; built from "), F.col("non_register_source_datasets")))))
PB = save_delta(property_base, "stg_property_base")
display(PB.groupBy("property_in_register").agg(F.count("*").alias("properties"),
        F.sum(F.col("property_duplicate_ref_flag").cast("int")).alias("with_multiple_register_rows"),
        F.sum(F.col("property_conflicting_coordinates_flag").cast("int")).alias("conflicting_coordinates"),
        F.sum(F.col("has_valid_coordinates").cast("int")).alias("with_valid_coordinates")))

# COMMAND ----------

# DBTITLE 1,Exact and secondary matching to properties
NEAR, MAXD = float(MATCH_NEAR_M), float(MATCH_MAX_M)
M = PB.select(F.col(REF).alias("m_ref"), F.col("postcode_clean").alias("m_pc"), F.col("property_name_clean").alias("m_name"),
              F.col("full_address_clean").alias("m_addr"), F.col("latitude_clean").alias("m_lat"), F.col("longitude_clean").alias("m_lon"),
              F.col("easting_clean").alias("m_e"), F.col("northing_clean").alias("m_n"),
              F.col("ni_easting_clean").alias("m_nie"), F.col("ni_northing_clean").alias("m_nin"))
pb_refs = PB.select(F.col(REF).alias("_ref"), F.col("property_in_register").alias("_in_register"))
LINKED_DATASETS = [d for d in ["occupation", "vacant_space", "surplus", "building", "land"] if d in SOURCE_TABLES_FOUND]

exact_parts, cand_parts = [], []
for ds in LINKED_DATASETS:
    d = spark.table(T(f"stg_{ds}_located")).join(pb_refs, F.col(REF) == F.col("_ref"), "left")
    method = "insite_land_reference_prefix" if ds == "land" else "insite_property_reference"
    exact_parts.append(d.filter(F.col("_ref").isNotNull()).select(
        "source_dataset", "source_row_id", F.col(REF).alias("candidate_property_reference"),
        F.lit(method).alias("property_match_method"), F.lit("Exact").alias("property_match_confidence"),
        F.lit(None).cast("double").alias("match_distance_m"), F.lit(1).cast("long").alias("match_candidate_count"),
        F.array(F.lit(method)).alias("agreeing_methods"),
        F.when(F.col("_in_register"), F.lit("Reference found in property register"))
         .otherwise(F.lit("Reference not in property register; matched to property built from non-register sources")).alias("property_match_notes")))
    cand_parts.append(d.filter(F.col("_ref").isNull()).select(
        "source_dataset", F.col("source_row_id").alias("cid"), F.col("postcode_clean").alias("c_pc"),
        F.col("postcode_valid_format_flag").alias("c_pc_valid"), F.col("property_name_clean").alias("c_name"),
        F.col("street_name_clean").alias("c_street"), F.col("full_address_clean").alias("c_addr"),
        F.col("latitude_clean").alias("c_lat"), F.col("longitude_clean").alias("c_lon"),
        F.col("easting_clean").alias("c_e"), F.col("northing_clean").alias("c_n"),
        F.col("ni_easting_clean").alias("c_nie"), F.col("ni_northing_clean").alias("c_nin"), F.col(REF).alias("c_source_ref")))
exact_df = reduce(lambda a, b: a.unionByName(b), exact_parts)
C = save_delta(reduce(lambda a, b: a.unionByName(b), cand_parts), "stg_match_candidates")
print(f"Records needing secondary matching: {C.count():,}")

def rank_pairs(pairs, method):
    w = W.partitionBy("cid").orderBy(F.col("dist_m").asc_nulls_last(), "m_ref")
    return (pairs.withColumn("n_candidates", F.count(F.lit(1)).over(W.partitionBy("cid")))
                 .withColumn("_rk", F.row_number().over(w)).withColumn("second_dist_m", F.lead("dist_m").over(w))
                 .filter("_rk = 1").drop("_rk").withColumn("method", F.lit(method)))

def text_conf():
    return (F.when(F.col("n_candidates") == 1, F.lit("High"))
             .when((F.col("dist_m") <= NEAR) & (F.col("second_dist_m") > NEAR), F.lit("Medium")).otherwise(F.lit("Low")))

def dist_txt():
    return F.when(F.col("dist_m").isNotNull(), F.format_string("; nearest %.0f m", F.col("dist_m"))).otherwise(F.lit("; distance unavailable"))

def spatial_conf():
    agree = safe_bool(F.col("c_pc") == F.col("m_pc")) | safe_bool(F.col("c_name") == F.col("m_name"))
    return (F.when((F.col("dist_m") <= NEAR) & agree & (F.col("second_dist_m").isNull() | (F.col("second_dist_m") > NEAR)), F.lit("Medium"))
             .otherwise(F.lit("Low")), agree)

with_dist = lambda df: df.withColumn("dist_m", haversine_m(F.col("c_lat"), F.col("c_lon"), F.col("m_lat"), F.col("m_lon")))
# A: postcode + name
bA = rank_pairs(with_dist(C.filter("c_pc_valid AND c_name IS NOT NULL").join(M, (F.col("c_pc") == F.col("m_pc")) & (F.col("c_name") == F.col("m_name")))), "postcode_and_name")
bA = bA.withColumn("confidence", text_conf()).withColumn("note", F.concat(F.lit("Cleaned postcode and property name match "), F.col("n_candidates").cast("string"), F.lit(" property record(s)"), dist_txt()))
# B: full address (street required)
bB = rank_pairs(with_dist(C.filter("c_pc_valid AND c_street IS NOT NULL AND c_addr IS NOT NULL").join(M, F.col("c_addr") == F.col("m_addr"))), "full_address")
bB = bB.withColumn("confidence", text_conf()).withColumn("note", F.concat(F.lit("Cleaned full address matches "), F.col("n_candidates").cast("string"), F.lit(" property record(s)"), dist_txt()))
# C: lat/lon proximity
pC = with_dist(grid_neighbour_join(C.filter("c_lat IS NOT NULL"), M.filter("m_lat IS NOT NULL"), "c_lat", "c_lon", "m_lat", "m_lon", 0.001, 0.002)).filter(F.col("dist_m") <= MAXD)
bC = rank_pairs(pC, "lat_lon_proximity")
confC, agreeC = spatial_conf()
bC = bC.withColumn("confidence", confC).withColumn("note", F.concat(F.lit("Nearest property by latitude/longitude at "), F.format_string("%.0f m", F.col("dist_m")),
        F.lit(" ("), F.col("n_candidates").cast("string"), F.lit(f" within {MATCH_MAX_M:.0f} m)"), F.when(agreeC, F.lit("; postcode or name agrees")).otherwise(F.lit("; postcode and name do not agree"))))
# D: Easting/Northing proximity (GB and NI grids)
def en_pairs(ec, nc, mec, mnc):
    p = grid_neighbour_join(C.filter(F.col(ec).isNotNull()), M.filter(F.col(mec).isNotNull()), nc, ec, mnc, mec, 100.0, 100.0)
    return p.withColumn("dist_m", F.sqrt(F.pow(F.col(ec) - F.col(mec), 2) + F.pow(F.col(nc) - F.col(mnc), 2))).filter(F.col("dist_m") <= MAXD)
bD = rank_pairs(en_pairs("c_e", "c_n", "m_e", "m_n").unionByName(en_pairs("c_nie", "c_nin", "m_nie", "m_nin")).dropDuplicates(["cid", "m_ref"]), "easting_northing_proximity")
confD, agreeD = spatial_conf()
bD = bD.withColumn("confidence", confD).withColumn("note", F.concat(F.lit("Nearest property by Easting/Northing at "), F.format_string("%.0f m", F.col("dist_m")),
        F.lit(" ("), F.col("n_candidates").cast("string"), F.lit(f" within {MATCH_MAX_M:.0f} m)"), F.when(agreeD, F.lit("; postcode or name agrees")).otherwise(F.lit("; postcode and name do not agree"))))

sel = ["cid", "m_ref", "method", "confidence", "dist_m", "n_candidates", "note"]
allm = bA.select(*sel).unionByName(bB.select(*sel)).unionByName(bC.select(*sel)).unionByName(bD.select(*sel))
allm = allm.join(allm.groupBy("cid", "m_ref").agg(F.array_sort(F.collect_set("method")).alias("agreeing_methods")), ["cid", "m_ref"])
text_m = F.arrays_overlap(F.col("agreeing_methods"), F.array(F.lit("postcode_and_name"), F.lit("full_address")))
spat_m = F.arrays_overlap(F.col("agreeing_methods"), F.array(F.lit("lat_lon_proximity"), F.lit("easting_northing_proximity")))
allm = (allm.withColumn("confidence_final", F.when(text_m & spat_m & (F.col("confidence") == "Medium"), F.lit("High"))
                        .when(text_m & spat_m & (F.col("confidence") == "Low"), F.lit("Medium")).otherwise(F.col("confidence")))
            .withColumn("note", F.when(F.col("confidence_final") != F.col("confidence"),
                        F.concat(F.col("note"), F.lit("; upgraded one level because text-based and coordinate-based methods agree"))).otherwise(F.col("note"))))
CONF_RANK = F.create_map(*[x for k, v in {"Exact": 5, "High": 4, "Medium": 3, "Low": 2}.items() for x in (F.lit(k), F.lit(v))])
METH_RANK = F.create_map(*[x for k, v in {"postcode_and_name": 1, "full_address": 2, "lat_lon_proximity": 3, "easting_northing_proximity": 4}.items() for x in (F.lit(k), F.lit(v))])
wb = W.partitionBy("cid").orderBy(CONF_RANK[F.col("confidence_final")].desc(), F.size("agreeing_methods").desc(),
                                  METH_RANK[F.col("method")].asc(), F.col("dist_m").asc_nulls_last(), F.col("m_ref"))
best_sec = allm.withColumn("_rk", F.row_number().over(wb)).filter("_rk = 1")

sec = (C.select("source_dataset", "cid", "c_source_ref").join(best_sec, "cid", "left")
        .select("source_dataset", F.col("cid").alias("source_row_id"), F.col("m_ref").alias("candidate_property_reference"),
                F.coalesce(F.col("method"), F.lit("none")).alias("property_match_method"),
                F.coalesce(F.col("confidence_final"), F.lit("Unmatched")).alias("property_match_confidence"),
                F.col("dist_m").alias("match_distance_m"), F.col("n_candidates").cast("long").alias("match_candidate_count"),
                F.coalesce(F.col("agreeing_methods"), F.array().cast("array<string>")).alias("agreeing_methods"),
                F.concat(F.when(F.col("c_source_ref").isNotNull(), F.concat(F.lit("Source reference "), F.col("c_source_ref"), F.lit(" not found in property base; "))).otherwise(F.lit("No property reference; ")),
                         F.coalesce(F.col("note"), F.lit(f"no secondary match (postcode+name, address, or coordinates within {MATCH_MAX_M:.0f} m)"))).alias("property_match_notes")))

src_info = reduce(lambda a, b: a.unionByName(b), [
    spark.table(T(f"stg_{ds}_located")).select("source_row_id", "record_reference", F.col(REF).alias("source_insite_property_reference"),
        "property_name", "full_address", "postcode_clean", "latitude_clean", "longitude_clean", "location_quality_score")
    for ds in LINKED_DATASETS])
m_info = PB.select(F.col(REF).alias("candidate_property_reference"), F.col("property_name").alias("matched_property_name"),
                   F.col("full_address").alias("matched_full_address"), F.col("postcode_clean").alias("matched_postcode"),
                   F.col("latitude_clean").alias("matched_latitude"), F.col("longitude_clean").alias("matched_longitude"))
matches = (exact_df.unionByName(sec).join(src_info, "source_row_id", "left").join(m_info, "candidate_property_reference", "left")
    .withColumn("linked_property_reference", F.when(F.col("property_match_confidence").isin("Exact", "High", "Medium"), F.col("candidate_property_reference")))
    .withColumn("review_required_flag", F.col("property_match_confidence").isin("Low", "Unmatched")))
matches = save_delta(matches.select("source_dataset", "source_row_id", "record_reference", "source_insite_property_reference",
    "property_name", "full_address", "postcode_clean", "latitude_clean", "longitude_clean", "location_quality_score",
    "candidate_property_reference", "linked_property_reference", "property_match_method", "property_match_confidence",
    "property_match_notes", "match_distance_m", "match_candidate_count", "agreeing_methods", "review_required_flag",
    "matched_property_name", "matched_full_address", "matched_postcode", "matched_latitude", "matched_longitude"),
    "property_location_matches", "Match of every non-register record to a property, with method, confidence and notes")
display(matches.groupBy("source_dataset", "property_match_method", "property_match_confidence").count().orderBy("source_dataset", "property_match_confidence"))

# COMMAND ----------

# DBTITLE 1,8. Area conversion
# MAGIC %md
# MAGIC ## 8. Area conversion
# MAGIC All area fields and their unit/basis columns are profiled before any totals are calculated. The standard measures are `occupied_area_sqm`, `vacant_area_sqm`, `surplus_floor_area_sqm` and `surplus_land_area_hectares`, plus `property_floor_area_sqm`, `property_land_area_hectares`, `building_floor_area_sqm` and `land_area_hectares`.
# MAGIC * Each measure has a `*_unit_status`: `assumed_sqm_ipms`, `sqm`, `sqft` (converted ×0.0929), `hectares`, `acres`, `assumed_sqm_no_unit_field`, `assumed_hectares_no_unit_field`, or `unit_unknown`.
# MAGIC * `unit_unknown` values are kept in the raw column but left out of converted totals, never assumed to be sqm.
# MAGIC * `*_basis` records the IPMS basis, and `*_value_status` is `missing`, `zero`, `negative` or `present`.
# MAGIC * The clean tables are written here, each with its property-match columns.

# COMMAND ----------

# DBTITLE 1,Profile area fields and units
AREA_FIELDS = [("property", "floor_area", "floor_area_measurement"), ("property", "total_land_area", None),
               ("building", "floor_area", "floor_area_measurement"), ("occupation", "floor_area", "floor_area_measurement"),
               ("vacant_space", "vacant_area", "vacant_area_units"), ("surplus", "total_surplus_floor_area", None),
               ("surplus", "total_surplus_land_area", None), ("land", "total_land_area", None)]
parts = []
for ds, v, u in AREA_FIELDS:
    if ds not in SOURCE_TABLES_FOUND:
        continue
    d = spark.table(T(f"stg_{ds}_located"))
    parts.append(d.groupBy(F.lit(ds).alias("dataset"), F.lit(v).alias("area_field"),
                           (F.col(u).cast("string") if u else F.lit("(no unit field)")).alias("unit_or_basis"))
                  .agg(F.count("*").alias("rows"), F.sum(F.col(v).isNull().cast("int")).alias("null_values"),
                       F.sum((F.col(v) == 0).cast("int")).alias("zero_values"), F.sum((F.col(v) < 0).cast("int")).alias("negative_values"),
                       F.min(v).alias("min"), F.percentile_approx(v, 0.5).alias("median"), F.max(v).alias("max")))
display(reduce(lambda a, b: a.unionByName(b), parts).orderBy("dataset", "area_field", "unit_or_basis"))

# COMMAND ----------

# DBTITLE 1,Convert areas and write clean tables
SQM_FACTOR = {"assumed_sqm_ipms": 1.0, "sqm": 1.0, "assumed_sqm_no_unit_field": 1.0, "sqft": 0.09290304,
              "hectares": 10000.0, "acres": 4046.8564224}
HA_FACTOR = {"assumed_hectares_no_unit_field": 1.0, "hectares": 1.0, "sqm": 0.0001, "assumed_sqm_ipms": 0.0001,
             "sqft": 0.09290304 / 10000, "acres": 0.40468564224}

def unit_status_expr(unit_col):
    u = F.upper(F.trim(F.col(unit_col).cast("string")))
    return (F.when(u.isNull() | (u == ""), F.lit("unit_unknown"))
             .when(u.rlike("IPMS"), F.lit("assumed_sqm_ipms" if ASSUME_IPMS_IS_SQM else "unit_unknown"))
             .when(u.rlike(r"SQ\.?\s*F|FT2|SQUARE\s*F"), F.lit("sqft"))
             .when(u.rlike(r"SQ\.?\s*M|M2|SQUARE\s*M|SQM"), F.lit("sqm"))
             .when(u.rlike(r"HECTARE|^HA$"), F.lit("hectares"))
             .when(u.rlike("ACRE"), F.lit("acres")).otherwise(F.lit("unit_unknown")))

def ipms_basis(unit_col):
    u = F.upper(F.col(unit_col).cast("string"))
    return (F.when(u.rlike(r"IPMS\s*1"), "IPMS 1 Gross Floor Area").when(u.rlike(r"IPMS\s*2"), "IPMS 2 Gross Internal Area")
             .when(u.rlike(r"IPMS\s*3"), "IPMS 3 Net Internal Area"))

def value_status(v):
    c = F.col(v)
    return F.when(c.isNull(), "missing").when(c < 0, "negative").when(c == 0, "zero").otherwise("present")

def convert(v, status_col, factors):
    e = None
    for k, f in factors.items():
        cond = F.col(status_col) == k
        e = F.when(cond, F.col(v) * f) if e is None else e.when(cond, F.col(v) * f)
    return F.when(F.col(v) >= 0, e)

def add_area(df, v, unit_col, prefix, target, fixed_status=None):
    st = F.lit(fixed_status) if fixed_status else unit_status_expr(unit_col)
    df = (df.withColumn(f"{prefix}_unit_status", st)
            .withColumn(f"{prefix}_value_status", value_status(v))
            .withColumn(f"{prefix}_basis", ipms_basis(unit_col) if unit_col else F.lit(None).cast("string")))
    return df.withColumn(f"{prefix}_{target}", F.round(convert(v, f"{prefix}_unit_status", SQM_FACTOR if target == "sqm" else HA_FACTOR), 4))

HA_FIXED = "assumed_hectares_no_unit_field"
AREA_SPECS = {
    "property": [("floor_area", "floor_area_measurement", "property_floor_area", "sqm", None),
                 ("total_land_area", None, "property_land_area", "hectares", HA_FIXED if ASSUME_LAND_AREA_IS_HECTARES else "unit_unknown")],
    "building": [("floor_area", "floor_area_measurement", "building_floor_area", "sqm", None)],
    "occupation": [("floor_area", "floor_area_measurement", "occupied_area", "sqm", None)],
    "vacant_space": [("vacant_area", "vacant_area_units", "vacant_area", "sqm", None)],
    "surplus": [("total_surplus_floor_area", None, "surplus_floor_area", "sqm", "assumed_sqm_no_unit_field" if ASSUME_SURPLUS_FLOOR_AREA_IS_SQM else "unit_unknown"),
                ("total_surplus_land_area", None, "surplus_land_area", "hectares", HA_FIXED if ASSUME_SURPLUS_LAND_AREA_IS_HECTARES else "unit_unknown")],
    "land": [("total_land_area", None, "land_area", "hectares", HA_FIXED if ASSUME_LAND_AREA_IS_HECTARES else "unit_unknown")],
}
MATCH_COLS = ["candidate_property_reference", "linked_property_reference", "property_match_method", "property_match_confidence",
              "property_match_notes", "match_distance_m", "review_required_flag"]
match_sel = spark.table(T("property_location_matches")).select("source_row_id", *MATCH_COLS)

area_summary = []
for ds in SOURCE_TABLES_FOUND:
    d = spark.table(T(f"stg_{ds}_located"))
    for v, u, prefix, target, fixed in AREA_SPECS.get(ds, []):
        d = add_area(d, v, u, prefix, target, fixed)
        area_summary.append((ds, prefix, target))
    if ds == "vacant_space":
        d = d.withColumn("vacant_zero_area_flag", safe_bool(F.col("vacant_area") == 0))
    if ds == "property":
        d = (d.withColumn("candidate_property_reference", F.col(REF)).withColumn("linked_property_reference", F.col(REF))
              .withColumn("property_match_method", F.lit("insite_property_reference (property register)"))
              .withColumn("property_match_confidence", F.lit("Exact"))
              .withColumn("property_match_notes", F.lit("Property register record"))
              .withColumn("match_distance_m", F.lit(None).cast("double")).withColumn("review_required_flag", F.lit(False)))
    else:
        d = d.join(match_sel, "source_row_id", "left")
    save_delta(d, f"{ds}_clean", f"Cleaned {ds} records with location, data-quality, area and property-match columns (one row per source row)")

unit_rows = reduce(lambda a, b: a.unionByName(b), [
    spark.table(T(f"{ds}_clean")).groupBy(F.lit(ds).alias("dataset"), F.lit(f"{prefix}_{target}").alias("measure"),
                                         F.col(f"{prefix}_unit_status").alias("unit_status"), F.col(f"{prefix}_basis").alias("basis"))
        .agg(F.count("*").alias("rows"), F.sum(F.col(f"{prefix}_{target}").isNotNull().cast("int")).alias("converted_rows"),
             F.round(F.sum(f"{prefix}_{target}"), 1).alias("converted_total"))
    for ds, prefix, target in area_summary])
display(unit_rows.orderBy("dataset", "measure", "unit_status"))

# COMMAND ----------

# DBTITLE 1,9. Feature engineering
# MAGIC %md
# MAGIC ## 9. Feature engineering
# MAGIC Property-level indicators. Totals use only Exact, High and Medium links and exclude exact-duplicate copies. The drill-down arrays `*_source_row_ids` keep every record traceable.
# MAGIC 1. **`vacant_area_ratio`** = vacant / (occupied + vacant) where the occupied area is known. If no occupation area is recorded, the fallback is vacant / property contract floor area (capped at 1); `vacant_area_ratio_basis` says which was used.
# MAGIC 2. **`large_vacant_space_flag`**: vacant area ≥ the regional 75th percentile of properties with vacant space (national if the region has fewer than 5), and ≥ 100 sqm.
# MAGIC 3. **`long_term_vacancy_flag`**: earliest available date ≥ 365 days ago.
# MAGIC 4. **`multiple_occupation_flag`**: ≥ 2 occupation records or ≥ 2 occupying organisations.
# MAGIC 5–7. **`surplus_flag`, `on_market_flag`, `under_offer_flag`** (under offer in either the surplus or vacant-space records).
# MAGIC 8. **`consolidation_opportunity_flag`**: significant occupation (≥ 100 sqm or ≥ 2 records) **and** significant vacancy (≥ 100 sqm or ratio ≥ 20%).
# MAGIC 9. **`reuse_opportunity_flag`**: available vacant space not on the market and not under offer for disposal.
# MAGIC 10. **`disposal_review_flag`**: surplus but neither on the market nor under offer.
# MAGIC 11. **`data_quality_risk_flag`**: any data-quality issue at property level (`dq_issue_list`).
# MAGIC 12. **Geospatial:** counts of other properties, occupied properties, vacant properties and vacant sqm within 1/5/10 km (Haversine with grid bucketing, UK coordinates only). Also `geo_cluster_id` (H3 resolution 7, Databricks-native) with cluster statistics, and `local_consolidation_cluster_flag`.
# MAGIC
# MAGIC Energy context (`worst_epc_band`, `worst_dec_band`, expired and implausible certificates) is aggregated from `building_clean`. It is used **only** in explanations, not in the score.

# COMMAND ----------

# DBTITLE 1,Aggregate linked records to property level
nd = ~F.col("is_duplicate_copy")
def cnt_if(cond):
    return F.sum(F.when(cond, 1).otherwise(0))
key = F.col("linked_property_reference").alias(REF)
lk = F.col("linked_property_reference").isNotNull()
EMPTY_ARR = F.expr("CAST(array() AS array<string>)")
ids = lambda: F.array_sort(F.collect_set("source_row_id"))

pc = spark.table(T("property_clean"))
prop_area = pc.groupBy(REF).agg(
    F.sum(F.when(nd, F.col("property_floor_area_sqm"))).alias("property_floor_area_sqm"),
    cnt_if((F.col("property_floor_area_unit_status") == "unit_unknown") & (F.col("floor_area") > 0)).alias("property_area_unit_unknown_count"),
    cnt_if(F.col("property_floor_area_value_status").isin("missing", "zero", "negative")).alias("property_missing_or_zero_area_count"),
    F.collect_set("property_floor_area_basis").alias("_prop_bases"),
    F.max("property_land_area_hectares").alias("property_land_area_hectares"))

occ = spark.table(T("occupation_clean")).filter(lk)
occ_agg = occ.groupBy(key).agg(
    F.count("*").alias("occupation_record_count"),
    F.countDistinct("organisation").alias("occupying_organisation_count"),
    jset("organisation").alias("occupying_organisations"),
    F.round(F.sum(F.when(nd, F.col("occupied_area_sqm"))), 2).alias("total_occupied_area_sqm"),
    cnt_if((F.col("occupied_area_unit_status") == "unit_unknown") & (F.col("floor_area") > 0)).alias("occupation_area_unit_unknown_count"),
    cnt_if(F.col("occupied_area_value_status").isin("missing", "zero", "negative")).alias("occupation_missing_or_zero_area_count"),
    cnt_if(F.col("reference_duplicate_flag") | F.col("exact_duplicate_row_flag")).alias("occupation_repeated_record_count"),
    F.collect_set("occupied_area_basis").alias("_occ_bases"), ids().alias("occupation_source_row_ids"))

vac = spark.table(T("vacant_space_clean")).filter(lk)
start = F.coalesce(F.col("vacant_available_start_date"), F.col("vacation_date_parsed"))
vac_agg = vac.groupBy(key).agg(
    F.count("*").alias("vacant_space_record_count"),
    F.round(F.sum(F.when(nd, F.col("vacant_area_sqm"))), 2).alias("total_vacant_area_sqm"),
    jset("vacant_space_status_clean").alias("vacant_space_status"),
    cnt_if(F.col("vacant_space_status_clean") == "Available").alias("vacant_available_count"),
    cnt_if(F.col("vacant_space_status_clean") == "Under Offer").alias("vacant_under_offer_count"),
    F.min(start).alias("earliest_vacant_available_date"),
    cnt_if((F.col("vacant_area_unit_status") == "unit_unknown") & (F.col("vacant_area") > 0)).alias("vacant_area_unit_unknown_count"),
    cnt_if(F.col("vacant_zero_area_flag")).alias("vacant_zero_area_count"),
    cnt_if(F.col("vacant_available_start_date_invalid_flag") | F.col("vacant_availability_end_date_invalid_flag") | F.col("vacation_date_parsed_invalid_flag")).alias("vacant_invalid_date_count"),
    cnt_if(F.col("vacant_missing_start_date_flag")).alias("vacant_missing_start_date_count"),
    cnt_if(F.col("reference_duplicate_flag") | F.col("exact_duplicate_row_flag")).alias("vacant_repeated_record_count"),
    F.collect_set("vacant_area_basis").alias("_vac_bases"), ids().alias("vacant_source_row_ids"))

sur = spark.table(T("surplus_clean")).filter(lk)
sur_agg = sur.groupBy(key).agg(
    F.count("*").alias("surplus_record_count"),
    F.max(F.col("surplus_status_clean").cast("int")).cast("boolean").alias("surplus_status"),
    jset("status_of_sale_clean").alias("status_of_sale"),
    cnt_if(F.col("status_of_sale_clean") == "On the Market").alias("surplus_on_market_count"),
    cnt_if(F.col("status_of_sale_clean") == "Under Offer").alias("surplus_under_offer_count"),
    cnt_if(F.col("status_of_sale_clean") == "Not on the Market").alias("surplus_not_on_market_count"),
    F.round(F.sum(F.when(nd, F.col("surplus_land_area_hectares"))), 4).alias("total_surplus_land_area_hectares"),
    F.round(F.sum(F.when(nd, F.col("surplus_floor_area_sqm"))), 2).alias("total_surplus_floor_area_sqm"),
    jset("land_usage").alias("surplus_land_usage"),
    cnt_if(F.col("reference_duplicate_flag") | F.col("exact_duplicate_row_flag")).alias("surplus_repeated_record_count"),
    ids().alias("surplus_source_row_ids"))

lnd = spark.table(T("land_clean"))
land_agg = lnd.filter(lk).groupBy(key).agg(
    F.count("*").alias("linked_land_record_count"),
    F.round(F.sum(F.when(nd, F.col("land_area_hectares"))), 4).alias("linked_land_area_hectares"),
    jset("usage").alias("linked_land_usage"), ids().alias("land_source_row_ids"))
land_low = (lnd.filter("property_match_confidence = 'Low'").groupBy(F.col("candidate_property_reference").alias(REF))
               .agg(F.count("*").alias("land_low_confidence_candidate_count")))

bld = spark.table(T("building_clean")).filter(lk)
bld_agg = bld.groupBy(key).agg(
    F.count("*").alias("building_record_count"),
    F.round(F.sum(F.when(nd, F.col("building_floor_area_sqm"))), 2).alias("total_building_floor_area_sqm"),
    cnt_if(F.col("epc_band").isNotNull()).alias("epc_rated_building_count"),
    F.max("epc_band_rank").alias("_epc_worst_rank"), F.min("epc_band_rank").alias("_epc_best_rank"),
    cnt_if(F.col("epc_band").isNotNull() & F.col("epc_expired_flag")).alias("epc_expired_count"),
    cnt_if(F.col("epc_implausible_date_flag")).alias("epc_implausible_date_count"),
    cnt_if(F.col("dec_band").isNotNull()).alias("dec_rated_building_count"),
    F.max("dec_band_rank").alias("_dec_worst_rank"),
    cnt_if(F.col("dec_band").isNotNull() & F.col("dec_expired_flag")).alias("dec_expired_count"),
    cnt_if(F.col("dec_implausible_date_flag")).alias("dec_implausible_date_count"),
    jset("breeam_rating_type").alias("breeam_ratings"),
    F.max(F.col("listed_building_flag").cast("int")).cast("boolean").alias("listed_building_flag"),
    jset("listed_building").alias("listed_building_grades"),
    F.min(F.when(~F.col("construction_date_implausible_flag"), F.col("construction_date"))).alias("earliest_construction_date"),
    ids().alias("building_source_row_ids"))

PB = spark.table(T("stg_property_base"))
pm = PB
for agg_df in [prop_area, occ_agg, vac_agg, sur_agg, land_agg, land_low, bld_agg]:
    pm = pm.join(agg_df, REF, "left")
COUNT_COLS = ["property_area_unit_unknown_count", "property_missing_or_zero_area_count", "occupation_record_count",
              "occupying_organisation_count", "occupation_area_unit_unknown_count", "occupation_missing_or_zero_area_count",
              "occupation_repeated_record_count", "vacant_space_record_count", "vacant_available_count", "vacant_under_offer_count",
              "vacant_area_unit_unknown_count", "vacant_zero_area_count", "vacant_invalid_date_count", "vacant_missing_start_date_count",
              "vacant_repeated_record_count", "surplus_record_count", "surplus_on_market_count", "surplus_under_offer_count",
              "surplus_not_on_market_count", "surplus_repeated_record_count", "linked_land_record_count",
              "land_low_confidence_candidate_count", "building_record_count", "epc_rated_building_count", "epc_expired_count",
              "epc_implausible_date_count", "dec_rated_building_count", "dec_expired_count", "dec_implausible_date_count"]
pm = pm.fillna(0, subset=COUNT_COLS)
for c in ["occupation_source_row_ids", "vacant_source_row_ids", "surplus_source_row_ids", "land_source_row_ids", "building_source_row_ids"]:
    pm = pm.withColumn(c, F.coalesce(F.col(c), EMPTY_ARR))

band_arr = F.array(*[F.lit(b) for b in EPC_BANDS])
occ_a, vac_a, pfa = F.col("total_occupied_area_sqm"), F.col("total_vacant_area_sqm"), F.col("property_floor_area_sqm")
pm = (pm
    .withColumn("area_bases", F.array_sort(F.array_distinct(F.concat(F.coalesce("_prop_bases", EMPTY_ARR), F.coalesce("_occ_bases", EMPTY_ARR), F.coalesce("_vac_bases", EMPTY_ARR)))))
    .withColumn("inconsistent_area_basis_flag", F.size("area_bases") > 1)
    .withColumn("worst_epc_band", F.when(F.col("_epc_worst_rank").isNotNull(), F.element_at(band_arr, F.col("_epc_worst_rank").cast("int") + 1)))
    .withColumn("best_epc_band", F.when(F.col("_epc_best_rank").isNotNull(), F.element_at(band_arr, F.col("_epc_best_rank").cast("int") + 1)))
    .withColumn("worst_dec_band", F.when(F.col("_dec_worst_rank").isNotNull(), F.element_at(band_arr, F.col("_dec_worst_rank").cast("int") + 1)))
    .withColumn("has_energy_data", (F.col("epc_rated_building_count") + F.col("dec_rated_building_count")) > 0)
    .withColumn("surplus_status", F.coalesce("surplus_status", F.lit(False)))
    .withColumn("listed_building_flag", F.coalesce("listed_building_flag", F.lit(False)))
    .withColumn("property_scale_sqm", F.greatest(F.coalesce(pfa, F.lit(0.0)), F.coalesce(F.col("total_building_floor_area_sqm"), F.lit(0.0)),
                                                 F.coalesce(occ_a, F.lit(0.0)) + F.coalesce(vac_a, F.lit(0.0))))
    # 1. vacant_area_ratio
    .withColumn("vacant_area_ratio_basis", F.when(vac_a.isNull(), F.lit(None)).when(F.coalesce(occ_a, F.lit(0.0)) > 0, "occupied_plus_vacant")
                .when(pfa > 0, "property_floor_area_fallback"))
    .withColumn("vacant_area_ratio", F.round(F.when(F.col("vacant_area_ratio_basis") == "occupied_plus_vacant", vac_a / (occ_a + vac_a))
                .when(F.col("vacant_area_ratio_basis") == "property_floor_area_fallback", F.least(F.lit(1.0), vac_a / pfa)), 4)))

# 2. large_vacant_space_flag (relative to comparable properties in the same region)
vp = pm.filter(vac_a > 0)
reg = vp.groupBy("region_clean").agg(F.percentile_approx("total_vacant_area_sqm", LARGE_VACANT_PERCENTILE).alias("_reg_p"), F.count("*").alias("_reg_n"))
nat_p = vp.agg(F.percentile_approx("total_vacant_area_sqm", LARGE_VACANT_PERCENTILE)).first()[0]
pm = (pm.join(reg, "region_clean", "left")
    .withColumn("comparable_group", F.when(F.col("_reg_n") >= MIN_COMPARABLE_GROUP, F.concat(F.lit("region: "), F.col("region_clean"))).otherwise(F.lit("national")))
    .withColumn("comparable_vacant_threshold_sqm", F.greatest(F.when(F.col("_reg_n") >= MIN_COMPARABLE_GROUP, F.col("_reg_p")).otherwise(F.lit(nat_p)), F.lit(float(LARGE_VACANT_MIN_SQM))))
    .withColumn("large_vacant_space_flag", safe_bool(vac_a >= F.col("comparable_vacant_threshold_sqm")))
    # 3. long-term vacancy
    .withColumn("max_vacancy_days", F.when(F.col("earliest_vacant_available_date").isNotNull(), F.greatest(F.lit(0), F.datediff(F.current_date(), F.col("earliest_vacant_available_date")))))
    .withColumn("long_term_vacancy_flag", safe_bool(F.col("max_vacancy_days") >= LONG_TERM_VACANCY_DAYS))
    # 4-10
    .withColumn("multiple_occupation_flag", (F.col("occupation_record_count") >= MULTIPLE_OCCUPATION_MIN) | (F.col("occupying_organisation_count") >= MULTIPLE_OCCUPATION_MIN))
    .withColumn("surplus_flag", F.col("surplus_status") & (F.col("surplus_record_count") > 0))
    .withColumn("on_market_flag", F.col("surplus_on_market_count") > 0)
    .withColumn("surplus_under_offer_flag", F.col("surplus_under_offer_count") > 0)
    .withColumn("under_offer_flag", F.col("surplus_under_offer_flag") | (F.col("vacant_under_offer_count") > 0))
    .withColumn("consolidation_opportunity_flag",
                safe_bool((occ_a >= SIGNIFICANT_OCCUPIED_SQM) | (F.col("occupation_record_count") >= MULTIPLE_OCCUPATION_MIN))
                & safe_bool((vac_a >= SIGNIFICANT_VACANT_SQM) | (F.col("vacant_area_ratio") >= SIGNIFICANT_VACANT_RATIO)))
    .withColumn("reuse_opportunity_flag", (F.col("vacant_available_count") > 0) & ~F.col("on_market_flag") & ~F.col("surplus_under_offer_flag"))
    .withColumn("disposal_review_flag", F.col("surplus_flag") & ~F.col("on_market_flag") & ~F.col("surplus_under_offer_flag"))
    .drop("_reg_p", "_reg_n"))

PF = save_delta(pm, "stg_property_features")
display(PF.agg(*[F.sum(F.col(c).cast("int")).alias(c) for c in [
    "large_vacant_space_flag", "long_term_vacancy_flag", "multiple_occupation_flag", "surplus_flag", "on_market_flag",
    "under_offer_flag", "consolidation_opportunity_flag", "reuse_opportunity_flag", "disposal_review_flag",
    "inconsistent_area_basis_flag", "has_energy_data"]]))

# COMMAND ----------

# DBTITLE 1,Geospatial indicators, data-quality flags, write property_master
PF = spark.table(T("stg_property_features"))
R_MAX = max(NEARBY_RADII_KM)
# Grid cells of 0.1 deg lat (~11.1 km) x 0.2 deg lon (>= 10.8 km up to 60.9N) guarantee all pairs within 10 km are compared.
assert R_MAX <= 10, "Increase grid cell sizes if radii above 10 km are needed"
pts = PF.filter("coordinate_status = 'valid_uk'").select(
    REF, "latitude_clean", "longitude_clean",
    (F.col("occupation_record_count") > 0).cast("int").alias("is_occ"),
    (F.col("vacant_space_record_count") > 0).cast("int").alias("is_vac"),
    F.coalesce("total_vacant_area_sqm", F.lit(0.0)).alias("vac_sqm"))
grp = pts.groupBy("latitude_clean", "longitude_clean").agg(F.count("*").alias("g_n"), F.sum("is_occ").alias("g_occ"),
                                                           F.sum("is_vac").alias("g_vac"), F.sum("vac_sqm").alias("g_vs"))
L = grp.select(F.col("latitude_clean").alias("l_lat"), F.col("longitude_clean").alias("l_lon"))
Rr = grp.select(F.col("latitude_clean").alias("r_lat"), F.col("longitude_clean").alias("r_lon"), "g_n", "g_occ", "g_vac", "g_vs")
pairs = (grid_neighbour_join(L, Rr, "l_lat", "l_lon", "r_lat", "r_lon", 0.1, 0.2)
         .withColumn("d_km", haversine_m(F.col("l_lat"), F.col("l_lon"), F.col("r_lat"), F.col("r_lon")) / 1000)
         .filter(F.col("d_km") <= R_MAX))
aggs = []
for r in NEARBY_RADII_KM:
    within = F.col("d_km") <= r
    aggs += [F.sum(F.when(within, F.col("g_n")).otherwise(0)).alias(f"_n{r}"), F.sum(F.when(within, F.col("g_occ")).otherwise(0)).alias(f"_o{r}"),
             F.sum(F.when(within, F.col("g_vac")).otherwise(0)).alias(f"_v{r}"), F.sum(F.when(within, F.col("g_vs")).otherwise(0.0)).alias(f"_s{r}")]
nb = pairs.groupBy("l_lat", "l_lon").agg(*aggs)
geo = pts.join(nb, (F.col("latitude_clean") == F.col("l_lat")) & (F.col("longitude_clean") == F.col("l_lon")), "left")
geo_cols = []
for r in NEARBY_RADII_KM:   # subtract the property itself
    geo_cols += [(F.col(f"_n{r}") - 1).alias(f"nearby_properties_{r}km"),
                 (F.col(f"_o{r}") - F.col("is_occ")).alias(f"nearby_occupied_properties_{r}km"),
                 (F.col(f"_v{r}") - F.col("is_vac")).alias(f"nearby_vacant_properties_{r}km"),
                 F.round(F.col(f"_s{r}") - F.col("vac_sqm"), 2).alias(f"nearby_vacant_area_sqm_{r}km")]
geo = save_delta(geo.select(REF, *geo_cols), "stg_property_geo")

r0 = LOCAL_CLUSTER_RADIUS_KM
pm = (PF.join(geo, REF, "left")
    .withColumn("geospatial_analysis_status", F.when(F.col("coordinate_status") == "valid_uk", "analysed")
                .otherwise(F.concat(F.lit("not analysed ("), F.col("coordinate_status"), F.lit(")"))))
    .withColumn("geo_cluster_id", F.when(F.col("coordinate_status") == "valid_uk",
                F.expr(f"h3_longlatash3string(longitude_clean, latitude_clean, {H3_CLUSTER_RESOLUTION})"))))
cl = pm.filter("geo_cluster_id IS NOT NULL").groupBy("geo_cluster_id").agg(
    F.count("*").alias("geo_cluster_property_count"),
    F.sum((F.col("occupation_record_count") > 0).cast("int")).alias("geo_cluster_occupied_count"),
    F.sum((F.col("vacant_space_record_count") > 0).cast("int")).alias("geo_cluster_vacant_count"),
    F.round(F.sum(F.coalesce("total_vacant_area_sqm", F.lit(0.0))), 2).alias("geo_cluster_vacant_area_sqm"))
pm = (pm.join(cl, "geo_cluster_id", "left")
    .withColumn("geo_cluster_consolidation_flag", safe_bool((F.col("geo_cluster_vacant_count") >= 1)
                & ((F.col("geo_cluster_occupied_count") + F.col("geo_cluster_vacant_count")) >= 2)))
    .withColumn("local_consolidation_cluster_flag", safe_bool(
        ((F.col("occupation_record_count") > 0) & (F.col(f"nearby_vacant_properties_{r0}km") >= 1))
        | ((F.col("vacant_space_record_count") > 0) & (F.col(f"nearby_occupied_properties_{r0}km") >= 1)))))

# 11. Data-quality uncertainty at property level (uncertainty, NOT evidence of poor performance)
DQ_RULES = [
    (~F.col("has_valid_coordinates"), "no valid coordinates"),
    (F.col("postcode_clean").isNull(), "missing postcode"),
    (F.col("postcode_clean").isNotNull() & ~F.col("postcode_valid_format_flag"), "postcode format invalid"),
    (F.col("property_name_clean").isNull(), "missing property name"),
    (F.col("invalid_easting_northing_flag"), "Easting/Northing out of range"),
    (F.col("location_conflict_flag"), "location attributes conflict with other records"),
    (F.col("property_conflicting_coordinates_flag"), "register rows give conflicting coordinates"),
    (~F.col("property_in_register"), "not found in property register"),
    ((F.col("occupation_area_unit_unknown_count") + F.col("vacant_area_unit_unknown_count") + F.col("property_area_unit_unknown_count")) > 0, "area recorded without a unit/basis"),
    (F.col("inconsistent_area_basis_flag"), "mixed IPMS measurement bases"),
    (F.col("vacant_zero_area_count") > 0, "vacant space recorded with zero area"),
    (F.col("vacant_invalid_date_count") > 0, "invalid vacancy dates"),
    (F.col("vacant_missing_start_date_count") > 0, "missing vacancy start date"),
    (F.col("occupation_missing_or_zero_area_count") > 0, "occupation with missing or zero floor area"),
    ((F.col("epc_implausible_date_count") + F.col("dec_implausible_date_count")) > 0, "implausible energy-certificate dates"),
    ((F.col("occupation_repeated_record_count") + F.col("vacant_repeated_record_count") + F.col("surplus_repeated_record_count")) > 0, "repeated occupation, vacant-space or surplus records"),
    (F.col("land_low_confidence_candidate_count") > 0, "low-confidence land matches awaiting review")]
pm = (pm.withColumn("dq_issue_count", reduce(lambda a, b: a + b, [F.when(safe_bool(c), 1).otherwise(0) for c, _ in DQ_RULES]))
        .withColumn("dq_issue_list", F.concat_ws("; ", *[F.when(safe_bool(c), F.lit(l)) for c, l in DQ_RULES]))
        .withColumn("dq_issue_list", F.when(F.col("dq_issue_list") != "", F.col("dq_issue_list")))
        .withColumn("data_quality_risk_flag", F.col("dq_issue_count") > 0))
pm = pm.drop(*[c for c in pm.columns if c.startswith("_")])
PM = save_delta(pm, "property_master", "One row per property with location, aggregated occupation/vacancy/surplus/land/building measures, indicators, geospatial and data-quality flags")
display(PM.agg(F.count("*").alias("properties"), F.sum(F.col("local_consolidation_cluster_flag").cast("int")).alias("local_consolidation_cluster"),
               F.sum(F.col("geo_cluster_consolidation_flag").cast("int")).alias("geo_cluster_consolidation"),
               F.sum(F.col("data_quality_risk_flag").cast("int")).alias("data_quality_risk"),
               F.round(F.avg("nearby_properties_1km"), 1).alias("avg_nearby_1km"), F.max("nearby_properties_10km").alias("max_nearby_10km")))

# COMMAND ----------

# DBTITLE 1,Record-level data-quality issue table
COMMON_RULES = [
    ("missing_property_name", "property_name_clean IS NULL", "Medium", "property_name", "property_name"),
    ("missing_postcode", "postcode_clean IS NULL", "Medium", "postcode", "postcode"),
    ("invalid_postcode_format", "postcode_clean IS NOT NULL AND NOT postcode_valid_format_flag", "Low", "postcode", "postcode"),
    ("missing_coordinates", "coordinate_status = 'missing'", "High", "latitude/longitude", "concat_ws(', ', latitude, longitude)"),
    ("zero_coordinates", "coordinate_status = 'zero'", "High", "latitude/longitude", "concat_ws(', ', latitude, longitude)"),
    ("coordinates_out_of_range", "coordinate_status IN ('out_of_range', 'possibly_swapped')", "High", "latitude/longitude", "concat(coordinate_status, ': ', concat_ws(', ', latitude, longitude))"),
    ("invalid_easting_northing", "invalid_easting_northing_flag", "Low", "easting/northing", "concat_ws(' | ', concat_ws(', ', easting, northing), concat_ws(', ', ni_easting, ni_northing))"),
    ("location_conflict", "location_conflict_flag", "Low", "postcode/town/region/local_authority", "location_conflict_notes"),
    ("exact_duplicate_record", "is_duplicate_copy", "Medium", "(all columns)", "source_row_hash"),
    ("repeated_record_reference", "reference_duplicate_flag", "Low", "record_reference", "concat(record_reference, ' x', reference_duplicate_count)")]
MISSING_REF_RULE = ("missing_property_reference", "insite_property_reference IS NULL", "Medium", "insite_property_reference", None)
MATCH_RULES = [("property_match_unmatched", "property_match_confidence = 'Unmatched'", "Medium", "property_match_confidence", "property_match_notes"),
               ("property_match_low_confidence", "property_match_confidence = 'Low'", "Medium", "candidate_property_reference", "concat(candidate_property_reference, ' - ', property_match_notes)")]
def area_rules(prefix, v, unit_field):
    r = [("missing_or_zero_area", f"{prefix}_value_status IN ('missing', 'zero', 'negative')", "Low", v, v)]
    if unit_field:
        r.append(("area_unit_unknown", f"{prefix}_unit_status = 'unit_unknown' AND {v} IS NOT NULL", "Low", unit_field, f"concat_ws(' ', {v}, {unit_field})"))
    return r
DATASET_RULES = {
    "property": area_rules("property_floor_area", "floor_area", "floor_area_measurement") + [
        ("country_region_conflict", "country_region_conflict_flag", "Low", "country", "country"),
        ("implausible_acquisition_date", "acquisition_date_implausible_flag", "Low", "acquisition_date", "acquisition_date")],
    "building": area_rules("building_floor_area", "floor_area", "floor_area_measurement") + [
        ("implausible_epc_dates", "epc_implausible_date_flag", "Low", "epc_certificate_issue_date/epc_expiry_date", "concat_ws(' / ', epc_certificate_issue_date, epc_expiry_date)"),
        ("implausible_dec_dates", "dec_implausible_date_flag", "Low", "dec_certificate_issue_date/dec_expiry_date", "concat_ws(' / ', dec_certificate_issue_date, dec_expiry_date)"),
        ("implausible_construction_date", "construction_date_implausible_flag", "Low", "construction_date", "construction_date")],
    "occupation": area_rules("occupied_area", "floor_area", "floor_area_measurement"),
    "vacant_space": area_rules("vacant_area", "vacant_area", "vacant_area_units") + [
        ("vacant_space_zero_area", "vacant_zero_area_flag", "Medium", "vacant_area", "vacant_area"),
        ("invalid_vacant_start_date", "vacant_available_start_date_invalid_flag", "Medium", "vacant_space_available_start_date", "vacant_space_available_start_date"),
        ("invalid_vacant_end_date", "vacant_availability_end_date_invalid_flag", "Low", "vacant_space_availability_end_date", "vacant_space_availability_end_date"),
        ("invalid_vacation_date", "vacation_date_parsed_invalid_flag", "Low", "vacation_date", "vacation_date"),
        ("missing_vacant_start_date", "vacant_missing_start_date_flag", "Medium", "vacant_space_available_start_date", None)],
    "surplus": [("missing_or_zero_surplus_area", "coalesce(total_surplus_land_area, 0) <= 0 AND coalesce(total_surplus_floor_area, 0) <= 0", "Low",
                 "total_surplus_land_area/total_surplus_floor_area", "concat_ws(' / ', total_surplus_land_area, total_surplus_floor_area)")],
    "land": area_rules("land_area", "total_land_area", None) + [
        ("property_reference_not_derivable", "insite_property_reference IS NULL", "Medium", "insite_land_reference", "insite_land_reference")],
}
DQ_OUT_COLS = ["source_dataset", "source_row_id", "record_reference", "property_reference", "issue_type", "severity",
               "field_name", "original_value", "property_name", "postcode", "latitude", "longitude"]

def dq_records(ds, df):
    rules = list(COMMON_RULES) + DATASET_RULES.get(ds, [])
    if ds != "land":
        rules.append(MISSING_REF_RULE)
    if ds != "property":
        rules += MATCH_RULES
    prop_ref = F.col(REF) if ds == "property" else F.coalesce(F.col("linked_property_reference"), F.col("candidate_property_reference"), F.col(REF))
    return reduce(lambda a, b: a.unionByName(b), [
        df.filter(F.expr(cond)).select(
            F.lit(ds).alias("source_dataset"), "source_row_id", "record_reference", prop_ref.cast("string").alias("property_reference"),
            F.lit(issue).alias("issue_type"), F.lit(sev).alias("severity"), F.lit(field).alias("field_name"),
            (F.expr(val).cast("string") if val else F.lit(None).cast("string")).alias("original_value"),
            "property_name", "postcode", F.col("latitude").cast("double").alias("latitude"), F.col("longitude").cast("double").alias("longitude"))
        for issue, cond, sev, field, val in rules])

record_issues = reduce(lambda a, b: a.unionByName(b), [dq_records(ds, spark.table(T(f"{ds}_clean"))) for ds in SOURCE_TABLES_FOUND])

PL_RULES = [("inconsistent_area_basis", "inconsistent_area_basis_flag", "Low", "area_bases", "array_join(area_bases, '; ')"),
            ("conflicting_coordinates_across_register_rows", "property_conflicting_coordinates_flag", "Medium", "latitude/longitude", None),
            ("not_in_property_register", "NOT property_in_register", "Medium", "insite_property_reference", "non_register_source_datasets"),
            ("low_confidence_land_matches_pending_review", "land_low_confidence_candidate_count > 0", "Low", "land", "CAST(land_low_confidence_candidate_count AS STRING)")]
PM = spark.table(T("property_master"))
prop_issues = reduce(lambda a, b: a.unionByName(b), [
    PM.filter(F.expr(cond)).select(F.lit("property_master").alias("source_dataset"), F.lit(None).cast("string").alias("source_row_id"),
        F.col(REF).alias("record_reference"), F.col(REF).alias("property_reference"), F.lit(issue).alias("issue_type"), F.lit(sev).alias("severity"),
        F.lit(field).alias("field_name"), (F.expr(val).cast("string") if val else F.lit(None).cast("string")).alias("original_value"),
        "property_name", "postcode", F.col("latitude").cast("double").alias("latitude"), F.col("longitude").cast("double").alias("longitude"))
    for issue, cond, sev, field, val in PL_RULES])

# Dataset-level notes: column-mapping gaps and documented unit assumptions
note_rows = [(r.dataset, "column_missing_created_null" if r.mapping_method.startswith("missing") else f"column_{r.mapping_method.replace(' ', '_').replace('/', '')}",
              "Info", r.canonical_column, r.note) for r in spark.table(T("column_mapping")).filter("mapping_method != 'exact'").collect()]
assumptions = [("property/building/occupation/vacant_space", "IPMS measurement basis treated as square metres" if ASSUME_IPMS_IS_SQM else "IPMS basis not converted"),
               ("land/property", f"Total Land Area treated as hectares (no unit field): {ASSUME_LAND_AREA_IS_HECTARES}"),
               ("surplus", f"Total Surplus Land Area treated as hectares (no unit field): {ASSUME_SURPLUS_LAND_AREA_IS_HECTARES}"),
               ("surplus", f"Total Surplus Floor Area treated as square metres (no unit field): {ASSUME_SURPLUS_FLOOR_AREA_IS_SQM}")]
note_rows += [(ds, "area_unit_assumption", "Info", "area", txt) for ds, txt in assumptions]
notes_df = (spark.createDataFrame(note_rows, "source_dataset string, issue_type string, severity string, field_name string, original_value string")
            .select("source_dataset", F.lit(None).cast("string").alias("source_row_id"), F.lit(None).cast("string").alias("record_reference"),
                    F.lit(None).cast("string").alias("property_reference"), "issue_type", "severity", "field_name", "original_value",
                    F.lit(None).cast("string").alias("property_name"), F.lit(None).cast("string").alias("postcode"),
                    F.lit(None).cast("double").alias("latitude"), F.lit(None).cast("double").alias("longitude")))

dq_all = record_issues.select(*DQ_OUT_COLS).unionByName(prop_issues.select(*DQ_OUT_COLS)).unionByName(notes_df.select(*DQ_OUT_COLS))
dq_all = dq_all.withColumn("dq_issue_id", F.xxhash64(*[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in ["source_dataset", "source_row_id", "record_reference", "issue_type", "field_name"]]))
DQ = save_delta(dq_all, "property_data_quality_issues", "One row per data-quality issue; original values retained; records are flagged, never deleted")
display(DQ.groupBy("source_dataset", "severity").agg(F.count("*").alias("issues"), F.countDistinct("issue_type").alias("issue_types")).orderBy("source_dataset", "severity"))

# COMMAND ----------

# DBTITLE 1,10. Priority scoring
# MAGIC %md
# MAGIC ## 10. Priority scoring
# MAGIC A transparent 0–100 score; each component is 0–100 and is stored separately. No AI or ML is used.
# MAGIC
# MAGIC | Component | Weight | Rule |
# MAGIC | --- | --- | --- |
# MAGIC | `vacancy_score` | 30% | If vacant area > 0: 50% × vacant ratio (as %) + 50% × vacant-area scale, where scale = 20 + 80 × percentile of log(vacant sqm) among vacant properties; without a ratio, scale only. Vacant records with no usable area = 10. |
# MAGIC | `vacancy_duration_score` | 15% | days available / 1,095, capped at 100 |
# MAGIC | `surplus_score` | 20% | surplus not on market (disposal review) 100; surplus other 80; on the market 60; under offer 30 |
# MAGIC | `consolidation_reuse_score` | 20% | consolidation 40 + reuse 30 + local cluster 20 + multiple occupation 10 (max 100) |
# MAGIC | `scale_score` | 10% | percentile of log(property scale sqm) × 100 |
# MAGIC | `data_uncertainty_score` | 5% | 20 per property-level data-quality issue (max 100). This is **uncertainty that raises the need to investigate, not evidence of poor property performance**. |
# MAGIC
# MAGIC Bands: 80–100 Very High · 60–79 High · 40–59 Medium · 20–39 Low · 0–19 Very Low.

# COMMAND ----------

# DBTITLE 1,Calculate component and priority scores
PM = spark.table(T("property_master"))
vac_pct = PM.filter(F.col("total_vacant_area_sqm") > 0).select(
    REF, F.percent_rank().over(W.orderBy(F.log1p("total_vacant_area_sqm"))).alias("vacant_scale_percentile"))
scale_pct = PM.filter(F.col("property_scale_sqm") > 0).select(
    REF, F.percent_rank().over(W.orderBy(F.log1p("property_scale_sqm"))).alias("scale_percentile"))
sc = PM.join(vac_pct, REF, "left").join(scale_pct, REF, "left")

ratio_pct = F.least(F.col("vacant_area_ratio"), F.lit(1.0)) * 100
vac_scale = 100 * (0.2 + 0.8 * F.col("vacant_scale_percentile"))
b = lambda c: F.col(c).cast("int")
sc = (sc
    .withColumn("vacancy_score", F.round(
        F.when(F.col("total_vacant_area_sqm") > 0,
               F.when(F.col("vacant_area_ratio").isNotNull(), 0.5 * ratio_pct + 0.5 * vac_scale).otherwise(vac_scale))
         .when(F.col("vacant_space_record_count") > 0, F.lit(10.0)).otherwise(F.lit(0.0)), 1))
    .withColumn("vacancy_duration_score", F.round(
        F.when(F.col("max_vacancy_days") > 0, F.least(F.lit(1.0), F.col("max_vacancy_days") / VACANCY_DURATION_FULL_SCORE_DAYS) * 100)
         .otherwise(F.lit(0.0)), 1))
    .withColumn("surplus_score", F.when(F.col("disposal_review_flag"), F.lit(100.0))
                .when(F.col("surplus_flag") & F.col("on_market_flag"), F.lit(60.0))
                .when(F.col("surplus_flag") & F.col("surplus_under_offer_flag"), F.lit(30.0))
                .when(F.col("surplus_flag"), F.lit(80.0)).otherwise(F.lit(0.0)))
    .withColumn("consolidation_reuse_score", F.least(F.lit(100.0), (40 * b("consolidation_opportunity_flag") + 30 * b("reuse_opportunity_flag")
                + 20 * b("local_consolidation_cluster_flag") + 10 * b("multiple_occupation_flag")).cast("double")))
    .withColumn("scale_score", F.round(F.coalesce(F.col("scale_percentile") * 100, F.lit(0.0)), 1))
    .withColumn("data_uncertainty_score", F.least(F.lit(100.0), (F.col("dq_issue_count") * 20).cast("double"))))
for c, w in WEIGHTS.items():
    sc = sc.withColumn(f"{c}_weighted", F.round(F.col(c) * w, 2))
total = reduce(lambda a, b2: a + b2, [F.col(c) * w for c, w in WEIGHTS.items()])
sc = sc.withColumn("investigation_priority_score", F.round(F.greatest(F.lit(0.0), F.least(F.lit(100.0), total)), 1))
band = None
for lo, name in PRIORITY_BANDS:
    cond = F.col("investigation_priority_score") >= lo
    band = F.when(cond, name) if band is None else band.when(cond, name)
sc = sc.withColumn("priority_band", band)
SCORED = save_delta(sc, "stg_property_scored")

COMPONENTS = list(WEIGHTS.keys())
display(SCORED.groupBy("priority_band").agg(F.count("*").alias("properties"), F.round(F.min("investigation_priority_score"), 1).alias("min_score"),
                                            F.round(F.max("investigation_priority_score"), 1).alias("max_score"))
              .orderBy(F.desc("min_score")))
display(SCORED.agg(*[F.round(F.avg(c), 1).alias(f"avg_{c}") for c in COMPONENTS + ["investigation_priority_score"]],
                   *[F.sum((F.col(c) > 0).cast("int")).alias(f"n_{c}_gt0") for c in COMPONENTS]))

# COMMAND ----------

# DBTITLE 1,11. Explanations
# MAGIC %md
# MAGIC ## 11. Explanations
# MAGIC `priority_explanation` is assembled from deterministic rules that refer only to evidence in the datasets: vacancy, duration, surplus status, consolidation, reuse, multiple occupation, nearby properties and scale.
# MAGIC * **Energy:** where EPC/DEC ratings exist for the property's buildings, the worst band and the counts of rated, expired and implausibly dated certificates are reported, for context only. Inefficiency is never claimed. Where no ratings exist, the exact sentence required by the brief is used.
# MAGIC * **Data quality:** issues are listed separately and labelled as uncertainty, not evidence of poor performance.
# MAGIC
# MAGIC The result is written to `property_priority_results`, which keeps every location field (original and cleaned) and separate numeric latitude/longitude, ready for mapping.

# COMMAND ----------

# DBTITLE 1,Build explanations and write property_priority_results
SCORED = spark.table(T("stg_property_scored"))
r0 = LOCAL_CLUSTER_RADIUS_KM
num = lambda c, d=0: F.format_number(F.col(c), d)
reasons = F.array(
    F.when(F.col("vacant_area_ratio") >= 0.5, F.concat(F.lit("has a high proportion of vacant floor space ("), F.format_number(F.col("vacant_area_ratio") * 100, 0),
                                                      F.lit("% of recorded area, "), num("total_vacant_area_sqm"), F.lit(" sqm vacant)")))
     .when(F.col("total_vacant_area_sqm") > 0, F.concat(F.lit("has "), num("total_vacant_area_sqm"), F.lit(" sqm of recorded vacant space"),
                                                       F.when(F.col("vacant_area_ratio").isNotNull(), F.concat(F.lit(" ("), F.format_number(F.col("vacant_area_ratio") * 100, 0), F.lit("% of recorded area)"))).otherwise(F.lit("")),
                                                       F.when(F.col("large_vacant_space_flag"), F.lit(", which is large relative to comparable properties")).otherwise(F.lit(""))))
     .when(F.col("vacant_space_record_count") > 0, F.lit("has vacant-space records without a usable area")),
    F.when(F.col("long_term_vacancy_flag"), F.concat(F.lit("has space that has been available for an extended period (since "),
                                                       F.date_format("earliest_vacant_available_date", "d MMM yyyy"), F.lit(", "), num("max_vacancy_days"), F.lit(" days)"))),
    F.when(F.col("disposal_review_flag"), F.lit("has been declared surplus but is not on the market"))
     .when(F.col("surplus_flag") & F.col("on_market_flag"), F.lit("has been declared surplus and is on the market"))
     .when(F.col("surplus_flag") & F.col("surplus_under_offer_flag"), F.lit("has been declared surplus with a sale under offer"))
     .when(F.col("surplus_flag"), F.lit("has been declared surplus")),
    F.when(F.col("consolidation_opportunity_flag"), F.lit("has both significant occupation and significant vacant space, suggesting a possible consolidation opportunity")),
    F.when(F.col("reuse_opportunity_flag"), F.lit("has available vacant space that is not being marketed for disposal and may offer reuse potential")),
    F.when(F.col("multiple_occupation_flag"), F.concat(F.lit("has "), F.col("occupation_record_count").cast("string"), F.lit(" occupation records across "),
                                                        F.col("occupying_organisation_count").cast("string"), F.lit(" organisation(s)"))),
    F.when(F.col("local_consolidation_cluster_flag"), F.concat(F.lit(f"is located within {r0} km of "), F.col(f"nearby_occupied_properties_{r0}km").cast("string"),
        F.lit(" other government propert(ies) with occupation records and "), F.col(f"nearby_vacant_properties_{r0}km").cast("string"),
        F.lit(" with vacant space, which may offer consolidation opportunities"))),
    F.when(F.col("scale_score") >= 90, F.concat(F.lit("is among the largest 10% of properties by recorded floor area ("), num("property_scale_sqm"), F.lit(" sqm)"))))
SC2 = SCORED.withColumn("_r", F.filter(reasons, lambda x: x.isNotNull()))
joined = (F.when(F.size("_r") == 0, F.lit(None)).when(F.size("_r") == 1, F.element_at("_r", 1))
           .otherwise(F.concat(F.array_join(F.expr("slice(_r, 1, size(_r) - 1)"), ", "), F.lit(" and "), F.element_at("_r", -1))))
lead = (F.when((F.col("investigation_priority_score") >= 20) & joined.isNotNull(),
               F.concat(F.lit("This property is prioritised for investigation ("), F.col("priority_band"), F.lit(", score "),
                        F.col("investigation_priority_score").cast("string"), F.lit(") because it "), joined, F.lit(".")))
          .when(joined.isNotNull(), F.concat(F.lit("This property has a low investigation priority (score "), F.col("investigation_priority_score").cast("string"),
                                             F.lit("); the evidence found is that it "), joined, F.lit(".")))
          .otherwise(F.concat(F.lit("This property has a low investigation priority (score "), F.col("investigation_priority_score").cast("string"),
                              F.lit(") because no vacancy, surplus or consolidation indicators were found in the supplied datasets."))))
energy_parts = F.concat_ws("; ",
    F.when(F.col("epc_rated_building_count") > 0, F.concat(F.lit("worst EPC band "), F.col("worst_epc_band"), F.lit(" across "), F.col("epc_rated_building_count").cast("string"), F.lit(" EPC-rated building(s)"))),
    F.when(F.col("dec_rated_building_count") > 0, F.concat(F.lit("worst DEC operational band "), F.col("worst_dec_band"))),
    F.when((F.col("epc_expired_count") + F.col("dec_expired_count")) > 0, F.concat((F.col("epc_expired_count") + F.col("dec_expired_count")).cast("string"), F.lit(" certificate(s) past expiry"))),
    F.when((F.col("epc_implausible_date_count") + F.col("dec_implausible_date_count")) > 0, F.lit("some certificate dates look implausible")))
energy = F.when(F.col("has_energy_data"), F.concat(F.lit("Energy certificates on record (context only, not scored): "), energy_parts,
                F.lit(". The score identifies property-utilisation and estate-management opportunities; certificate ratings alone do not confirm retrofit need."))
          ).otherwise(F.lit(ENERGY_ABSENT_SENTENCE))
dq_txt = F.when(F.col("dq_issue_count") > 0, F.concat(F.lit("Data-quality uncertainty (not evidence of poor property performance): "), F.col("dq_issue_list"), F.lit(".")))
SC2 = (SC2.withColumn("priority_reasons", F.col("_r")).withColumn("energy_context", energy)
          .withColumn("priority_explanation", F.concat_ws(" ", lead, energy, dq_txt)).drop("_r")
          .withColumn("map_latitude", F.col("latitude_clean")).withColumn("map_longitude", F.col("longitude_clean")))

KEY_COLS = [REF, "property_name", "full_address", "property_number", "street_name", "town", "postcode", "postcode_clean", "region",
            "local_authority", "easting", "northing", "ni_easting", "ni_northing", "latitude", "longitude", "map_latitude", "map_longitude",
            "has_valid_coordinates", "coordinate_status", "investigation_priority_score", "priority_band", "priority_explanation",
            *COMPONENTS, *[f"{c}_weighted" for c in COMPONENTS], "total_occupied_area_sqm", "total_vacant_area_sqm", "surplus_status", "status_of_sale"]
RES = SC2.select(*KEY_COLS, *[c for c in SC2.columns if c not in KEY_COLS])
RES = save_delta(RES, "property_priority_results", "Explainable investigation priority per property with component scores, explanations and all location fields (map-ready)")
display(RES.orderBy(F.desc("investigation_priority_score")).select(REF, "property_name", "town", "investigation_priority_score", "priority_band", "priority_explanation").limit(10))

# COMMAND ----------

# DBTITLE 1,12. Visualisations
# MAGIC %md
# MAGIC ## 12. Visualisations
# MAGIC **Map (visual 1).** The next cell outputs a mapping-ready table and also tries an interactive Plotly map. To build the built-in Databricks map instead:
# MAGIC 1. Under the table output, click **+ → Visualization**.
# MAGIC 2. Choose **Map (Markers)**, or a **Point map** widget in an AI/BI dashboard.
# MAGIC 3. Set Latitude = `map_latitude`, Longitude = `map_longitude`, Group by / colour = `priority_band`.
# MAGIC 4. Add `property_name`, `full_address`, `investigation_priority_score` and `priority_explanation` as tooltips.
# MAGIC
# MAGIC **Power BI:**
# MAGIC 1. Connect with the Azure Databricks connector to `property_priority_results` in the output schema, and filter `has_valid_coordinates = True`.
# MAGIC 2. Set the data category of `map_latitude` to *Latitude* and `map_longitude` to *Longitude*, both *Don't summarize*.
# MAGIC 3. Use the Azure Maps or Map visual: Legend = `priority_band`, Size = `investigation_priority_score`, Tooltips = name, address and explanation.

# COMMAND ----------

# DBTITLE 1,Visual 1: Priority map
RES = spark.table(T("property_priority_results"))
map_df = RES.filter("has_valid_coordinates").select(REF, "property_name", "full_address", "town", "postcode", "region", "local_authority",
    "map_latitude", "map_longitude", "priority_band", "investigation_priority_score", "priority_explanation")
display(map_df.orderBy(F.desc("investigation_priority_score")))

# Interactive map (visual only): every property at Low band or above, plus a 10% sample of Very Low for context
try:
    import plotly.express as px
    band_order = [b for _, b in PRIORITY_BANDS]
    colours = {"Very High": "#b2182b", "High": "#ef8a62", "Medium": "#f4c430", "Low": "#67a9cf", "Very Low": "#bdbdbd"}
    plot_df = map_df.filter("priority_band != 'Very Low'").unionByName(map_df.filter("priority_band = 'Very Low'").sample(0.1, seed=42))
    pdf = plot_df.select("property_name", "full_address", "map_latitude", "map_longitude", "priority_band", "investigation_priority_score").toPandas()
    kw = dict(lat="map_latitude", lon="map_longitude", color="priority_band", category_orders={"priority_band": band_order},
              color_discrete_map=colours, hover_name="property_name", hover_data=["full_address", "investigation_priority_score"],
              zoom=4.5, center={"lat": 54.5, "lon": -3.0}, height=700, title="Government properties by investigation priority band")
    if hasattr(px, "scatter_map"):
        fig = px.scatter_map(pdf, map_style="open-street-map", **kw)
    else:
        fig = px.scatter_mapbox(pdf, mapbox_style="open-street-map", **kw)
    fig.update_layout(margin=dict(l=0, r=0, t=40, b=0))
    fig.show()
except Exception as e:
    print(f"Interactive map not rendered ({str(e)[:150]}); use the table above with the map instructions.")

# COMMAND ----------

# DBTITLE 1,Visual 2: Top 20 properties by priority
top20 = (RES.orderBy(F.desc("investigation_priority_score"), REF).limit(20)
           .select(F.concat(F.col("property_name"), F.lit(" ("), F.col(REF), F.lit(")")).alias("property"), "town", "region",
                   "investigation_priority_score", "priority_band", *[f"{c}_weighted" for c in COMPONENTS]))
display(top20)

# COMMAND ----------

# DBTITLE 1,Visual 3: Vacant area by region
display(RES.filter(F.col("total_vacant_area_sqm") > 0).groupBy(F.coalesce("region", F.lit("(region missing)")).alias("region"))
           .agg(F.round(F.sum("total_vacant_area_sqm"), 0).alias("total_vacant_area_sqm"), F.count("*").alias("properties_with_vacancy"))
           .orderBy(F.desc("total_vacant_area_sqm")))

# COMMAND ----------

# DBTITLE 1,Visual 4: Vacant area by local authority
display(RES.filter(F.col("total_vacant_area_sqm") > 0).groupBy(F.coalesce("local_authority", F.lit("(local authority missing)")).alias("local_authority"))
           .agg(F.round(F.sum("total_vacant_area_sqm"), 0).alias("total_vacant_area_sqm"), F.count("*").alias("properties_with_vacancy"))
           .orderBy(F.desc("total_vacant_area_sqm")).limit(25))

# COMMAND ----------

# DBTITLE 1,Visual 5: Properties by surplus and sale status
display(RES.filter(F.col("surplus_record_count") > 0)
           .groupBy(F.when(F.col("surplus_status"), "Surplus").otherwise("Not surplus").alias("surplus_status"),
                    F.coalesce("status_of_sale", F.lit("(no sale status)")).alias("status_of_sale"))
           .agg(F.count("*").alias("properties"), F.round(F.sum("total_surplus_floor_area_sqm"), 0).alias("surplus_floor_area_sqm"),
                F.round(F.sum("total_surplus_land_area_hectares"), 2).alias("surplus_land_area_hectares"))
           .orderBy(F.desc("properties")))

# COMMAND ----------

# DBTITLE 1,Visual 6: Consolidation opportunities by town and postcode area
cons = RES.filter("consolidation_opportunity_flag OR local_consolidation_cluster_flag")
print(f"Properties with a consolidation indicator: {cons.count():,}")
display(cons.groupBy(F.coalesce("town_clean", F.lit("(town missing)")).alias("town"))
            .agg(F.count("*").alias("properties"), F.sum(F.col("consolidation_opportunity_flag").cast("int")).alias("on_site_consolidation"),
                 F.sum(F.col("local_consolidation_cluster_flag").cast("int")).alias("local_cluster"),
                 F.round(F.sum(F.coalesce("total_vacant_area_sqm", F.lit(0.0))), 0).alias("vacant_area_sqm"))
            .orderBy(F.desc("properties"), F.desc("vacant_area_sqm")).limit(25))
display(cons.groupBy(F.coalesce("postcode_area", F.lit("(no valid postcode)")).alias("postcode_area"))
            .agg(F.count("*").alias("properties"), F.round(F.sum(F.coalesce("total_vacant_area_sqm", F.lit(0.0))), 0).alias("vacant_area_sqm"))
            .orderBy(F.desc("properties")).limit(25))

# COMMAND ----------

# DBTITLE 1,Visual 7: Data-quality issues by dataset
DQ = spark.table(T("property_data_quality_issues"))
display(DQ.groupBy("source_dataset", "severity").agg(F.count("*").alias("issues")).orderBy("source_dataset", "severity"))

# COMMAND ----------

# DBTITLE 1,Visual 8: Property priority summary table
display(RES.select("property_name", "full_address", "town", "postcode", "region", "local_authority",
                   F.col("total_occupied_area_sqm").alias("total_occupied_area_sqm"), F.col("total_vacant_area_sqm").alias("total_vacant_area_sqm"),
                   F.when(F.col("surplus_flag"), F.concat(F.lit("Surplus - "), F.coalesce("status_of_sale", F.lit("no sale status")))).otherwise(F.lit("Not surplus")).alias("surplus_status"),
                   "investigation_priority_score", "priority_band", "priority_explanation")
           .orderBy(F.desc("investigation_priority_score")))

# COMMAND ----------

# DBTITLE 1,13. Data-quality report
# MAGIC %md
# MAGIC ## 13. Data-quality report
# MAGIC Issue counts by dataset and type, column-mapping gaps, documented assumptions, property-level uncertainty and coordinate usability. Full detail is in `property_data_quality_issues`: one row per issue, with the original value and the source row id.

# COMMAND ----------

# DBTITLE 1,Data-quality summaries
display(DQ.groupBy("source_dataset", "issue_type", "severity").agg(F.count("*").alias("issues"), F.countDistinct("property_reference").alias("properties_affected"))
          .orderBy("source_dataset", F.desc("issues")))
print("Column-mapping notes and documented assumptions:")
display(DQ.filter("severity = 'Info'").select("source_dataset", "issue_type", "field_name", "original_value"))
print("Property-level data-quality uncertainty (feeds data_uncertainty_score):")
display(RES.select(REF, F.explode(F.split("dq_issue_list", "; ")).alias("issue")).groupBy("issue").agg(F.count("*").alias("properties")).orderBy(F.desc("properties")))
print("Coordinate usability by dataset:")
display(reduce(lambda a, b2: a.unionByName(b2), [spark.table(T(f"{ds}_clean")).groupBy(F.lit(ds).alias("dataset"), "coordinate_status").count()
                                                 for ds in SOURCE_TABLES_FOUND]).orderBy("dataset", "coordinate_status"))

# COMMAND ----------

# DBTITLE 1,14. Validation
# MAGIC %md
# MAGIC ## 14. Validation
# MAGIC Automated checks:
# MAGIC * Row counts are preserved from the bronze tables to the clean tables.
# MAGIC * `property_master` has one row per property and keeps every source reference.
# MAGIC * The results table matches the master and the source location rows field by field.
# MAGIC * Scores stay within 0–100, recompute exactly, and agree with their bands.
# MAGIC * Explanations exist for every property, use the energy sentence correctly, and never claim inefficiency.
# MAGIC * All evidence ids trace back to clean source rows, and Low or Unmatched records are never linked.
# MAGIC
# MAGIC Reports follow: unmatched records by dataset, Low-confidence matches, records without usable coordinates, and the top properties with their component scores.

# COMMAND ----------

# DBTITLE 1,Validation checks
checks = []
def add_check(name, passed, detail):
    checks.append((name, bool(passed), str(detail)))

PM = spark.table(T("property_master")); RES = spark.table(T("property_priority_results"))
# 1. Record counts before/after
for ds in SOURCE_TABLES_FOUND:
    n_src = spark.table(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{ds}").count(); n_cln = spark.table(T(f"{ds}_clean")).count()
    add_check(f"Row count preserved: {ds}", n_src == n_cln, f"bronze={n_src:,}, clean={n_cln:,}")
    m = spark.table(T(f"{ds}_clean")).agg(*[F.count(c).alias(c) for c in ["latitude", "longitude", "postcode", "property_name"]]).first()
    s = spark.table(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{ds}").agg(*[F.count(c).alias(a) for c, a in [("Latitude", "latitude"), ("Longitude", "longitude"), ("Postcode", "postcode"), ("Property Name", "property_name")]]).first()
    add_check(f"Original location values retained: {ds}", m.asDict() == s.asDict(), f"non-null counts clean={m.asDict()} bronze={s.asDict()}")
# 2. One row per property; every source reference present
n_pm, n_pm_ref = PM.count(), PM.select(REF).distinct().count()
add_check("property_master: one row per property reference", n_pm == n_pm_ref, f"rows={n_pm:,}, distinct refs={n_pm_ref:,}")
src_refs = reduce(lambda a, b2: a.unionByName(b2), [spark.table(T(f"{d}_clean")).select(REF) for d in ["property"] + OTHER_REF_DATASETS]).filter(F.col(REF).isNotNull()).distinct()
missing_refs = src_refs.join(PM.select(REF), REF, "left_anti").count()
add_check("Every non-null source property reference is in property_master", missing_refs == 0, f"missing={missing_refs}")
# 3. Results vs master; locations not lost
n_res = RES.count()
add_check("property_priority_results rows = property_master rows", n_res == n_pm, f"results={n_res:,}, master={n_pm:,}")
LOC_CHECK = LOC + ["latitude_clean", "longitude_clean", "postcode_clean", "full_address", "has_valid_coordinates"]
cmp = PM.select(REF, *[F.col(c).alias(f"a_{c}") for c in LOC_CHECK]).join(RES.select(REF, *[F.col(c).alias(f"b_{c}") for c in LOC_CHECK]), REF, "full")
loc_mismatch = cmp.filter(reduce(lambda x, y: x | y, [~F.col(f"a_{c}").eqNullSafe(F.col(f"b_{c}")) for c in LOC_CHECK])).count()
add_check("Location fields identical in property_master and results", loc_mismatch == 0, f"mismatching properties={loc_mismatch}")
src_loc = reduce(lambda a, b2: a.unionByName(b2), [spark.table(T(f"{d}_clean")).select(F.col("source_row_id").alias("location_source_row_id"), *[F.col(c).alias(f"s_{c}") for c in LOC])
                                                   for d in ["property"] + OTHER_REF_DATASETS])
tr = RES.select(REF, "location_source_row_id", *LOC).join(src_loc, "location_source_row_id", "left")
not_traced = tr.filter(reduce(lambda x, y: x | y, [~F.col(c).eqNullSafe(F.col(f"s_{c}")) for c in LOC])).count()
add_check("Every property location equals its source row (location_source_row_id)", not_traced == 0, f"untraceable locations={not_traced}")
add_check("Valid-coordinate count unchanged master -> results", PM.filter("has_valid_coordinates").count() == RES.filter("has_valid_coordinates").count(), "")
# 4. Score range, recomputation, bands
st = RES.agg(*[F.min(c).alias(f"min_{c}") for c in COMPONENTS + ["investigation_priority_score"]],
             *[F.max(c).alias(f"max_{c}") for c in COMPONENTS + ["investigation_priority_score"]],
             *[F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}") for c in COMPONENTS + ["investigation_priority_score"]]).first().asDict()
for c in COMPONENTS + ["investigation_priority_score"]:
    add_check(f"{c} within 0-100 and never null", st[f"min_{c}"] >= 0 and st[f"max_{c}"] <= 100 and st[f"null_{c}"] == 0,
              f"min={st[f'min_{c}']}, max={st[f'max_{c}']}, nulls={st[f'null_{c}']}")
recomp = reduce(lambda a, b2: a + b2, [F.col(c) * w for c, w in WEIGHTS.items()])
bad_recomp = RES.filter(F.abs(F.round(recomp, 1) - F.col("investigation_priority_score")) > 0.05).count()
add_check("Score equals weighted sum of components", bad_recomp == 0, f"mismatches={bad_recomp}")
band_bad = RES.filter(~(((F.col("investigation_priority_score") >= 80) & (F.col("priority_band") == "Very High")) |
    ((F.col("investigation_priority_score") >= 60) & (F.col("investigation_priority_score") < 80) & (F.col("priority_band") == "High")) |
    ((F.col("investigation_priority_score") >= 40) & (F.col("investigation_priority_score") < 60) & (F.col("priority_band") == "Medium")) |
    ((F.col("investigation_priority_score") >= 20) & (F.col("investigation_priority_score") < 40) & (F.col("priority_band") == "Low")) |
    ((F.col("investigation_priority_score") < 20) & (F.col("priority_band") == "Very Low")))).count()
add_check("Priority band consistent with score", band_bad == 0, f"inconsistent={band_bad}")
# 5. Explanations
add_check("Every property has an explanation", RES.filter(F.col("priority_explanation").isNull() | (F.length("priority_explanation") < 20)).count() == 0, "")
no_energy_wrong = RES.filter(~F.col("has_energy_data") & ~F.col("priority_explanation").contains(ENERGY_ABSENT_SENTENCE)).count()
add_check("Energy-absent sentence present wherever no EPC/DEC data", no_energy_wrong == 0, f"missing={no_energy_wrong}")
add_check("No explanation claims energy inefficiency", RES.filter(F.lower("priority_explanation").contains("inefficien")).count() == 0, "")
add_check("Data-quality text labelled as uncertainty", RES.filter((F.col("dq_issue_count") > 0) & ~F.col("priority_explanation").contains("not evidence of poor property performance")).count() == 0, "")
# 6. Traceability of evidence
for score_col, ids_col, tbl in [("vacancy_score", "vacant_source_row_ids", "vacant_space_clean"), ("vacancy_duration_score", "vacant_source_row_ids", "vacant_space_clean"),
                                ("surplus_score", "surplus_source_row_ids", "surplus_clean")]:
    n_bad = RES.filter((F.col(score_col) > 0) & (F.size(ids_col) == 0)).count()
    add_check(f"{score_col} > 0 always backed by {tbl} records", n_bad == 0, f"unbacked={n_bad}")
n_bad = RES.filter(F.col("consolidation_opportunity_flag") & ((F.size("occupation_source_row_ids") == 0) | (F.size("vacant_source_row_ids") == 0))).count()
add_check("Consolidation flags backed by occupation and vacant-space records", n_bad == 0, f"unbacked={n_bad}")
for ids_col, tbl in [("occupation_source_row_ids", "occupation_clean"), ("vacant_source_row_ids", "vacant_space_clean"), ("surplus_source_row_ids", "surplus_clean"),
                     ("land_source_row_ids", "land_clean"), ("building_source_row_ids", "building_clean"), ("property_source_row_ids", "property_clean")]:
    orphan = RES.select(F.explode(ids_col).alias("source_row_id")).distinct().join(spark.table(T(tbl)).select("source_row_id"), "source_row_id", "left_anti").count()
    add_check(f"All {ids_col} exist in {tbl}", orphan == 0, f"orphans={orphan}")
MT = spark.table(T("property_location_matches"))
low_linked = MT.filter(F.col("property_match_confidence").isin("Low", "Unmatched") & F.col("linked_property_reference").isNotNull()).count()
add_check("Low/Unmatched matches never linked automatically", low_linked == 0, f"linked={low_linked}")

validation_df = spark.createDataFrame(checks, "check string, passed boolean, detail string")
display(validation_df)
n_fail = validation_df.filter("NOT passed").count()
print("ALL VALIDATION CHECKS PASSED" if n_fail == 0 else f"{n_fail} VALIDATION CHECK(S) FAILED - see table above")

# COMMAND ----------

# DBTITLE 1,Validation reports
print("Unmatched records by dataset:")
display(MT.groupBy("source_dataset").agg(F.count("*").alias("records"), F.sum((F.col("property_match_confidence") == "Unmatched").cast("int")).alias("unmatched"),
                                         F.sum((F.col("property_match_confidence") == "Low").cast("int")).alias("low_confidence")).orderBy("source_dataset"))
print("Low-confidence and unmatched records (retained for review, not combined):")
display(MT.filter(F.col("property_match_confidence").isin("Low", "Unmatched")).select("source_dataset", "source_row_id", "record_reference", "property_name", "full_address",
        "postcode_clean", "latitude_clean", "longitude_clean", "candidate_property_reference", "matched_property_name", "property_match_method",
        "property_match_confidence", "match_distance_m", "property_match_notes"))
print("Records without usable coordinates by dataset:")
display(reduce(lambda a, b2: a.unionByName(b2), [spark.table(T(f"{ds}_clean")).filter("NOT has_valid_coordinates").groupBy(F.lit(ds).alias("dataset"), "coordinate_status").count()
                                                 for ds in SOURCE_TABLES_FOUND]).orderBy("dataset"))
print("Top 20 properties with component scores:")
display(RES.orderBy(F.desc("investigation_priority_score")).select(REF, "property_name", "town", "region", "investigation_priority_score", "priority_band",
        *COMPONENTS, "total_vacant_area_sqm", "vacant_area_ratio", "max_vacancy_days", "status_of_sale").limit(20))

# COMMAND ----------

# DBTITLE 1,15. Export and next steps
# MAGIC %md
# MAGIC ## 15. Export and next steps
# MAGIC All outputs are Delta tables in `TARGET_SCHEMA`. The next cell lists them with row counts. `stg_*` tables are intermediate checkpoints; set `DROP_STAGING_TABLES = True` in Section 1 to remove them at the end of a run.
# MAGIC
# MAGIC **Suggested next steps**
# MAGIC * Review the Low-confidence and unmatched land records, and `property_master` rows with `data_quality_risk_flag`.
# MAGIC * Confirm the area-unit assumptions (IPMS treated as sqm; land and surplus areas treated as hectares) with the data owner.
# MAGIC * Validate the score weights and thresholds with estate managers, for example against a sample of known consolidation or disposal cases.
# MAGIC * EPC/DEC coverage is partial. Extend it, and consider adding a separately labelled energy-performance component once coverage is sufficient.
# MAGIC * Build an AI/BI dashboard or a Power BI report on `property_priority_results` (map, top-N list, regional vacancy) and schedule this notebook as a job.

# COMMAND ----------

# DBTITLE 1,Output table inventory
OUTPUTS = ["property_master", "property_clean", "building_clean", "occupation_clean", "land_clean", "surplus_clean", "vacant_space_clean",
           "property_location_matches", "property_priority_results", "property_data_quality_issues", "column_mapping"]
inv = [(t, T(t), spark.table(T(t)).count(), "output") for t in OUTPUTS]
stg = [r.tableName for r in spark.sql(f"SHOW TABLES IN {TARGET_SCHEMA}").collect() if r.tableName.startswith("stg_")]
inv += [(t, T(t), spark.table(T(t)).count(), "staging") for t in stg]
display(spark.createDataFrame(inv, "table string, full_name string, row_count long, kind string"))
if DROP_STAGING_TABLES:
    for t in stg:
        spark.sql(f"DROP TABLE IF EXISTS {T(t)}")
    print(f"Dropped {len(stg)} staging tables")