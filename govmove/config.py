"""GovMove configuration.

CONFIGURATION VALUES TO REPLACE BEFORE DEPLOYMENT
-------------------------------------------------
Each value can be set as an environment variable in app.yaml (preferred) or edited here.

  GOVMOVE_CATALOG                  Catalog holding the cleaned GovMove tables.        Placeholder: <catalog_name>
  GOVMOVE_SCHEMA                   Schema holding the cleaned GovMove tables.         Placeholder: <schema_name>
  GOVMOVE_PROPERTY_TABLE           Cleaned property-location table (data-prep).       Placeholder: <property_table_name>
  GOVMOVE_POSTCODE_TABLE           Postcode centroid lookup table (data-prep).        Placeholder: <postcode_lookup_table_name>
  GOVMOVE_POSTCODE_FALLBACK_TABLE  Raw approved address table used if the centroid   Placeholder: <catalog>.<schema>.<address_table>
                                   table is unavailable (AddressBase Premium).
  GOVMOVE_COMPUTE                  'serverless' or an all-purpose cluster ID used by  Placeholder: <serverless | cluster_id>
                                   Databricks Connect (PySpark).
  GOVMOVE_SQL_WAREHOUSE_ID         SQL warehouse ID used to run the data-prep SQL     Placeholder: <sql_warehouse_id>
                                   (optional; not used by the app at runtime).
  GOVMOVE_SOURCE_CATALOG/SCHEMA    Bronze source tables used by the data-prep notebook (property, vacant_space, surplus).
  GOVMOVE_AVAILABILITY_RULES       Optional JSON overriding AVAILABILITY_RULES below.

Defaults below point at the development environment this app was built against.
"""
import json
import os

CATALOG = os.getenv("GOVMOVE_CATALOG", "manual_sandbox_dev")
SCHEMA = os.getenv("GOVMOVE_SCHEMA", "property_prioritisation")
PROPERTY_TABLE = os.getenv("GOVMOVE_PROPERTY_TABLE", "govmove_property_locations")
POSTCODE_TABLE = os.getenv("GOVMOVE_POSTCODE_TABLE", "govmove_postcode_centroids")
POSTCODE_FALLBACK_TABLE = os.getenv("GOVMOVE_POSTCODE_FALLBACK_TABLE", "address_dev.bronze.addressbasepremium")
COMPUTE = os.getenv("GOVMOVE_COMPUTE", "serverless")
SQL_WAREHOUSE_ID = os.getenv("GOVMOVE_SQL_WAREHOUSE_ID", "<sql_warehouse_id>")
SOURCE_CATALOG = os.getenv("GOVMOVE_SOURCE_CATALOG", "manual_sandbox_dev")
SOURCE_SCHEMA = os.getenv("GOVMOVE_SOURCE_SCHEMA", "bronze")


def fq(table: str) -> str:
    """Fully qualify a table name with the configured catalog and schema unless already qualified."""
    return table if table.count(".") == 2 else f"{CATALOG}.{SCHEMA}.{table}"


PROPERTY_TABLE_FQ = fq(PROPERTY_TABLE)
POSTCODE_TABLE_FQ = fq(POSTCODE_TABLE)

# One row per property with related-record counts (built by the data-prep notebook). Placeholder: <evidence_table_name>
EVIDENCE_TABLE_FQ = fq(os.getenv("GOVMOVE_EVIDENCE_TABLE", "govmove_property_evidence"))
# Cleaned source tables used by the Estate Intelligence views. Placeholders: <property_clean> etc.
CLEAN_TABLES = {
    "property": fq(os.getenv("GOVMOVE_PROPERTY_CLEAN_TABLE", "property_clean")),
    "building": fq(os.getenv("GOVMOVE_BUILDING_CLEAN_TABLE", "building_clean")),
    "land": fq(os.getenv("GOVMOVE_LAND_CLEAN_TABLE", "land_clean")),
    "occupation": fq(os.getenv("GOVMOVE_OCCUPATION_CLEAN_TABLE", "occupation_clean")),
}
# Priority scores from the prioritisation notebook (Estate opportunity explorer). Placeholder: <priority_results_table>
PRIORITY_TABLE_FQ = fq(os.getenv("GOVMOVE_PRIORITY_TABLE", "property_priority_results"))

# Availability rules per property dataset. A record counts as 'available' when its
# availability_status is in default_statuses. Users can explicitly pick other statuses in the UI.
AVAILABILITY_RULES = {
    "vacant_space": {
        "label": "Vacant space",
        "status_source": "Vacant Space Status",
        "default_statuses": ["Available"],
    },
    "surplus": {
        "label": "Surplus property",
        "status_source": "Status of Sale",
        "default_statuses": ["On the Market", "Not on the Market"],
    },
}
if os.getenv("GOVMOVE_AVAILABILITY_RULES"):
    AVAILABILITY_RULES = json.loads(os.environ["GOVMOVE_AVAILABILITY_RULES"])

DEFAULT_DATASET = "vacant_space"
DEFAULT_RESULTS = 10
MAX_RESULTS = 200

# Reasonable coordinate ranges (metres).
# British National Grid (EPSG:27700) covers GB and, by extension, Northern Ireland.
BNG_EASTING_RANGE = (0.0, 700000.0)
BNG_NORTHING_RANGE = (0.0, 1300000.0)
# Irish Grid (EPSG:29903, TM75) for Northern Ireland 'NI Easting' / 'NI Northing'.
IRISH_GRID_EPSG = 29903
IRISH_GRID_EASTING_RANGE = (0.0, 400000.0)
IRISH_GRID_NORTHING_RANGE = (0.0, 500000.0)

# Coordinate statuses (set by the data-prep notebook) that can be compared in BNG.
COMPARABLE_STATUSES = ["valid_bng", "transformed_from_irish_grid"]
