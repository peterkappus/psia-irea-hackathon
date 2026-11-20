"""GovMove configuration.

LANDING LOCATION (the only values that must be set)
---------------------------------------------------
  GOVMOVE_CATALOG   Catalog where the 'GovMove Setup' notebook landed all datasets.   Placeholder: <catalog_name>
  GOVMOVE_SCHEMA    Schema where the 'GovMove Setup' notebook landed all datasets.    Placeholder: <schema_name>

Use the same values as the 'catalog' and 'schema' widgets of the GovMove Setup notebook. Every table the app
reads is <GOVMOVE_CATALOG>.<GOVMOVE_SCHEMA>.<name>; the names are fixed by the setup notebook:
  source:   property, building, land, occupation, surplus, vacant_space, addressbasepremium
  app:      govmove_property_locations, govmove_postcode_centroids, govmove_property_evidence,
            govmove_property_clean, govmove_building_clean, govmove_land_clean, govmove_occupation_clean

OPTIONAL
  GOVMOVE_ADDRESSBASE_TABLE   AddressBase Premium table or view used as the postcode-lookup fallback. A bare name
                              is resolved in <GOVMOVE_CATALOG>.<GOVMOVE_SCHEMA>; a full catalog.schema.table name
                              is used as is. Default: addressbasepremium (the view built by the setup notebook).
                              Placeholder: <catalog>.<schema>.<addressbase_table>
  GOVMOVE_PRIORITY_TABLE      Full name of property_priority_results from the prioritisation notebook (not a
                              source dataset). Used by Estate opportunity explorer and Sustainability &
                              investment prioritisation. Placeholder: <catalog>.<schema>.property_priority_results
  GOVMOVE_COMPUTE             'serverless' or an all-purpose cluster ID used by Databricks Connect.
  GOVMOVE_SQL_WAREHOUSE_ID    Optional; not used by the app at runtime.
  GOVMOVE_AVAILABILITY_RULES  Optional JSON overriding AVAILABILITY_RULES below.

Defaults below are the values for the existing development environment.
"""
import json
import os

CATALOG = os.getenv("GOVMOVE_CATALOG", "manual_sandbox_dev")
SCHEMA = os.getenv("GOVMOVE_SCHEMA", "property_prioritisation")
COMPUTE = os.getenv("GOVMOVE_COMPUTE", "serverless")
SQL_WAREHOUSE_ID = os.getenv("GOVMOVE_SQL_WAREHOUSE_ID", "<sql_warehouse_id>")
ADDRESSBASE_TABLE = os.getenv("GOVMOVE_ADDRESSBASE_TABLE", "").strip() or "addressbasepremium"


def fq(table: str) -> str:
    """<GOVMOVE_CATALOG>.<GOVMOVE_SCHEMA>.<table> (names that are already fully qualified are returned unchanged)."""
    return table if table.count(".") == 2 else f"{CATALOG}.{SCHEMA}.{table}"


# Source datasets (landed by the GovMove Setup notebook).
SOURCE_TABLES = {name: fq(name) for name in ("property", "building", "land", "occupation", "surplus", "vacant_space")}
SOURCE_TABLES["addressbasepremium"] = fq(ADDRESSBASE_TABLE)

# App tables (built by the GovMove Setup notebook in the same schema).
PROPERTY_TABLE_FQ = fq("govmove_property_locations")
POSTCODE_TABLE_FQ = fq("govmove_postcode_centroids")
POSTCODE_FALLBACK_TABLE = SOURCE_TABLES["addressbasepremium"]
EVIDENCE_TABLE_FQ = fq("govmove_property_evidence")
CLEAN_TABLES = {name: fq(f"govmove_{name}_clean") for name in ("property", "building", "land", "occupation")}

# Optional output of the prioritisation notebook (not part of the landing schema).
PRIORITY_TABLE_FQ = os.getenv("GOVMOVE_PRIORITY_TABLE",
                              "manual_sandbox_dev.property_prioritisation.property_priority_results")

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
