"""Pure-Python helpers for the Sustainability & investment prioritisation task (no Spark or Streamlit).

All thresholds are indicative analytical assumptions, not government policy.
"""
import math
import re

EARTH_RADIUS_MILES = 3958.7613
METRES_PER_MILE = 1609.344

# Default proximity bands (upper bounds, miles): Local <=10, Accessible <=25, Extended <=50, Distant >50.
DEFAULT_BANDS = (10.0, 25.0, 50.0)
BAND_NAMES = ("Local", "Accessible", "Extended", "Distant")
BAND_POINTS = (10, 7, 4, 1)
UNAVAILABLE = "Proximity unavailable"

CORE_BANDS = ((75, "Very high priority"), (60, "High priority"), (40, "Medium priority"),
              (0, "Lower priority based on available evidence"))

# Component maxima of the 100-point core score.
COMPONENTS = (("A", "Vacancy and underutilisation", 30), ("B", "Surplus and disposal position", 25),
              ("C", "Consolidation opportunity", 20), ("D", "Property and contract review", 10),
              ("E", "Sustainability opportunity indicator", 10), ("F", "Data-quality risk", 5))

CATEGORIES = ("Consolidation opportunity", "Vacant-space intervention", "Surplus-property review",
              "Disposal acceleration", "Sustainability investigation", "Contract and duplication review",
              "Local investigation opportunity", "Data-quality investigation")

DEFAULT_CORE_WEIGHT = 0.85

_QUESTION_TERMS = {
    "Consolidation opportunity": ("consolidat", "co-locat", "colocat", "relocat", "move"),
    "Vacant-space intervention": ("vacant", "empty", "unused", "underutil", "under-util", "utilis", "utiliz"),
    "Surplus-property review": ("surplus",),
    "Disposal acceleration": ("dispos", "sale", "sell", "market"),
    "Sustainability investigation": ("sustainab", "carbon", "energy", "emission", "net zero", "net-zero", "retrofit"),
    "Contract and duplication review": ("contract", "lease", "duplicat"),
    "Data-quality investigation": ("data quality", "data-quality", "conflict", "missing", "error"),
}
_LOCAL_TERMS = ("local", "near", "close to", "nearby", "within", "commut", "travel", "around", "proximity")


def haversine_miles(lat1, lon1, lat2, lon2):
    """Approximate great-circle (straight-line) distance in miles between two WGS84 points."""
    if None in (lat1, lon1, lat2, lon2):
        return None
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_MILES * math.asin(math.sqrt(a))


def bng_miles(e1, n1, e2, n2):
    """Straight-line distance in miles between two British National Grid (EPSG:27700) points."""
    if None in (e1, n1, e2, n2):
        return None
    return math.hypot(e2 - e1, n2 - n1) / METRES_PER_MILE


def validate_bands(bands):
    """Return bands as three strictly increasing positive floats, else the defaults."""
    try:
        b = tuple(float(x) for x in bands)
    except (TypeError, ValueError):
        return DEFAULT_BANDS
    if len(b) != 3 or not (0 < b[0] < b[1] < b[2]):
        return DEFAULT_BANDS
    return b


def proximity_band(miles, bands=DEFAULT_BANDS):
    if miles is None or (isinstance(miles, float) and math.isnan(miles)):
        return UNAVAILABLE
    for limit, name in zip(bands, BAND_NAMES):
        if miles <= limit:
            return name
    return BAND_NAMES[-1]


def proximity_points(miles, bands=DEFAULT_BANDS):
    """Optional proximity score (separate from the core score). None when distance is unavailable."""
    band = proximity_band(miles, bands)
    return None if band == UNAVAILABLE else BAND_POINTS[BAND_NAMES.index(band)]


def core_priority_label(score):
    if score is None:
        return "Not scored"
    for floor, label in CORE_BANDS:
        if score >= floor:
            return label
    return CORE_BANDS[-1][1]


def combined_score(core, prox_points, core_weight=DEFAULT_CORE_WEIGHT):
    """Weighted ranking score (0-100): core score and proximity (rescaled from 0-10 to 0-100).
    Returns None when proximity is unavailable, so such properties are not silently penalised."""
    if core is None or prox_points is None:
        return None
    w = min(max(float(core_weight), 0.0), 1.0)
    return round(core * w + prox_points * 10 * (1 - w), 1)


def interpret_question(text):
    """Keyword interpretation of the user's question. Returns focus categories and whether local
    options are explicitly prioritised. Transparent and deliberately simple: results are shown to the user."""
    t = (text or "").lower()
    cats = [c for c, terms in _QUESTION_TERMS.items() if any(k in t for k in terms)]
    local = any(k in t for k in _LOCAL_TERMS)
    if local:
        cats.append("Local investigation opportunity")
    return {"categories": cats, "local_focus": local}


def postcode_parts(pc):
    """(area, district) of a normalised UK postcode, e.g. 'SW1A 2AA' -> ('SW', 'SW1A')."""
    pc = (pc or "").strip().upper()
    district = pc.split(" ")[0] if pc else ""
    m = re.match(r"[A-Z]{1,2}", district)
    return (m.group(0) if m else "", district)
