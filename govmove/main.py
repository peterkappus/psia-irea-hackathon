"""GovMove: Government Estate Intelligence (Streamlit Databricks App entry point).

Layout follows the GOV.UK-styled 'Government Estate Intelligence' template:
  * Estate Intelligence: explore and understand the estate (overview, explorer, dataset analytics,
    co-location, data quality).
  * Estate Planning & Decision Support: task-led investigations, including the postcode-based
    Vacant space finder (straight-line distance on British National Grid).
Configuration values to replace before deployment are listed in config.py and README.md.
"""
import base64
import os

import streamlit as st

ASSETS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets")


def logo_data_uri():
    """Brand logo as a data URI. A PNG at assets/govmove_logo.png (the original artwork) takes precedence
    over the vector recreation at assets/govmove_logo.svg."""
    for name, mime in (("govmove_logo.png", "image/png"), ("govmove_logo.svg", "image/svg+xml")):
        path = os.path.join(ASSETS, name)
        if os.path.exists(path):
            with open(path, "rb") as fh:
                return f"data:{mime};base64,{base64.b64encode(fh.read()).decode()}"
    return None


def page_icon():
    """Browser tab icon: the two-building GovMove mark (no wordmark, so it stays legible at 16-32 px).
    Uses assets/govmove_icon.png if supplied; otherwise draws the mark with Pillow from the same
    geometry as assets/govmove_logo.svg."""
    path = os.path.join(ASSETS, "govmove_icon.png")
    if os.path.exists(path):
        return path
    try:
        from PIL import Image, ImageDraw

        size, s = 256, 2.1                       # canvas px, scale from SVG units
        ox, oy = (size - 116 * s) / 2, (size - 104 * s) / 2 - 1 * s
        pt = lambda pts: [(ox + x * s, oy + y * s) for x, y in pts]
        img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
        d = ImageDraw.Draw(img)
        d.polygon(pt([(0, 32), (62, 1), (62, 63), (59, 61), (59, 76), (31, 76), (31, 90), (59, 90), (59, 105), (0, 105)]),
                  fill=(13, 31, 69, 255))
        d.polygon(pt([(67, 23), (116, 40), (116, 104), (65, 104), (65, 99), (82, 83), (67, 68)]), fill=(7, 134, 107, 255))
        for x, y, w, h in [(15, 34, 12, 12), (34, 34, 12, 12), (15, 54, 12, 13), (34, 54, 12, 13),
                           (81, 45, 9, 9), (97, 45, 10, 9), (81, 61, 9, 10), (97, 61, 10, 10)]:
            d.rectangle(pt([(x, y), (x + w, y + h)]), fill=(255, 255, 255, 255))
        return img
    except Exception:
        return None  # fall back to Streamlit's default icon


st.set_page_config(page_title="GovMove — Government Estate Intelligence", page_icon=page_icon(), layout="wide",
                   initial_sidebar_state="expanded")

CSS = """
<style>
:root { --blue:#1d70b8; --navy:#0b0c0c; --ink:#17222d; --muted:#5f6c78; --line:#d8e0e7; --bg:#f4f6f8;
        --pale:#eaf3fb; --green:#00703c; --amber:#f47738; --red:#d4351c;
        --brand-navy:#0d1f45; --brand-teal:#07866b; }
.stApp { background: var(--bg); }
/* Hide Streamlit's fixed white top bar (toolbar + decoration) so it cannot overlap the GovMove header or page content */
header[data-testid="stHeader"], div[data-testid="stToolbar"], div[data-testid="stDecoration"] { display: none !important; }
.block-container { padding-top: 1rem !important; }
.gm-header { background: #fff; color: var(--brand-navy); padding: 10px 20px; display: flex; align-items: center; gap: 16px;
             border-bottom: 6px solid var(--brand-teal); box-shadow: 0 1px 0 var(--line); margin: 0 -1rem 12px -1rem; }
.gm-header img { height: 46px; }
.gm-header .gm-wordmark { font-size: 1.6rem; font-weight: 800; }
.gm-header .gm-sep { width: 1px; height: 30px; background: var(--line); }
.gm-header strong { font-size: 1.05rem; font-weight: 600; color: var(--brand-navy); }
.gm-header .gm-status { margin-left: auto; font-size: .85rem; color: var(--brand-navy); background: #e6f4f0;
                        border: 1px solid #b7ded3; padding: 4px 10px; border-radius: 12px; }
.gm-header .gm-status:before { content: ''; display: inline-block; width: 8px; height: 8px; border-radius: 50%;
                               background: #00a94f; margin-right: 6px; }
.gm-kicker { color: var(--blue); font-weight: 700; font-size: .78rem; letter-spacing: .08em; margin-top: 8px; }
.gm-title { color: var(--ink); margin: 2px 0 4px 0; }
.gm-lead { color: var(--muted); max-width: 900px; }
.gm-muted { color: var(--muted); font-size: .85rem; }
.gm-card { background: #fff; border: 1px solid var(--line); border-top: 4px solid var(--brand-teal); border-radius: 4px;
           padding: 12px 14px; margin-bottom: 10px; min-height: 108px; }
.gm-card-label { color: var(--muted); font-size: .8rem; font-weight: 600; text-transform: none; }
.gm-card-value { color: var(--ink); font-size: 1.6rem; font-weight: 700; line-height: 1.3; }
.gm-card-sub { color: var(--muted); font-size: .78rem; }
div[data-testid="stSidebar"] { background: #fff; border-right: 1px solid var(--line); }
.stButton button[kind="primary"] { background: var(--brand-teal); border-color: var(--brand-teal); }
.gm-kicker { color: var(--brand-teal) !important; }
.gm-title { color: var(--brand-navy) !important; }
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)
_logo = logo_data_uri()
_brand = (f"<img src='{_logo}' alt='GovMove'>" if _logo else
          "<span class='gm-wordmark'>Gov<span style='color:var(--brand-teal)'>Move</span></span>")
st.markdown(f"<div class='gm-header'>{_brand}<span class='gm-sep'></span><strong>Government Estate Intelligence</strong>"
            "<span class='gm-status'>6 of 6 source datasets connected</span></div>", unsafe_allow_html=True)

try:
    import data_access as da
    da.get_spark()
    import estate_views as ev
    import planning_views as pv
except Exception as exc:  # connection or import failure: explain rather than crash
    st.error("GovMove could not connect to Databricks compute or load its modules. Check that the app's service principal "
             f"can use the configured compute and read the GovMove tables.\n\nDetails: {exc}")
    st.stop()

MODES = {"estate": "Estate Intelligence — explore and understand the estate",
         "planning": "Estate Planning & Decision Support — investigate capacity and planning options"}
mode = st.radio("Primary product navigation", list(MODES), format_func=MODES.get, horizontal=True,
                key="mode", label_visibility="collapsed")


def run(view):
    try:
        view()
    except Exception as exc:
        st.error(f"This view could not be loaded. Details: {exc}")


if mode == "estate":
    with st.sidebar:
        st.markdown("<div class='gm-kicker'>ESTATE INTELLIGENCE</div>", unsafe_allow_html=True)
        view_name = st.radio("View", list(ev.VIEWS), key="estate_view", label_visibility="collapsed")
        st.divider()
        st.caption("Records are joined only on exact Insite Property Reference. Values are shown as recorded in source.")
    run(ev.VIEWS[view_name])
else:
    task = st.session_state.get("task")
    with st.sidebar:
        st.markdown("<div class='gm-kicker'>PLANNING TASKS</div>", unsafe_allow_html=True)
        if st.button("All tasks", use_container_width=True, type="primary" if not task else "secondary"):
            st.session_state["task"] = None
            st.rerun()
        for tid, title, _ in pv.TASKS:
            if st.button(title, key=f"side_{tid}", use_container_width=True,
                         type="primary" if task == tid else "secondary"):
                st.session_state["task"] = tid
                st.rerun()
    run(pv.TASK_VIEWS[task] if task in pv.TASK_VIEWS else pv.render_landing)
