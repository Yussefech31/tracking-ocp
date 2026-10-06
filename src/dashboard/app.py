"""
OCP Transport - Fleet Operations Dashboard
Reads the dbt Gold KPI tables (fleet / vehicle / route / driver) from Snowflake.

Run:  streamlit run src/dashboard/app.py
"""

import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import snowflake.connector
import streamlit as st
from dotenv import load_dotenv

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

SCHEMA = os.getenv("DASHBOARD_SCHEMA", "ANALYTICS")

PALETTE = ["#00D4AA", "#4F8BFF", "#FFB547", "#FF6B8B", "#A78BFA", "#38BDF8"]
ACCENT = "#00D4AA"

st.set_page_config(
    page_title="OCP Transport | Fleet Operations Dashboard",
    page_icon="🚛",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --------------------------------------------------------------------------- #
# Styling
# --------------------------------------------------------------------------- #
st.markdown(
    """
<style>
@import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700&display=swap');

html, body, [class*="css"], .stMarkdown, .stText { font-family: 'Outfit', sans-serif; }

.stApp {
    background: radial-gradient(circle at 10% 0%, #0f2a3a 0%, #0a0f1c 45%, #070a12 100%);
    color: #e6edf6;
}
section[data-testid="stSidebar"] {
    background: rgba(12, 18, 32, 0.85);
    border-right: 1px solid rgba(255,255,255,0.06);
}
.block-container { padding-top: 1.6rem; }

.hero {
    padding: 1.4rem 1.8rem;
    border-radius: 20px;
    background: linear-gradient(120deg, rgba(0,212,170,0.18), rgba(79,139,255,0.14));
    border: 1px solid rgba(255,255,255,0.08);
    margin-bottom: 1.4rem;
    animation: fadeIn .6s ease-out;
}
.hero h1 { margin: 0; font-weight: 700; font-size: 2.1rem;
    background: linear-gradient(90deg, #00D4AA, #4F8BFF);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
.hero p { margin: .3rem 0 0; color: #9fb0c8; }

.kpi {
    background: rgba(255,255,255,0.04);
    border: 1px solid rgba(255,255,255,0.07);
    backdrop-filter: blur(12px);
    border-radius: 16px;
    padding: 1rem 1.2rem;
    transition: transform .2s ease, border-color .2s ease, box-shadow .2s ease;
    animation: fadeIn .5s ease-out;
    height: 100%;
}
.kpi:hover { transform: translateY(-3px); border-color: rgba(0,212,170,.5);
    box-shadow: 0 8px 28px rgba(0,212,170,.15); }
.kpi .label { font-size: .78rem; text-transform: uppercase; letter-spacing: .08em; color: #8fa1ba; }
.kpi .value { font-size: 1.75rem; font-weight: 700; color: #f2f6fb; margin-top: .2rem; }
.kpi .sub   { font-size: .8rem; color: #00D4AA; margin-top: .15rem; }

.section-title { font-size: 1.15rem; font-weight: 600; margin: 1.2rem 0 .4rem; color: #dbe4f0; }

div[data-testid="stTabs"] button { font-family: 'Outfit', sans-serif; font-size: 1rem; }
div[data-testid="stTabs"] button[aria-selected="true"] { color: #00D4AA; }

@keyframes fadeIn { from { opacity: 0; transform: translateY(6px);} to { opacity: 1; transform: none;} }
</style>
""",
    unsafe_allow_html=True,
)


# --------------------------------------------------------------------------- #
# Data access
# --------------------------------------------------------------------------- #
@st.cache_resource(show_spinner=False)
def get_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT", "FMSAMMD-XE70136"),
        user=os.getenv("SNOWFLAKE_USER", "YUSSEF31"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "OCP_TRANSPORTS_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "OCP_TRANSPORTS"),
        schema=SCHEMA,
    )


@st.cache_data(ttl=600, show_spinner=False)
def load_table(name: str) -> pd.DataFrame:
    cur = get_connection().cursor()
    try:
        cur.execute(f"SELECT * FROM {SCHEMA}.{name}")
        df = cur.fetch_pandas_all()
    finally:
        cur.close()
    df.columns = [c.lower() for c in df.columns]
    # Snowflake NUMBER -> Decimal/object; coerce numerics
    for c in df.columns:
        if df[c].dtype == object:
            converted = pd.to_numeric(df[c], errors="coerce")
            if converted.notna().sum() == df[c].notna().sum() and df[c].notna().any():
                df[c] = converted
    return df


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def fmt(n, decimals=0, prefix="", suffix=""):
    if n is None or pd.isna(n):
        return "—"
    n = float(n)
    for unit, div in (("B", 1e9), ("M", 1e6), ("K", 1e3)):
        if abs(n) >= div:
            return f"{prefix}{n / div:,.1f}{unit}{suffix}"
    return f"{prefix}{n:,.{decimals}f}{suffix}"


def kpi(col, label, value, sub=""):
    col.markdown(
        f"""<div class="kpi"><div class="label">{label}</div>
        <div class="value">{value}</div><div class="sub">{sub}</div></div>""",
        unsafe_allow_html=True,
    )


def style(fig, height=380):
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.02)",
        font=dict(family="Outfit, sans-serif", color="#cfd9e6"),
        margin=dict(l=10, r=10, t=40, b=10),
        height=height,
        colorway=PALETTE,
        legend=dict(bgcolor="rgba(0,0,0,0)"),
        hoverlabel=dict(font_family="Outfit"),
    )
    fig.update_xaxes(gridcolor="rgba(255,255,255,0.05)")
    fig.update_yaxes(gridcolor="rgba(255,255,255,0.05)")
    return fig


def section(title):
    st.markdown(f'<div class="section-title">{title}</div>', unsafe_allow_html=True)


# --------------------------------------------------------------------------- #
# Load
# --------------------------------------------------------------------------- #
try:
    with st.spinner("Connecting to Snowflake…"):
        fleet = load_table("FLEET_KPIS")
        vehicles = load_table("VEHICLE_KPIS")
        routes = load_table("ROUTE_KPIS")
        drivers = load_table("DRIVER_KPIS")
except Exception as e:  # noqa: BLE001
    st.error(f"❌ Could not load data from Snowflake schema `{SCHEMA}`.\n\n{e}")
    st.info(
        "Check `SNOWFLAKE_*` variables in `.env`. If your dbt models landed in a "
        "different schema, set `DASHBOARD_SCHEMA` (e.g. `STAGING_ANALYTICS`)."
    )
    st.stop()

# --------------------------------------------------------------------------- #
# Sidebar filters
# --------------------------------------------------------------------------- #
with st.sidebar:
    st.markdown("## 🚛 OCP Transport")
    st.caption(f"Source: `{os.getenv('SNOWFLAKE_DATABASE', 'OCP_TRANSPORTS')}.{SCHEMA}`")
    st.divider()

    v_types = sorted(vehicles["vehicle_type"].dropna().unique())
    sel_types = st.multiselect("Vehicle type", v_types, default=v_types)

    v_status = sorted(vehicles["status"].dropna().unique())
    sel_status = st.multiselect("Vehicle status", v_status, default=v_status)

    r_types = sorted(routes["route_type"].dropna().unique())
    sel_rtypes = st.multiselect("Route type", r_types, default=r_types)

    depts = sorted(drivers["department"].dropna().unique())
    sel_depts = st.multiselect("Driver department", depts, default=depts)

    st.divider()
    if st.button("🔄 Refresh data", width="stretch"):
        st.cache_data.clear()
        st.rerun()

vf = vehicles[vehicles["vehicle_type"].isin(sel_types) & vehicles["status"].isin(sel_status)]
rf = routes[routes["route_type"].isin(sel_rtypes)]
df_ = drivers[drivers["department"].isin(sel_depts)]

# --------------------------------------------------------------------------- #
# Header
# --------------------------------------------------------------------------- #
st.markdown(
    """<div class="hero"><h1>Fleet Operations Dashboard</h1>
    <p>Phosphate transport performance — trips, fuel, maintenance & safety (Gold layer, Snowflake)</p></div>""",
    unsafe_allow_html=True,
)

f = fleet.iloc[0] if not fleet.empty else pd.Series(dtype=float)

tab_overview, tab_vehicles, tab_routes, tab_drivers = st.tabs(
    ["📊 Overview", "🚚 Vehicles", "🗺️ Routes", "👷 Drivers"]
)

# =========================================================================== #
# OVERVIEW
# =========================================================================== #
with tab_overview:
    c = st.columns(5)
    active_pct = (f.get("active_vehicles", 0) / f.get("total_vehicles", 1) * 100) if f.get("total_vehicles") else 0
    comp_pct = (f.get("completed_trips", 0) / f.get("total_trips", 1) * 100) if f.get("total_trips") else 0
    kpi(c[0], "Active Vehicles", f"{int(f.get('active_vehicles', 0))}/{int(f.get('total_vehicles', 0))}",
        f"{active_pct:.1f}% availability")
    kpi(c[1], "Completed Trips", fmt(f.get("completed_trips")), f"{comp_pct:.1f}% completion rate")
    kpi(c[2], "Distance Driven", fmt(f.get("total_distance_km"), suffix=" km"), "completed trips")
    kpi(c[3], "Cargo Moved", fmt(f.get("total_cargo_tons"), suffix=" t"), "phosphate tonnage")
    kpi(c[4], "Incidents", fmt(f.get("total_incidents")), f"{f.get('incidents_per_trip', 0):.3f} per trip")

    st.write("")
    c = st.columns(5)
    kpi(c[0], "Fuel Consumed", fmt(f.get("total_fuel_liters"), suffix=" L"), "all transactions")
    kpi(c[1], "Fuel Efficiency", f"{f.get('fuel_liters_per_100km', 0):.2f}", "L / 100 km")
    kpi(c[2], "Fuel Cost", fmt(f.get("total_fuel_cost"), suffix=" MAD"),
        f"{f.get('fuel_cost_per_km', 0):.2f} MAD / km")
    kpi(c[3], "Maintenance Cost", fmt(f.get("total_maintenance_cost"), suffix=" MAD"),
        f"{f.get('maintenance_cost_per_km', 0):.2f} MAD / km")
    kpi(c[4], "Downtime", fmt(f.get("total_downtime_hours"), suffix=" h"), "maintenance hours")

    col1, col2 = st.columns([1, 1])
    with col1:
        section("Operating cost breakdown")
        cost = pd.DataFrame({
            "type": ["Fuel", "Maintenance"],
            "cost": [f.get("total_fuel_cost", 0), f.get("total_maintenance_cost", 0)],
        })
        fig = px.pie(cost, names="type", values="cost", hole=0.62,
                     color_discrete_sequence=[ACCENT, "#4F8BFF"])
        fig.update_traces(textinfo="percent+label")
        st.plotly_chart(style(fig), width="stretch")
    with col2:
        section("Fleet by vehicle type & status")
        g = vf.groupby(["vehicle_type", "status"]).size().reset_index(name="vehicles")
        fig = px.bar(g, x="vehicle_type", y="vehicles", color="status", barmode="stack")
        st.plotly_chart(style(fig), width="stretch")

    col1, col2 = st.columns([1, 1])
    with col1:
        section("Cargo moved by vehicle type")
        g = vf.groupby("vehicle_type", as_index=False)[["total_cargo_tons", "total_distance_km"]].sum()
        fig = px.bar(g.sort_values("total_cargo_tons"), x="total_cargo_tons", y="vehicle_type",
                     orientation="h", color="total_distance_km", color_continuous_scale="Tealgrn",
                     labels={"total_cargo_tons": "Cargo (t)", "vehicle_type": "",
                             "total_distance_km": "Distance (km)"})
        st.plotly_chart(style(fig), width="stretch")
    with col2:
        section("Fuel efficiency gauge")
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=float(f.get("fuel_liters_per_100km", 0) or 0),
            number={"suffix": " L/100km"},
            gauge={
                "axis": {"range": [0, max(80, float(f.get("fuel_liters_per_100km", 0) or 0) * 1.5)]},
                "bar": {"color": ACCENT},
                "bgcolor": "rgba(255,255,255,0.03)",
                "steps": [
                    {"range": [0, 35], "color": "rgba(0,212,170,0.15)"},
                    {"range": [35, 55], "color": "rgba(255,181,71,0.15)"},
                    {"range": [55, 200], "color": "rgba(255,107,139,0.15)"},
                ],
            },
        ))
        st.plotly_chart(style(fig), width="stretch")

# =========================================================================== #
# VEHICLES
# =========================================================================== #
with tab_vehicles:
    c = st.columns(4)
    kpi(c[0], "Vehicles (filtered)", f"{len(vf)}", f"{(vf['status'] == 'Active').sum()} active")
    kpi(c[1], "Avg Fuel Efficiency", f"{vf.loc[vf['fuel_liters_per_100km'] > 0, 'fuel_liters_per_100km'].mean():.2f}",
        "L / 100 km")
    kpi(c[2], "Avg Maint. Cost / km", f"{vf.loc[vf['maintenance_cost_per_km'] > 0, 'maintenance_cost_per_km'].mean():.2f}",
        "MAD")
    kpi(c[3], "Total Downtime", fmt(vf["total_downtime_hours"].sum(), suffix=" h"), "maintenance")

    col1, col2 = st.columns([3, 2])
    with col1:
        section("Fuel consumption vs distance")
        fig = px.scatter(
            vf, x="total_distance_km", y="total_fuel_liters", color="vehicle_type",
            size="total_cargo_tons", size_max=28, hover_name="vehicle_id",
            hover_data=["manufacturer", "model", "fuel_liters_per_100km"],
            labels={"total_distance_km": "Distance (km)", "total_fuel_liters": "Fuel (L)"},
        )
        st.plotly_chart(style(fig, 420), width="stretch")
    with col2:
        section("Least efficient vehicles (L/100km)")
        top = vf[vf["fuel_liters_per_100km"] > 0].nlargest(10, "fuel_liters_per_100km")
        fig = px.bar(top.sort_values("fuel_liters_per_100km"), x="fuel_liters_per_100km", y="vehicle_id",
                     orientation="h", color="fuel_liters_per_100km", color_continuous_scale="Sunsetdark",
                     labels={"fuel_liters_per_100km": "L/100km", "vehicle_id": ""})
        fig.update_coloraxes(showscale=False)
        st.plotly_chart(style(fig, 420), width="stretch")

    col1, col2 = st.columns(2)
    with col1:
        section("Maintenance cost vs downtime")
        fig = px.scatter(
            vf, x="total_maintenance_cost", y="total_downtime_hours", color="manufacturer",
            size="maintenance_operations", hover_name="vehicle_id",
            labels={"total_maintenance_cost": "Maintenance cost (MAD)", "total_downtime_hours": "Downtime (h)"},
        )
        st.plotly_chart(style(fig), width="stretch")
    with col2:
        section("Incidents per trip by manufacturer")
        g = vf.groupby("manufacturer", as_index=False).agg(
            incidents=("incident_count", "sum"), trips=("completed_trips", "sum"))
        g["incidents_per_trip"] = (g["incidents"] / g["trips"].replace(0, pd.NA)).astype(float).round(3)
        fig = px.bar(g.sort_values("incidents_per_trip", ascending=False), x="manufacturer",
                     y="incidents_per_trip", color="incidents", color_continuous_scale="Reds",
                     labels={"incidents_per_trip": "Incidents / trip", "manufacturer": ""})
        st.plotly_chart(style(fig), width="stretch")

    section("Vehicle detail")
    st.dataframe(
        vf[["vehicle_id", "vehicle_type", "manufacturer", "model", "year", "status", "completed_trips",
            "total_distance_km", "total_cargo_tons", "fuel_liters_per_100km", "fuel_cost_per_km",
            "maintenance_cost_per_km", "total_downtime_hours", "incident_count"]]
        .sort_values("total_distance_km", ascending=False),
        width="stretch", hide_index=True,
        column_config={
            "fuel_liters_per_100km": st.column_config.ProgressColumn(
                "L/100km", format="%.2f", min_value=0,
                max_value=float(vf["fuel_liters_per_100km"].max() or 1)),
            "total_distance_km": st.column_config.NumberColumn("Distance (km)", format="%.0f"),
        },
    )

# =========================================================================== #
# ROUTES
# =========================================================================== #
with tab_routes:
    active_routes = rf[rf["completed_trips"] > 0]
    c = st.columns(4)
    kpi(c[0], "Routes", f"{len(rf)}", f"{len(active_routes)} with trips")
    kpi(c[1], "Avg Trip Duration", f"{active_routes['avg_trip_duration_minutes'].mean():.0f} min", "completed trips")
    kpi(c[2], "Avg Deviation", f"{active_routes['route_deviation_percent'].mean():+.2f}%", "actual vs planned")
    kpi(c[3], "Cargo on Routes", fmt(rf["total_cargo_tons"].sum(), suffix=" t"), "completed trips")

    rf = rf.assign(route=rf["origin"] + " → " + rf["destination"])
    active_routes = rf[rf["completed_trips"] > 0]

    col1, col2 = st.columns([3, 2])
    with col1:
        section("Top routes by cargo volume")
        top = active_routes.nlargest(12, "total_cargo_tons")
        fig = px.bar(top.sort_values("total_cargo_tons"), x="total_cargo_tons", y="route", orientation="h",
                     color="route_type", labels={"total_cargo_tons": "Cargo (t)", "route": ""})
        st.plotly_chart(style(fig, 440), width="stretch")
    with col2:
        section("Route deviation (%)")
        fig = px.histogram(active_routes, x="route_deviation_percent", nbins=25, color="route_type",
                           labels={"route_deviation_percent": "Deviation %"})
        fig.add_vline(x=0, line_dash="dash", line_color="rgba(255,255,255,0.35)")
        st.plotly_chart(style(fig, 440), width="stretch")

    col1, col2 = st.columns(2)
    with col1:
        section("Planned distance vs avg trip duration")
        fig = px.scatter(active_routes, x="planned_distance_km", y="avg_trip_duration_minutes",
                         size="completed_trips", color="route_type", hover_name="route",
                         labels={"planned_distance_km": "Planned distance (km)",
                                 "avg_trip_duration_minutes": "Avg duration (min)"})
        st.plotly_chart(style(fig), width="stretch")
    with col2:
        section("Origin → destination flows")
        flows = active_routes.groupby(["origin", "destination"], as_index=False)["total_cargo_tons"].sum()
        origins = list(flows["origin"].unique())
        dests = [d for d in flows["destination"].unique()]
        labels = [f"⬤ {o}" for o in origins] + [f"◎ {d}" for d in dests]
        src = [origins.index(o) for o in flows["origin"]]
        tgt = [len(origins) + dests.index(d) for d in flows["destination"]]
        fig = go.Figure(go.Sankey(
            node=dict(label=labels, pad=14, thickness=14,
                      color=[ACCENT] * len(origins) + ["#4F8BFF"] * len(dests)),
            link=dict(source=src, target=tgt, value=flows["total_cargo_tons"].astype(float),
                      color="rgba(0,212,170,0.25)"),
        ))
        st.plotly_chart(style(fig), width="stretch")

    section("Route detail")
    st.dataframe(
        rf[["route_id", "route", "route_type", "planned_distance_km", "completed_trips",
            "avg_actual_distance_km", "route_deviation_percent", "avg_trip_duration_minutes", "total_cargo_tons"]]
        .sort_values("completed_trips", ascending=False),
        width="stretch", hide_index=True,
    )

# =========================================================================== #
# DRIVERS
# =========================================================================== #
with tab_drivers:
    d = df_.assign(driver=df_["first_name"] + " " + df_["last_name"])
    d["completion_rate"] = (d["completed_trips"] / d["total_trips"].replace(0, pd.NA)).astype(float) * 100

    c = st.columns(4)
    kpi(c[0], "Drivers", f"{len(d)}", f"{(d['status'] == 'Active').sum()} active")
    kpi(c[1], "Avg Experience", f"{d['experience_years'].mean():.1f} yrs", "all drivers")
    kpi(c[2], "Avg Completion Rate", f"{d['completion_rate'].mean():.1f}%", "completed / assigned")
    kpi(c[3], "Avg Trips / Driver", f"{d['total_trips'].mean():.1f}", "assigned trips")

    col1, col2 = st.columns([3, 2])
    with col1:
        section("🏆 Leaderboard — distance driven")
        top = d.nlargest(15, "total_distance_km")
        fig = px.bar(top.sort_values("total_distance_km"), x="total_distance_km", y="driver", orientation="h",
                     color="completion_rate", color_continuous_scale="Tealgrn",
                     hover_data=["department", "experience_years", "completed_trips"],
                     labels={"total_distance_km": "Distance (km)", "driver": "", "completion_rate": "Completion %"})
        st.plotly_chart(style(fig, 480), width="stretch")
    with col2:
        section("Drivers by department")
        g = d.groupby("department", as_index=False).agg(
            drivers=("driver_id", "count"), cargo=("total_cargo_tons", "sum"))
        fig = px.pie(g, names="department", values="drivers", hole=0.55)
        st.plotly_chart(style(fig, 480), width="stretch")

    col1, col2 = st.columns(2)
    with col1:
        section("Experience vs completion rate")
        fig = px.scatter(d, x="experience_years", y="completion_rate", color="license_type",
                         size="total_cargo_tons", hover_name="driver",
                         labels={"experience_years": "Experience (yrs)", "completion_rate": "Completion %"})
        st.plotly_chart(style(fig), width="stretch")
    with col2:
        section("Avg trip duration by license type")
        g = d.groupby("license_type", as_index=False)["avg_trip_duration_minutes"].mean()
        fig = px.bar(g, x="license_type", y="avg_trip_duration_minutes", color="license_type",
                     labels={"avg_trip_duration_minutes": "Avg duration (min)", "license_type": ""})
        fig.update_layout(showlegend=False)
        st.plotly_chart(style(fig), width="stretch")

    section("Driver detail")
    st.dataframe(
        d[["driver_id", "driver", "department", "license_type", "experience_years", "status", "total_trips",
           "completed_trips", "completion_rate", "total_distance_km", "total_cargo_tons", "avg_trip_duration_minutes"]]
        .sort_values("total_distance_km", ascending=False),
        width="stretch", hide_index=True,
        column_config={
            "completion_rate": st.column_config.ProgressColumn("Completion %", format="%.1f", min_value=0,
                                                               max_value=100),
        },
    )

st.caption("OCP Transport Data Platform · Postgres → MinIO (Bronze/Silver/Gold) → Snowflake → dbt → Streamlit")
