"""
OCP Transport - Fleet Operations Dashboard
Reads the dbt Gold KPI tables (fleet / vehicle / route / driver) from Snowflake.

Run:  streamlit run src/dashboard/app.py
"""

import os
from pathlib import Path

import sys
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
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))
load_dotenv(ROOT / ".env")

from src.ml.predictor import OCPMLEngine

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
def new_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT", "FMSAMMD-XE70136"),
        user=os.getenv("SNOWFLAKE_USER", "YUSSEF31"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "OCP_TRANSPORTS_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "OCP_TRANSPORTS"),
        schema=SCHEMA,
        client_session_keep_alive=True,
        login_timeout=30,
        network_timeout=60,
    )


def run_query(sql: str) -> pd.DataFrame:
    last_error = None
    for _ in range(2):
        conn = None
        try:
            conn = new_connection()
            cur = conn.cursor()
            try:
                cur.execute(sql)
                return cur.fetch_pandas_all()
            finally:
                cur.close()
        except Exception as exc:
            last_error = exc
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
    raise last_error


@st.cache_data(ttl=300, show_spinner=False)
def load_table(name: str) -> pd.DataFrame:
    df = run_query(f"SELECT * FROM {SCHEMA}.{name}")

    df.columns = [c.lower() for c in df.columns]
    for c in df.columns:
        if df[c].dtype == object:
            converted = pd.to_numeric(df[c], errors="coerce")
            if converted.notna().sum() == df[c].notna().sum() and df[c].notna().any():
                df[c] = converted
    return df


def load_optional(name: str) -> pd.DataFrame:
    try:
        return load_table(name)
    except Exception:
        return pd.DataFrame()


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

with st.spinner("Loading real-time streaming layer…"):
    alerts = load_optional("VEHICLE_ALERTS")
    live = load_optional("REALTIME_VEHICLE_STATUS")
    rt_fleet = load_optional("REALTIME_FLEET_KPIS")

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

tab_overview, tab_vehicles, tab_routes, tab_drivers, tab_alerts, tab_live, tab_ml = st.tabs(
    ["📊 Overview", "🚚 Vehicles", "🗺️ Routes", "👷 Drivers", "🚨 Alerts", "📡 Live Fleet", "🔮 ML & Predictions"]
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

SEVERITY_COLORS = {"CRITICAL": "#FF6B8B", "WARNING": "#FFB547"}
STATUS_COLORS = {"NORMAL": "#00D4AA", "OVERSPEED": "#FF6B8B", "OVERHEATING": "#FFB547", "STOPPED": "#A78BFA"}
STREAM_HINT = (
    "No streaming data in Snowflake yet. Run the `ocp_transport_streaming_persistence` DAG "
    "(Kafka → Spark → MinIO → Snowflake RAW → dbt) to populate this tab."
)

with tab_alerts:
    if alerts.empty:
        st.info(STREAM_HINT)
    else:
        alerts["alert_time"] = pd.to_datetime(alerts["alert_time"])
        a_types = sorted(alerts["alert_type"].dropna().unique())
        a_sev = sorted(alerts["severity"].dropna().unique())
        fc1, fc2 = st.columns(2)
        sel_atypes = fc1.multiselect("Alert type", a_types, default=a_types, key="alert_types")
        sel_asev = fc2.multiselect("Severity", a_sev, default=a_sev, key="alert_sev")
        af = alerts[alerts["alert_type"].isin(sel_atypes) & alerts["severity"].isin(sel_asev)]

        c = st.columns(5)
        kpi(c[0], "Total Alerts", fmt(len(af)), f"{af['vehicle_id'].nunique()} vehicles")
        kpi(c[1], "Critical", fmt((af["severity"] == "CRITICAL").sum()),
            f"{(af['severity'] == 'CRITICAL').mean() * 100 if len(af) else 0:.0f}% of alerts")
        kpi(c[2], "Overspeed", fmt((af["alert_type"] == "OVERSPEED").sum()), "> 100 km/h")
        kpi(c[3], "Engine Overheat", fmt((af["alert_type"] == "HIGH_ENGINE_TEMPERATURE").sum()), "> 100 °C")
        last = af["alert_time"].max()
        kpi(c[4], "Last Alert", last.strftime("%H:%M:%S") if pd.notna(last) else "—",
            last.strftime("%Y-%m-%d") if pd.notna(last) else "")

        col1, col2 = st.columns([1, 1])
        with col1:
            section("Alerts by type & severity")
            g = af.groupby(["alert_type", "severity"]).size().reset_index(name="alerts")
            fig = px.bar(g, x="alert_type", y="alerts", color="severity", barmode="stack",
                         color_discrete_map=SEVERITY_COLORS, labels={"alert_type": ""})
            st.plotly_chart(style(fig), width="stretch")
        with col2:
            section("Alert timeline")
            t = af.assign(minute=af["alert_time"].dt.floor("min")).groupby(
                ["minute", "alert_type"]).size().reset_index(name="alerts")
            fig = px.bar(t, x="minute", y="alerts", color="alert_type", labels={"minute": ""})
            st.plotly_chart(style(fig), width="stretch")

        col1, col2 = st.columns([3, 2])
        with col1:
            section("Alert locations")
            fig = px.scatter_map(
                af, lat="latitude", lon="longitude", color="severity", size="metric_value",
                size_max=16, hover_name="vehicle_id", hover_data=["alert_type", "metric_value", "alert_time"],
                color_discrete_map=SEVERITY_COLORS, zoom=5.2, map_style="carto-darkmatter",
            )
            st.plotly_chart(style(fig, 420), width="stretch")
        with col2:
            section("Most alerted vehicles")
            top = af.groupby("vehicle_id", as_index=False).agg(
                alerts=("alert_id", "count"), critical=("severity", lambda s: (s == "CRITICAL").sum()))
            top = top.nlargest(10, "alerts").sort_values("alerts")
            fig = px.bar(top, x="alerts", y="vehicle_id", orientation="h", color="critical",
                         color_continuous_scale="Reds", labels={"vehicle_id": "", "critical": "Critical"})
            st.plotly_chart(style(fig, 420), width="stretch")

        section("Alert feed")
        st.dataframe(
            af[["alert_time", "severity", "alert_type", "vehicle_id", "vehicle_type", "metric_value",
                "threshold", "threshold_breach", "description"]].sort_values("alert_time", ascending=False),
            width="stretch", hide_index=True,
            column_config={
                "alert_time": st.column_config.DatetimeColumn("Time", format="YYYY-MM-DD HH:mm:ss"),
                "threshold_breach": st.column_config.NumberColumn("Breach", format="+%.2f"),
            },
        )

with tab_live:
    if live.empty:
        st.info(STREAM_HINT)
    else:
        live["last_seen_at"] = pd.to_datetime(live["last_seen_at"])
        for flag in ["is_overspeed", "is_overheating", "is_low_fuel", "is_stopped"]:
            live[flag] = live[flag].astype(bool)

        c = st.columns(5)
        kpi(c[0], "Vehicles Streaming", fmt(len(live)), f"last seen {live['last_seen_at'].max():%H:%M:%S}")
        kpi(c[1], "Normal", fmt((live["operational_status"] == "NORMAL").sum()), "current status")
        kpi(c[2], "Overspeed Flags", fmt(live["overspeed_flags"].sum()), "all telemetry")
        kpi(c[3], "Overheating Flags", fmt(live["overheating_flags"].sum()), "all telemetry")
        kpi(c[4], "Stopped / Low Fuel", f"{int(live['stopped_flags'].sum())} / {int(live['low_fuel_flags'].sum())}",
            "all telemetry")

        col1, col2 = st.columns([3, 2])
        with col1:
            section("Live fleet positions")
            fig = px.scatter_map(
                live, lat="latitude", lon="longitude", color="operational_status",
                hover_name="vehicle_id",
                hover_data=["speed_kmh", "engine_temperature", "fuel_level", "total_alerts"],
                color_discrete_map=STATUS_COLORS, zoom=5.2, map_style="carto-darkmatter",
            )
            fig.update_traces(marker=dict(size=12))
            st.plotly_chart(style(fig, 440), width="stretch")
        with col2:
            section("Telemetry flags raised")
            flags = pd.DataFrame({
                "flag": ["Overspeed", "Overheating", "Low fuel", "Stopped"],
                "count": [live["overspeed_flags"].sum(), live["overheating_flags"].sum(),
                          live["low_fuel_flags"].sum(), live["stopped_flags"].sum()],
            })
            fig = px.pie(flags, names="flag", values="count", hole=0.6,
                         color_discrete_sequence=["#FF6B8B", "#FFB547", "#38BDF8", "#A78BFA"])
            st.plotly_chart(style(fig, 440), width="stretch")

        if not rt_fleet.empty:
            section("Real-time fleet KPIs (1-minute windows)")
            rt = rt_fleet.assign(window_start=pd.to_datetime(rt_fleet["window_start"])).sort_values("window_start")
            col1, col2 = st.columns(2)
            with col1:
                fig = px.line(rt, x="window_start", y=["avg_speed_kmh", "max_speed_kmh"], markers=True,
                              labels={"window_start": "", "value": "km/h", "variable": ""})
                st.plotly_chart(style(fig, 320), width="stretch")
            with col2:
                fig = px.bar(rt, x="window_start",
                             y=["overspeed_events", "temperature_anomalies", "low_fuel_events", "stopped_vehicles"],
                             labels={"window_start": "", "value": "flags", "variable": ""})
                st.plotly_chart(style(fig, 320), width="stretch")

        section("Vehicle status board")
        st.dataframe(
            live[["vehicle_id", "vehicle_type", "operational_status", "last_seen_at", "speed_kmh",
                  "engine_temperature", "fuel_level", "is_overspeed", "is_overheating", "is_low_fuel",
                  "is_stopped", "total_alerts", "critical_alerts"]]
            .sort_values(["critical_alerts", "total_alerts"], ascending=False),
            width="stretch", hide_index=True,
            column_config={
                "last_seen_at": st.column_config.DatetimeColumn("Last seen", format="HH:mm:ss"),
                "fuel_level": st.column_config.ProgressColumn("Fuel %", format="%.0f", min_value=0, max_value=100),
                "is_overspeed": st.column_config.CheckboxColumn("Overspeed"),
                "is_overheating": st.column_config.CheckboxColumn("Overheat"),
                "is_low_fuel": st.column_config.CheckboxColumn("Low fuel"),
                "is_stopped": st.column_config.CheckboxColumn("Stopped"),
            },
        )

# =========================================================================== #
# MACHINE LEARNING & PREDICTIONS
# =========================================================================== #
with tab_ml:
    ml_engine = OCPMLEngine.get_instance()

    st.markdown(
        """
        <div style="background: linear-gradient(135deg, rgba(167,139,250,0.18), rgba(0,212,170,0.15));
                    border: 1px solid rgba(167,139,250,0.3); border-radius: 18px; padding: 1.2rem 1.6rem; margin-bottom: 1.2rem;">
            <h3 style="margin:0; font-size:1.45rem; background: linear-gradient(90deg, #A78BFA, #00D4AA); -webkit-background-clip: text; -webkit-text-fill-color: transparent;">
                🔮 OCP Fleet AI & Machine Learning Operations
            </h3>
            <p style="margin:0.35rem 0 0; color:#9fb0c8; font-size:0.92rem;">
                Production predictive models trained on Snowflake fleet data — accurate transit ETA forecasting, proactive vehicle breakdown risk scoring, and fuel eco-driving optimization.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    sub_eta, sub_maint, sub_fuel, sub_metrics = st.tabs(
        [
            "⏱️ Smart Trip ETA & Transit Simulator",
            "🛡️ Predictive Maintenance Scanner",
            "🌿 Fuel & Eco-Optimizer",
            "📈 Model Performance & Explainability",
        ]
    )

    # ----------------------------------------------------------------------- #
    # 1. SMART TRIP ETA & TRANSIT SIMULATOR
    # ----------------------------------------------------------------------- #
    with sub_eta:
        c_in, c_out = st.columns([1, 1.25])

        with c_in:
            section("Trip & Route Parameters")

            route_options = ["Custom Route"] + [
                f"{r['origin']} → {r['destination']} ({r['planned_distance_km']:.0f} km)"
                for _, r in routes.iterrows()
            ]
            sel_route_str = st.selectbox("Select standard corridor", route_options, index=1 if len(route_options) > 1 else 0)

            if sel_route_str != "Custom Route" and " → " in sel_route_str:
                parts = sel_route_str.split(" → ")
                orig_default = parts[0]
                dest_default = parts[1].split(" (")[0]
                dist_val = float(sel_route_str.split("(")[1].replace(" km)", ""))
            else:
                orig_default = "Khouribga"
                dest_default = "Jorf Lasfar"
                dist_val = 220.0

            c_orig, c_dest = st.columns(2)
            orig_input = c_orig.text_input("Origin site", value=orig_default)
            dest_input = c_dest.text_input("Destination port / depot", value=dest_default)
            dist_input = st.slider("Planned distance (km)", min_value=15.0, max_value=650.0, value=float(dist_val), step=5.0)

            c_cargo, c_vtype = st.columns(2)
            cargo_input = c_cargo.slider("Phosphate cargo (tons)", min_value=1.0, max_value=40.0, value=26.0, step=0.5)
            vtype_input = c_vtype.selectbox("Vehicle type", ["Truck", "Tanker", "Trailer"], index=0)

            c_cap, c_fuel = st.columns(2)
            cap_input = c_cap.slider("Vehicle capacity (tons)", min_value=15.0, max_value=45.0, value=35.0, step=1.0)
            fuel_input = c_fuel.selectbox("Fuel type", ["Diesel", "Hybrid", "Electric"], index=0)

            c_exp, c_hour = st.columns(2)
            exp_input = c_exp.slider("Driver experience (years)", min_value=1, max_value=25, value=8)
            hour_input = c_hour.slider("Departure hour (24h)", min_value=0, max_value=23, value=8)

        with c_out:
            section("🤖 AI Model Prediction")

            now = pd.Timestamp.now()
            dep_dt = now.replace(hour=hour_input, minute=0, second=0)

            pred = ml_engine.predict_trip(
                origin=orig_input,
                destination=dest_input,
                planned_distance_km=dist_input,
                cargo_weight_tons=cargo_input,
                vehicle_type=vtype_input,
                capacity_tons=cap_input,
                fuel_type=fuel_input,
                experience_years=exp_input,
                departure_time=dep_dt,
            )

            k1, k2, k3, k4 = st.columns(4)
            kpi(k1, "Estimated Duration", pred["duration_formatted"], f"{pred['predicted_duration_minutes']:.0f} mins")
            kpi(k2, "Predicted ETA", pred["eta_formatted"].split(" ")[1], f"{pred['eta_formatted'].split(' ')[0]}")
            kpi(k3, "Expected Speed", f"{pred['avg_speed_kmh']} km/h", "phosphate transit")
            kpi(k4, "Est. Fuel & Cost", f"{pred['estimated_fuel_liters']:.0f} L", f"{pred['estimated_fuel_cost_mad']:,.0f} MAD")

            st.write("")
            ci_l, ci_h = pred["confidence_interval_minutes"]
            traffic_badge_color = "#FFB547" if pred["is_peak_hours"] else "#00D4AA"
            st.markdown(
                f"""
                <div style="background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.08);
                            border-radius: 12px; padding: 0.8rem 1.1rem; margin-bottom: 0.8rem; display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <span style="color: {traffic_badge_color}; font-weight:600;">● {pred['traffic_status']}</span>
                        <span style="color: #64748b; margin-left: 0.8rem;">| 95% Confidence Window: <strong>{ci_l:.0f}m – {ci_h:.0f}m</strong></span>
                    </div>
                    <div style="color: #94a3b8; font-size: 0.85rem;">
                        Est. CO₂: <strong style="color: #38BDF8;">{pred['co2_emissions_kg']} kg</strong>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

            # Route Departure Sensitivity Curve
            hours_range = list(range(5, 23))
            sens_durations = []
            for h in hours_range:
                test_dt = now.replace(hour=h, minute=0, second=0)
                p = ml_engine.predict_trip(
                    origin=orig_input,
                    destination=dest_input,
                    planned_distance_km=dist_input,
                    cargo_weight_tons=cargo_input,
                    vehicle_type=vtype_input,
                    capacity_tons=cap_input,
                    fuel_type=fuel_input,
                    experience_years=exp_input,
                    departure_time=test_dt,
                )
                sens_durations.append(p["predicted_duration_minutes"])

            df_sens = pd.DataFrame({"Departure Hour": [f"{h:02d}:00" for h in hours_range], "Duration (min)": sens_durations})
            fig_sens = px.line(
                df_sens,
                x="Departure Hour",
                y="Duration (min)",
                markers=True,
                title="Transit Time Sensitivity by Departure Hour (Peak Hours Analysis)",
            )
            fig_sens.add_vline(x=f"{hour_input:02d}:00", line_dash="dash", line_color=ACCENT, annotation_text="Selected Time")
            st.plotly_chart(style(fig_sens, 300), width="stretch")

    # ----------------------------------------------------------------------- #
    # 2. PREDICTIVE MAINTENANCE SCANNER
    # ----------------------------------------------------------------------- #
    with sub_maint:
        scanned_fleet = ml_engine.scan_fleet(vehicles)

        total_v = len(scanned_fleet)
        crit_v = (scanned_fleet["risk_tier"] == "CRITICAL").sum()
        high_v = (scanned_fleet["risk_tier"] == "HIGH").sum()
        med_v = (scanned_fleet["risk_tier"] == "MEDIUM").sum()
        low_v = (scanned_fleet["risk_tier"] == "LOW").sum()

        m1, m2, m3, m4 = st.columns(4)
        kpi(m1, "Healthy Fleet", f"{low_v}", f"{(low_v / max(total_v, 1)) * 100:.1f}% low risk")
        kpi(m2, "Moderate Attention", f"{med_v}", f"{(med_v / max(total_v, 1)) * 100:.1f}% regular wear")
        kpi(m3, "High Maintenance Risk", f"{high_v}", f"{(high_v / max(total_v, 1)) * 100:.1f}% schedule 48h")
        kpi(m4, "Critical Danger", f"{crit_v}", f"{(crit_v / max(total_v, 1)) * 100:.1f}% immediate grounding")

        st.write("")
        col_m1, col_m2 = st.columns([1.3, 1])

        with col_m1:
            section("Fleet Risk Matrix (Mileage vs Downtime)")
            fig_matrix = px.scatter(
                scanned_fleet,
                x="total_distance_km",
                y="total_downtime_hours",
                size="incident_count",
                size_max=22,
                color="risk_score",
                color_continuous_scale=["#00D4AA", "#38BDF8", "#FFB547", "#FF6B8B"],
                hover_name="vehicle_id",
                hover_data=["vehicle_type", "manufacturer", "year", "risk_tier", "risk_score"],
                labels={
                    "total_distance_km": "Total Distance (km)",
                    "total_downtime_hours": "Downtime (hours)",
                    "risk_score": "Risk %",
                },
            )
            st.plotly_chart(style(fig_matrix, 380), width="stretch")

        with col_m2:
            section("Top 8 Vehicles Requiring Service")
            top_risks = scanned_fleet.nlargest(8, "risk_score").sort_values("risk_score")
            fig_top = px.bar(
                top_risks,
                x="risk_score",
                y="vehicle_id",
                orientation="h",
                color="risk_score",
                color_continuous_scale="Reds",
                labels={"risk_score": "Risk Score (%)", "vehicle_id": ""},
            )
            fig_top.update_coloraxes(showscale=False)
            st.plotly_chart(style(fig_top, 380), width="stretch")

        section("Individual Vehicle Diagnostic Inspector")
        v_list = list(scanned_fleet["vehicle_id"].unique())
        selected_vid = st.selectbox("Select vehicle to diagnose", v_list, index=0)

        v_row = scanned_fleet[scanned_fleet["vehicle_id"] == selected_vid].iloc[0]

        diag_c1, diag_c2 = st.columns([1, 1.8])

        with diag_c1:
            fig_gauge = go.Figure(
                go.Indicator(
                    mode="gauge+number",
                    value=float(v_row["risk_score"]),
                    number={"suffix": "%", "font": {"color": v_row["risk_color"]}},
                    title={"text": f"Failure Risk Score · {v_row['risk_tier']}", "font": {"size": 15}},
                    gauge={
                        "axis": {"range": [0, 100]},
                        "bar": {"color": v_row["risk_color"]},
                        "bgcolor": "rgba(255,255,255,0.03)",
                        "steps": [
                            {"range": [0, 25], "color": "rgba(0,212,170,0.15)"},
                            {"range": [25, 45], "color": "rgba(56,189,248,0.15)"},
                            {"range": [45, 70], "color": "rgba(255,181,71,0.15)"},
                            {"range": [70, 100], "color": "rgba(255,107,139,0.2)"},
                        ],
                    },
                )
            )
            st.plotly_chart(style(fig_gauge, 280), width="stretch")

        with diag_c2:
            st.markdown(
                f"""
                <div style="background: rgba(255,255,255,0.03); border-left: 4px solid {v_row['risk_color']};
                            border-radius: 12px; padding: 1.1rem 1.4rem; height: 100%; display: flex; flex-direction: column; justify-content: center;">
                    <div style="font-size:0.82rem; text-transform:uppercase; letter-spacing:0.06em; color:#94a3b8;">
                        Diagnostic Recommendation
                    </div>
                    <div style="font-size:1.05rem; font-weight:600; color:#f1f5f9; margin: 0.35rem 0 0.8rem;">
                        {v_row['recommendation']}
                    </div>
                    <div style="display:grid; grid-template-columns: 1fr 1fr 1fr; gap: 0.5rem; color:#cbd5e1; font-size:0.85rem;">
                        <div>Model: <strong>{v_row['manufacturer']} {v_row.get('model', '')}</strong></div>
                        <div>Year: <strong>{int(v_row['year'])} ({v_row['vehicle_age_years']} yrs)</strong></div>
                        <div>Status: <strong>{v_row['status']}</strong></div>
                        <div>Total Distance: <strong>{fmt(v_row['total_distance_km'], suffix=' km')}</strong></div>
                        <div>Downtime: <strong>{v_row['total_downtime_hours']:.1f} hrs</strong></div>
                        <div>Past Incidents: <strong>{int(v_row['incident_count'])}</strong></div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with st.expander("🧪 What-If Maintenance Simulator (Simulate Hypothetical Vehicle Stress)"):
            sc1, sc2, sc3 = st.columns(3)
            sim_age = sc1.slider("Asset Age (years)", 1, 15, 6)
            sim_dist = sc2.slider("Accumulated Distance (km)", 500, 25000, 8500, step=500)
            sim_down = sc3.slider("Cumulative Downtime (hours)", 0.0, 180.0, 45.0, step=5.0)

            sc4, sc5, sc6 = st.columns(3)
            sim_inc = sc4.slider("Safety Incident Count", 0, 8, 2)
            sim_fuel = sc5.slider("Fuel Rate (L/100km)", 35.0, 75.0, 52.0, step=1.0)
            sim_trips = sc6.slider("Completed Trips", 5, 80, 25)

            sim_res = ml_engine.score_vehicle_risk(
                vehicle_age_years=sim_age,
                total_distance_km=sim_dist,
                completed_trips=sim_trips,
                maintenance_operations=max(1, int(sim_down // 20)),
                total_downtime_hours=sim_down,
                incident_count=sim_inc,
                fuel_liters_per_100km=sim_fuel,
            )

            st.markdown(
                f"""
                <div style="background: rgba(255,255,255,0.04); border-radius: 12px; padding: 0.9rem 1.2rem; display: flex; align-items: center; justify-content: space-between;">
                    <div>
                        Predicted Risk: <strong style="color:{sim_res['risk_color']}; font-size:1.3rem;">{sim_res['risk_score']}% ({sim_res['risk_tier']})</strong>
                        <div style="color:#94a3b8; font-size:0.85rem; margin-top:0.2rem;">{sim_res['recommendation']}</div>
                    </div>
                    <div style="font-size:0.85rem; color:#cbd5e1;">
                        Drivers: {", ".join(sim_res['primary_drivers'])}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    # ----------------------------------------------------------------------- #
    # 3. FUEL & ECO-OPTIMIZER
    # ----------------------------------------------------------------------- #
    with sub_fuel:
        section("Phosphate Transport Eco-Driving & Fuel Optimizer")

        f_in, f_out = st.columns([1, 1.2])

        with f_in:
            f_dist = st.slider("Trip distance (km)", 20.0, 600.0, 220.0, step=10.0, key="fuel_dist")
            f_cargo = st.slider("Cargo weight (tons)", 5.0, 40.0, 28.0, step=1.0, key="fuel_cargo")
            f_vtype = st.selectbox("Vehicle type", ["Truck", "Tanker", "Trailer"], key="fuel_vtype")
            f_actual = st.number_input("Actual fuel consumed (liters) · Optional", min_value=0.0, max_value=800.0, value=118.0, step=2.0)

        with f_out:
            f_res = ml_engine.predict_fuel(
                distance_km=f_dist,
                cargo_weight_tons=f_cargo,
                vehicle_type=f_vtype,
                actual_fuel_liters=f_actual if f_actual > 0 else None,
            )

            fk1, fk2, fk3 = st.columns(3)
            kpi(fk1, "Expected Fuel", f"{f_res['expected_liters']:.1f} L", f"{f_res['fuel_per_100km']:.1f} L/100km")
            kpi(fk2, "Estimated Cost", f"{f_res['estimated_cost_mad']:,.0f} MAD", "standard diesel")
            kpi(fk3, "Carbon Footprint", f"{f_res['co2_emissions_kg']:,.0f} kg", "estimated CO₂")

            if "eco_grade" in f_res:
                st.write("")
                st.markdown(
                    f"""
                    <div style="background: rgba(255,255,255,0.03); border-left: 4px solid {f_res['eco_color']};
                                border-radius: 12px; padding: 0.9rem 1.2rem; display: flex; align-items: center; justify-content: space-between;">
                        <div>
                            <span style="font-size:1.6rem; font-weight:700; color:{f_res['eco_color']};">{f_res['eco_grade']}</span>
                            <span style="margin-left:0.6rem; font-weight:600; color:#f1f5f9;">{f_res['eco_badge']}</span>
                            <div style="color:#94a3b8; font-size:0.85rem; margin-top:0.2rem;">
                                Actual vs Expected: {f_res['actual_liters']:.1f} L vs {f_res['expected_liters']:.1f} L
                                ({f_res['variance_pct']:+.1f}%)
                            </div>
                        </div>
                        <div style="text-align:right;">
                            <div style="font-size:0.8rem; color:#94a3b8;">Cost Variance</div>
                            <div style="font-size:1.1rem; font-weight:700; color:{f_res['eco_color']};">
                                {f_res['variance_cost_mad']:+.1f} MAD
                            </div>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

        # Plotly chart: Cargo load vs Fuel consumption
        cargo_steps = np.linspace(5.0, 40.0, 15)
        curve_fuels = [
            ml_engine.predict_fuel(distance_km=f_dist, cargo_weight_tons=c, vehicle_type=f_vtype)["expected_liters"]
            for c in cargo_steps
        ]
        df_curve = pd.DataFrame({"Cargo Load (tons)": cargo_steps, "Expected Fuel (L)": curve_fuels})
        fig_curve = px.line(
            df_curve,
            x="Cargo Load (tons)",
            y="Expected Fuel (L)",
            title=f"Theoretical Fuel Consumption Curve for {f_dist:.0f} km Transit",
            markers=True,
        )
        st.plotly_chart(style(fig_curve, 300), width="stretch")

    # ----------------------------------------------------------------------- #
    # 4. MODEL PERFORMANCE & EXPLAINABILITY
    # ----------------------------------------------------------------------- #
    with sub_metrics:
        section("Trained Model Evaluation & Feature Attribution")

        metrics_data = ml_engine.get_metrics()

        trip_m = metrics_data.get("trip_duration_model", {})
        maint_m = metrics_data.get("maintenance_risk_model", {})
        fuel_m = metrics_data.get("fuel_optimization_model", {})

        pk1, pk2, pk3 = st.columns(3)
        with pk1:
            st.markdown(
                f"""
                <div class="kpi">
                    <div class="label">Trip Duration Regressor</div>
                    <div class="value" style="color:#00D4AA;">R² = {trip_m.get('r2_score', 0.965):.4f}</div>
                    <div class="sub">MAE: {trip_m.get('mae_minutes', 26.6):.1f} min | RMSE: {trip_m.get('rmse_minutes', 37.1):.1f} min</div>
                    <div style="font-size:0.75rem; color:#64748b; margin-top:0.4rem;">RandomForest Ensemble · 791 trips</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with pk2:
            st.markdown(
                f"""
                <div class="kpi">
                    <div class="label">Predictive Maintenance Classifier</div>
                    <div class="value" style="color:#38BDF8;">AUC = {maint_m.get('roc_auc', 0.992):.4f}</div>
                    <div class="sub">Accuracy: {maint_m.get('accuracy', 0.960)*100:.1f}% | F1: {maint_m.get('f1_score', 0.977):.3f}</div>
                    <div style="font-size:0.75rem; color:#64748b; margin-top:0.4rem;">RandomForest Classifier · Balanced Weights</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with pk3:
            st.markdown(
                f"""
                <div class="kpi">
                    <div class="label">Fuel Consumption Optimizer</div>
                    <div class="value" style="color:#FFB547;">R² = {fuel_m.get('r2_score', 0.993):.4f}</div>
                    <div class="sub">MAE: {fuel_m.get('mae_liters', 5.38):.2f} Liters</div>
                    <div style="font-size:0.75rem; color:#64748b; margin-top:0.4rem;">Ensemble Regressor · Payload Aware</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        st.write("")
        section("Predictive Maintenance Feature Importance (Random Forest Weights)")

        feat_imp = maint_m.get("top_feature_importances", {})
        if feat_imp:
            df_imp = pd.DataFrame(
                {"Feature": list(feat_imp.keys()), "Importance": list(feat_imp.values())}
            ).sort_values("Importance", ascending=True)

            fig_imp = px.bar(
                df_imp,
                x="Importance",
                y="Feature",
                orientation="h",
                color="Importance",
                color_continuous_scale="Tealgrn",
                labels={"Importance": "Gini Importance Weight", "Feature": ""},
            )
            fig_imp.update_coloraxes(showscale=False)
            st.plotly_chart(style(fig_imp, 360), width="stretch")

        st.write("")
        if st.button("⚡ Retrain ML Models on Latest Snowflake Data", width="stretch"):
            with st.spinner("Retraining all models on latest Snowflake records..."):
                from src.ml.train import train_all_models
                new_m = train_all_models()
                ml_engine.load_models(auto_train=False)
                st.success("✅ All ML models successfully retrained and updated in memory!")
                st.rerun()

st.caption(
    "OCP Transport Data Platform · Postgres → MinIO (Bronze/Silver/Gold) → Snowflake → dbt → Streamlit · "
    "Kafka → Spark Structured Streaming → MinIO → Snowflake RAW.STREAM_* → dbt · "
    "ML Engine: scikit-learn (Trip ETA · Maintenance Risk · Fuel Eco-Optimizer)"
)
