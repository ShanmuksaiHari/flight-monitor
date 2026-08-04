import streamlit as st
import duckdb
import pandas as pd
import json
import os
import time
from datetime import datetime
import anthropic
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────
DB_PATH = "./bronze/flights.duckdb"
INCIDENTS_FILE = "./incidents.jsonl"
LLM_API_KEY = os.getenv("LLM_API_KEY")

st.set_page_config(
    page_title="Flight Monitor — California",
    page_icon="✈️",
    layout="wide"
)

# ─────────────────────────────────────────
# STYLING
# ─────────────────────────────────────────
st.markdown("""
<style>
    .main { background-color: #0a0f1a; }
    .block-container { padding-top: 1rem; }
    .metric-card {
        background: #111827;
        border: 1px solid #1f2d45;
        border-radius: 12px;
        padding: 1rem;
        text-align: center;
    }
    .health-green {
        background: #052e16;
        border: 1px solid #16a34a;
        border-radius: 10px;
        padding: 0.75rem;
        text-align: center;
    }
    .health-red {
        background: #450a0a;
        border: 1px solid #dc2626;
        border-radius: 10px;
        padding: 0.75rem;
        text-align: center;
    }
    .health-label {
        font-size: 13px;
        font-weight: 500;
        margin-bottom: 4px;
    }
    .incident-row {
        background: #111827;
        border-left: 3px solid #dc2626;
        padding: 0.5rem 0.75rem;
        margin-bottom: 6px;
        border-radius: 0 8px 8px 0;
        font-size: 13px;
    }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────
# DATA HELPERS
# ─────────────────────────────────────────
@st.cache_data(ttl=15)
def load_silver_flights():
    """Load latest flight positions from Silver table."""
    try:
        conn = duckdb.connect(DB_PATH, read_only=True)
        df = conn.execute("""
            SELECT icao24, callsign, latitude, longitude,
                   altitude, velocity, heading, on_ground,
                   carrier_code, nearest_airport
            FROM silver_flights
            WHERE latitude IS NOT NULL
              AND longitude IS NOT NULL
            LIMIT 1000
        """).df()
        conn.close()
        return df
    except Exception as e:
        return pd.DataFrame()


@st.cache_data(ttl=15)
def load_top_carriers():
    """Load top carriers from Gold table."""
    try:
        conn = duckdb.connect(DB_PATH, read_only=True)
        df = conn.execute("""
            SELECT carrier_code, flight_count
            FROM top_carriers_today
            ORDER BY flight_count DESC
            LIMIT 10
        """).df()
        conn.close()
        return df
    except Exception as e:
        return pd.DataFrame()


@st.cache_data(ttl=15)
def load_flights_by_hour():
    """Load flights per hour from Gold table."""
    try:
        conn = duckdb.connect(DB_PATH, read_only=True)
        df = conn.execute("""
            SELECT hour, flight_count
            FROM active_flights_by_hour
            ORDER BY hour ASC
        """).df()
        conn.close()
        return df
    except Exception as e:
        return pd.DataFrame()


@st.cache_data(ttl=15)
def load_airport_traffic():
    """Load airport traffic from Gold table."""
    try:
        conn = duckdb.connect(DB_PATH, read_only=True)
        df = conn.execute("""
            SELECT nearest_airport, flight_count
            FROM airport_traffic_summary
        """).df()
        conn.close()
        return df
    except Exception as e:
        return pd.DataFrame()


def load_incidents():
    """Load incident history from jsonl log."""
    incidents = []
    if os.path.exists(INCIDENTS_FILE):
        with open(INCIDENTS_FILE, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        incidents.append(json.loads(line))
                    except Exception:
                        pass
    return list(reversed(incidents))[-20:]  # last 20


def get_health_status(incidents):
    """
    Determine health status of each alarm type
    based on recent incidents.
    """
    recent = incidents[:5]
    recent_types = [i.get("alarm_type") for i in recent]

    return {
        "Freshness": "🔴 ALARM" if "FRESHNESS" in recent_types else "🟢 OK",
        "Validity": "🔴 ALARM" if "VALIDITY" in recent_types else "🟢 OK",
        "Anomaly": "🟡 WARN" if "ANOMALY" in recent_types else "🟢 OK",
        "Volume": "🟡 WARN" if "VOLUME" in recent_types else "🟢 OK",
    }


# ─────────────────────────────────────────
# AI HELPER
# ─────────────────────────────────────────
def ask_ai(question, flights_df, carriers_df, incidents):
    """Send question + data context to Claude API."""
    if not LLM_API_KEY:
        return "⚠️ No API key configured. Add LLM_API_KEY to your .env file."

    # Build context from real data
    total_flights = len(flights_df)
    top_carrier = carriers_df.iloc[0]["carrier_code"] if not carriers_df.empty else "unknown"
    top_count = int(carriers_df.iloc[0]["flight_count"]) if not carriers_df.empty else 0
    incident_count = len(incidents)
    recent_alarm = incidents[0]["message"] if incidents else "No recent alarms"

    context = f"""
You are an AI assistant for a real-time California flight monitoring pipeline.
Answer questions ONLY based on the data below. If the question is outside this data, say so clearly.

CURRENT DATA:
- Active flights over California: {total_flights}
- Top carrier: {top_carrier} with {top_count} flights
- Total incidents today: {incident_count}
- Most recent alarm: {recent_alarm}
- Data source: Airplanes.live (real ADS-B data)
- Pipeline: Kafka → Bronze → Silver → Gold (dbt)
"""

    try:
        import requests
        response = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": LLM_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            },
            json={
                "model": "claude-sonnet-4-6",
                "max_tokens": 300,
                "messages": [
                    {"role": "user", "content": f"{context}\n\nQuestion: {question}"}
                ]
            },
            timeout=30
        )
        data = response.json()
        return data["content"][0]["text"]
    except Exception as e:
        return f"⚠️ AI error: {e}"


# ─────────────────────────────────────────
# MAIN DASHBOARD
# ─────────────────────────────────────────
def main():
    # Header
    col1, col2 = st.columns([3, 1])
    with col1:
        st.title("✈️ Flight Monitor — California Airspace")
        st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} · Auto-refreshes every 15s")
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🔄 Refresh Now"):
            st.cache_data.clear()
            st.rerun()

    # Load all data
    flights_df = load_silver_flights()
    carriers_df = load_top_carriers()
    hours_df = load_flights_by_hour()
    incidents = load_incidents()
    health = get_health_status(incidents)

    # ── TOP METRICS ──
    st.markdown("---")
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("✈️ Active Flights", len(flights_df))
    with m2:
        st.metric("🏆 Top Carrier", carriers_df.iloc[0]["carrier_code"] if not carriers_df.empty else "N/A")
    with m3:
        st.metric("🚨 Incidents Today", len(incidents))
    with m4:
        st.metric("📡 Data Source", "Airplanes.live")

    st.markdown("---")

    # ── ZONE 2: HEALTH PANEL (THE STAR) ──
    st.subheader("🚨 System Health Panel")
    h1, h2, h3, h4 = st.columns(4)

    for col, (alarm_name, status) in zip([h1, h2, h3, h4], health.items()):
        is_ok = "OK" in status
        css_class = "health-green" if is_ok else "health-red"
        with col:
            st.markdown(f"""
            <div class="{css_class}">
                <div class="health-label">{alarm_name}</div>
                <div style="font-size:20px">{status}</div>
            </div>
            """, unsafe_allow_html=True)

    # Incident History
    st.markdown("**📋 Incident History**")
    if incidents:
        for incident in incidents[:8]:
            severity = incident.get("severity", "warning")
            color = "#dc2626" if severity == "critical" else "#f59e0b"
            st.markdown(f"""
            <div class="incident-row" style="border-left-color:{color}">
                <b>{incident.get('alarm_type', 'UNKNOWN')}</b> —
                {incident.get('message', '')}
                <span style="color:#64748b;font-size:11px;float:right">
                    {incident.get('timestamp', '')[:19]}
                </span>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.success("✅ No incidents recorded yet")

    st.markdown("---")

    # ── ZONE 1: LIVE MAP ──
    st.subheader("🗺️ Live Flight Map — California")
    if not flights_df.empty:
        map_df = flights_df[["latitude", "longitude"]].rename(
            columns={"latitude": "lat", "longitude": "lon"}
        )
        st.map(map_df, zoom=5)
        st.caption(f"Showing {len(flights_df)} real aircraft over California · Updates every 15 seconds")
    else:
        st.warning("⚠️ No flight data available — make sure the producer and consumer are running")

    st.markdown("---")

    # ── ZONE 3: CHARTS ──
    st.subheader("📊 Analytics")
    c1, c2 = st.columns(2)

    with c1:
        st.markdown("**Top Carriers Today**")
        if not carriers_df.empty:
            st.bar_chart(carriers_df.set_index("carrier_code")["flight_count"])
        else:
            st.info("No carrier data yet")

    with c2:
        st.markdown("**Flights Per Hour**")
        if not hours_df.empty:
            st.line_chart(hours_df.set_index("hour")["flight_count"])
        else:
            st.info("No hourly data yet")

    st.markdown("---")

    # ── AI QUERY BOX ──
    st.subheader("🤖 Ask About the Data")
    st.caption("Answers are grounded in real pipeline data only — never guesses")

    question = st.text_input(
        "Ask a question",
        placeholder="Which airline has the most flights today? Were there any data issues?"
    )

    if question:
        with st.spinner("Thinking..."):
            answer = ask_ai(question, flights_df, carriers_df, incidents)
        st.markdown(f"**Answer:** {answer}")

    # Auto-refresh every 15 seconds
    time.sleep(15)
    st.rerun()


if __name__ == "__main__":
    main()