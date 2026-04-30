import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import duckdb

st.set_page_config(page_title="FPL Player Dashboard", layout="wide")
st.title("⚽ FPL Player Dashboard")

# ------------------------------------------------------------
# TEAM + POSITION MAP
# ------------------------------------------------------------
TEAM_MAP = {
    1: "Arsenal", 2: "Aston Villa", 3: "Bournemouth", 4: "Brentford", 5: "Brighton",
    6: "Chelsea", 7: "Crystal Palace", 8: "Everton", 9: "Fulham", 10: "Ipswich",
    11: "Leicester", 12: "Liverpool", 13: "Man City", 14: "Man United",
    15: "Newcastle", 16: "Nottingham Forest", 17: "Southampton",
    18: "Spurs", 19: "West Ham", 20: "Wolves"
}

POSITION_MAP = {1: "GK", 2: "DEF", 3: "MID", 4: "FWD"}

# ------------------------------------------------------------
# LOAD DATA
# ------------------------------------------------------------
@st.cache_data
def load_data(path="epl_duckdb.duckdb"):
    conn = duckdb.connect(path, read_only=True)
    try:
        df = conn.execute("SELECT * FROM epl_data.epl_raw_table__elements WHERE _dlt_parent_id = (SELECT _dlt_id FROM epl_data.epl_raw_table ORDER BY load_date DESC LIMIT 1)").df()
        # df = pd.read_csv(path)
    finally:
        pass

    numeric_cols = [
        "now_cost", "total_points", "event_points", "minutes",
        "goals_scored", "assists", "clean_sheets", "saves",
        "bonus", "bps", "selected_by_percent",
        "expected_goals_per_90", "expected_assists_per_90",
        "expected_goal_involvements_per_90",
        "expected_goals_conceded_per_90",
        "goals_conceded_per_90",
        "starts_per_90", "clean_sheets_per_90",
        "defensive_contribution_per_90",
        "clearances_blocks_interceptions",
        "recoveries", "tackles", "defensive_contribution"
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    str_numeric_cols = [
        "form", "points_per_game", "ict_index",
        "influence", "creativity", "threat",
        "expected_goals", "expected_assists",
        "expected_goal_involvements", "expected_goals_conceded"
    ]

    for col in str_numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["full_name"] = df["first_name"].fillna("") + " " + df["second_name"].fillna("")
    df["now_cost_m"] = df["now_cost"] / 10

    df["position"] = df["element_type"].map(POSITION_MAP)
    df["team_name"] = df["team"].map(TEAM_MAP)

    df["points_per_million"] = df["total_points"] / df["now_cost_m"]

    return df


# ------------------------------------------------------------
# SIDEBAR FILTERS
# ------------------------------------------------------------
st.sidebar.header("🔍 Filters")
# data_path = st.sidebar.text_input("CSV file path", "players.csv")

df = load_data()

search_name = st.sidebar.text_input("Search Player Name")

team_list = ["All"] + sorted(df["team_name"].dropna().unique().tolist())
pos_list = ["All"] + sorted(df["position"].dropna().unique().tolist())
status_list = ["All"] + sorted(df["status"].dropna().unique().tolist())

selected_team = st.sidebar.selectbox("Team", team_list)
selected_pos = st.sidebar.selectbox("Position", pos_list)
selected_status = st.sidebar.selectbox("Status", status_list)

min_price = float(df["now_cost_m"].min())
max_price = float(df["now_cost_m"].max())

price_range = st.sidebar.slider("Price Range (Million)", min_price, max_price, (min_price, max_price))

# Apply filters
filtered = df.copy()

if search_name:
    filtered = filtered[filtered["full_name"].str.contains(search_name, case=False, na=False)]

if selected_team != "All":
    filtered = filtered[filtered["team_name"] == selected_team]

if selected_pos != "All":
    filtered = filtered[filtered["position"] == selected_pos]

if selected_status != "All":
    filtered = filtered[filtered["status"] == selected_status]

filtered = filtered[(filtered["now_cost_m"] >= price_range[0]) & (filtered["now_cost_m"] <= price_range[1])]

# ------------------------------------------------------------
# KPI SECTION
# ------------------------------------------------------------
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Players", len(filtered))
k2.metric("Avg Points", round(filtered["total_points"].mean(), 2))
k3.metric("Avg Cost (M)", round(filtered["now_cost_m"].mean(), 2))
k4.metric("Avg Form", round(filtered["form"].mean(), 2))
k5.metric("Avg ICT", round(filtered["ict_index"].mean(), 2))

st.divider()

# ------------------------------------------------------------
# PLAYER LIST
# ------------------------------------------------------------
player_list = filtered["full_name"].dropna().unique().tolist()

if len(player_list) == 0:
    st.error("No players found with current filters.")
    st.stop()

selected_player = st.selectbox("Select Player", player_list)
player = filtered[filtered["full_name"] == selected_player].iloc[0]

# ------------------------------------------------------------
# PLAYER PHOTO
# ------------------------------------------------------------
photo_col, info_col = st.columns([1, 3])

with photo_col:
    st.write("### Player Photo")
    if "photo" in player and pd.notna(player["photo"]):
        # If your photo column is already URL, use directly:
        # st.image(player["photo"], use_column_width=True)

        # If photo is FPL-style ID, use this:
        photo_url = f"https://resources.premierleague.com/premierleague/photos/players/110x140/p{str(player['photo']).replace('.jpg','')}.png"
        st.image(photo_url, use_container_width=True)
    else:
        st.info("No photo available")

with info_col:
    st.write(f"## {player['full_name']}")
    st.write(f"**Team:** {player.get('team_name')} | **Position:** {player.get('position')} | **Status:** {player.get('status')}")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Points", player.get("total_points"))
    c2.metric("Cost (M)", player.get("now_cost_m"))
    c3.metric("Form", player.get("form"))
    c4.metric("Selected %", player.get("selected_by_percent"))

st.divider()

# ------------------------------------------------------------
# TABS
# ------------------------------------------------------------
tab1, tab2, tab3, tab4, tab5 = st.tabs(["📌 Overview", "📈 Performance", "🧠 Advanced", "🛡️ Defensive", "📄 Dataset"])

# ------------------------------------------------------------
# TAB 1 - OVERVIEW
# ------------------------------------------------------------
with tab1:
    st.subheader("🏆 Player Overview")

    o1, o2, o3, o4 = st.columns(4)
    o1.metric("Minutes", player.get("minutes"))
    o2.metric("Goals", player.get("goals_scored"))
    o3.metric("Assists", player.get("assists"))
    o4.metric("Clean Sheets", player.get("clean_sheets"))

    st.write("### News")
    st.warning(player.get("news", "No news available"))

# ------------------------------------------------------------
# TAB 2 - PERFORMANCE
# ------------------------------------------------------------
with tab2:
    st.subheader("📈 Performance Stats")

    perf_cols = [
        "event_points", "points_per_game", "bonus", "bps",
        "yellow_cards", "red_cards"
    ]
    perf_data = {col: player.get(col) for col in perf_cols if col in player.index}
    st.dataframe(pd.DataFrame([perf_data]))

    st.subheader("📊 Points vs Cost (Filtered Players)")
    fig = px.scatter(
        filtered,
        x="now_cost_m",
        y="total_points",
        color="position",
        hover_data=["full_name", "team_name"],
        title="Total Points vs Cost"
    )
    st.plotly_chart(fig, use_container_width=True)

# ------------------------------------------------------------
# TAB 3 - ADVANCED
# ------------------------------------------------------------
with tab3:
    st.subheader("🧠 Advanced Metrics")

    a1, a2, a3, a4 = st.columns(4)
    a1.metric("xG/90", player.get("expected_goals_per_90"))
    a2.metric("xA/90", player.get("expected_assists_per_90"))
    a3.metric("xGI/90", player.get("expected_goal_involvements_per_90"))
    a4.metric("ICT", player.get("ict_index"))

    st.subheader("📌 Radar Chart (Influence / Creativity / Threat / ICT)")

    radar_metrics = ["influence", "creativity", "threat", "ict_index"]
    radar_values = [player.get(m, 0) if pd.notna(player.get(m)) else 0 for m in radar_metrics]

    fig_radar = go.Figure()
    fig_radar.add_trace(go.Scatterpolar(
        r=radar_values,
        theta=radar_metrics,
        fill="toself",
        name=player["full_name"]
    ))

    fig_radar.update_layout(
        polar=dict(radialaxis=dict(visible=True)),
        showlegend=True
    )

    st.plotly_chart(fig_radar, use_container_width=True)

    st.subheader("📊 Top 20 by Selected Metric")
    metric = st.selectbox(
        "Choose metric",
        ["expected_goals_per_90", "expected_assists_per_90", "expected_goal_involvements_per_90",
         "influence", "creativity", "threat", "ict_index"]
    )

    fig_metric = px.bar(
        filtered.sort_values(metric, ascending=False).head(20),
        x="full_name",
        y=metric,
        color="position",
        title=f"Top 20 Players by {metric}"
    )
    st.plotly_chart(fig_metric, use_container_width=True)

# ------------------------------------------------------------
# TAB 4 - DEFENSIVE
# ------------------------------------------------------------
with tab4:
    st.subheader("🛡️ Defensive Contribution")

    d1, d2, d3, d4 = st.columns(4)
    d1.metric("Tackles", player.get("tackles"))
    d2.metric("Recoveries", player.get("recoveries"))
    d3.metric("Clearances/Blocks/Interceptions", player.get("clearances_blocks_interceptions"))
    d4.metric("Defensive Contribution", player.get("defensive_contribution"))

    st.subheader("📊 Defensive Contribution per 90 (Top 20)")
    if "defensive_contribution_per_90" in filtered.columns:
        fig_def = px.bar(
            filtered.sort_values("defensive_contribution_per_90", ascending=False).head(20),
            x="full_name",
            y="defensive_contribution_per_90",
            color="position",
            title="Top 20 Defensive Contribution per 90"
        )
        st.plotly_chart(fig_def, use_container_width=True)

# ------------------------------------------------------------
# TAB 5 - FULL DATASET
# ------------------------------------------------------------
with tab5:
    st.subheader("📄 Filtered Dataset")
    st.dataframe(filtered)

# ------------------------------------------------------------
# COMPARE TWO PLAYERS
# ------------------------------------------------------------
st.divider()
st.subheader("⚔️ Compare Two Players")

p1, p2 = st.columns(2)

player_a = st.selectbox("Select Player A", df["full_name"].dropna().unique(), index=0)
player_b = st.selectbox("Select Player B", df["full_name"].dropna().unique(), index=1)

pa = df[df["full_name"] == player_a].iloc[0]
pb = df[df["full_name"] == player_b].iloc[0]

compare_metrics = [
    "now_cost_m", "total_points", "minutes",
    "goals_scored", "assists", "clean_sheets",
    "expected_goals_per_90", "expected_assists_per_90",
    "expected_goal_involvements_per_90", "ict_index"
]

compare_df = pd.DataFrame({
    "Metric": compare_metrics,
    player_a: [pa.get(m) for m in compare_metrics],
    player_b: [pb.get(m) for m in compare_metrics]
})

st.dataframe(compare_df)

st.subheader("📌 Comparison Radar Chart")

radar_metrics = ["influence", "creativity", "threat", "ict_index"]

pa_vals = [pa.get(m, 0) if pd.notna(pa.get(m)) else 0 for m in radar_metrics]
pb_vals = [pb.get(m, 0) if pd.notna(pb.get(m)) else 0 for m in radar_metrics]

fig_compare = go.Figure()

fig_compare.add_trace(go.Scatterpolar(
    r=pa_vals,
    theta=radar_metrics,
    fill="toself",
    name=player_a
))

fig_compare.add_trace(go.Scatterpolar(
    r=pb_vals,
    theta=radar_metrics,
    fill="toself",
    name=player_b
))

fig_compare.update_layout(
    polar=dict(radialaxis=dict(visible=True)),
    showlegend=True
)

st.plotly_chart(fig_compare, use_container_width=True)