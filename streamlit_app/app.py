import duckdb
import streamlit as st

# Connect to the shared DuckDB file
DB_PATH = "epl_duckdb.duckdb"  # or "data/epl.db" if running locally
conn = duckdb.connect(DB_PATH, read_only=True)

st.set_page_config(page_title="EPL Dashboard", layout="wide")

st.title("⚽ English Premier League Dashboard")

# Sidebar filters
st.sidebar.header("Filters")
season = st.sidebar.selectbox("Season", ["2022-23", "2023-24", "2024-25", "2025-2026"], index=3)

# --- Team Standings ---
st.subheader("🏆 Team Standings")
standings_query = f"""
SELECT
    team,
    SUM(points) AS total_points,
    SUM(W) AS wins,
    SUM(L) AS losses,
    SUM(D) AS draws,
    SUM(GF) AS goals_for,
    SUM(GC) AS goals_against,
    sum(GD) AS goals_difference
FROM dim_standings
WHERE season = '{season}'
GROUP BY team
ORDER BY total_points DESC;
"""
standings_df = conn.execute(standings_query).df()
st.dataframe(standings_df, width='stretch')

# --- Most Goals ---
st.subheader("⚽ Top Goal Scorers")
goals_query = f"""
SELECT distinct(web_name), club, goals_scored AS goals
FROM fact_players_info
WHERE season = '{season}'

ORDER BY goals_scored DESC
LIMIT 10;
"""
goals_df = conn.execute(goals_query).df()
st.bar_chart(goals_df.set_index("web_name")["goals"])

# --- Most Assists ---
st.subheader("🎯 Top Assists")
assists_query = f"""
SELECT distinct(web_name), club, assists AS assists
FROM fact_players_info
WHERE season = '{season}'

ORDER BY assists DESC
LIMIT 10;
"""
assists_df = conn.execute(assists_query).df()
st.bar_chart(assists_df.set_index("web_name")["assists"])

# --- Most Minutes Played ---
st.subheader("⏱️ Most Minutes Played")
minutes_query = f"""
SELECT distinct(web_name), club,minutes AS minutes
FROM fact_players_info
WHERE season = '{season}'

ORDER BY minutes DESC
LIMIT 10;
"""
minutes_df = conn.execute(minutes_query).df()
st.bar_chart(minutes_df.set_index("web_name")["minutes"])

# --- Injury Lists of Players ----

st.subheader("🩺 Injury/Transfer  News")
injury_query = f"""
select NAME, pos,CLUB, NEWS, NEWS_DATED from dim_injury where season = '{season}'
order by NEWS_DATED desc ;
"""
injury_df = conn.execute(injury_query).df()

# maybe add filter:
team = st.selectbox("Filter by team", ["All"] + sorted(injury_df["CLUB"].unique()))
if team != "All":
    st.dataframe(injury_df[injury_df["CLUB"] == team])
else:
    st.dataframe(injury_df)

st.caption("Data source: DuckDB (from DLT pipeline and dbt transformations)")
