# football_dashboard.py
import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

# -----------------------------------------------------
# ⚙️ CONFIG
# -----------------------------------------------------
st.set_page_config(page_title="Football Dashboard", layout="wide")
st.title("⚽ Football Analytics Dashboard")


# -----------------------------------------------------
# 🗃️ DATA LOADING
# -----------------------------------------------------
@st.cache_data
def load_data():
    conn = duckdb.connect("epl_duckdb.duckdb", read_only=True)

    standings = conn.execute("SELECT * FROM dim_standings").df()
    players = conn.execute(
        "SELECT distinct(web_name), dt.name as CLUB, goals_scored,assists, minutes,dt.Season as  Season, second_name, total_points,"
        " starts,event_points as Gameweek_Points, clean_sheets, Saves, cast(expected_goals as DOUBLE) as expected_goals,"
        " cast(expected_assists as DOUBLE) as expected_assists ,cast(expected_goal_involvements as DOUBLE) as expected_goal_involvements,"
        "goals_conceded,cast(expected_goals_conceded as DOUBLE) as expected_goals_conceded , own_goals, penalties_saved, penalties_missed, yellow_cards, red_cards,"
        "bonus, bps, cast(influence as DOUBLE) as influence, cast(creativity as DOUBLE) as creativity,  cast(threat as DOUBLE) as threat, cast(ict_index as DOUBLE) as ict_index, transfers_in_event, transfers_out_event, transfers_in,"
        "transfers_out FROM src_elements se inner join dim_teams dt on se.team_code = dt.code").df()
    matches = conn.execute("SELECT * FROM dim_results").df()



    # Optional: injury news table
    try:
        injuries = conn.execute("SELECT * FROM dim_injury").df()
    except Exception:
        injuries = pd.DataFrame(columns=["NAME", "CLUB", "NEWS", "NEWS_DATED"])

    conn.close()
    return standings, players, matches, injuries


standings, players, matches, injuries = load_data()

# print(players)
# -----------------------------------------------------
# 🎛️ SIDEBAR FILTERS
# -----------------------------------------------------
st.sidebar.header("Filters")
# league = st.sidebar.selectbox("Select League", sorted(standings["league"].unique()))
season = st.sidebar.selectbox("Select Season", sorted(standings["Season"].unique(), reverse=True))

team_options = sorted(players["CLUB"].unique())
team = st.sidebar.selectbox("Select Team (optional)", ["All"] + team_options)

# Filter data
filtered_standings = (standings[standings["Season"] == season])
filtered_players = (players[players["Season"] == season])
filtered_matches = (matches[matches["Season"] == season])

if team != "All":
    filtered_players = filtered_players[filtered_players["CLUB"] == team]
    filtered_matches = filtered_matches[
        (filtered_matches["HOME_TEAM"] == team) | (filtered_matches["AWAY_TEAM"] == team)
        ]

# -----------------------------------------------------
# 🏅 LEAGUE STANDINGS
# -----------------------------------------------------
st.subheader(f"📊  {season} Standings")
st.dataframe(
    filtered_standings.sort_values(by=['Points', 'GD'], ascending=False, inplace=False),
    width='stretch',
)

# -----------------------------------------------------
# ⚽ PLAYER STATS
# -----------------------------------------------------
col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("⚽ Top Scorers")
    top_goals = filtered_players.nlargest(10, "goals_scored")[
        ["web_name", "CLUB", "goals_scored"]]
    st.dataframe(top_goals, width='stretch')

with col2:
    st.subheader("🎯 Top Assists")
    top_assists = filtered_players.nlargest(10, "assists").drop_duplicates(subset='web_name')[
        ["web_name", "CLUB", "assists"]]
    st.dataframe(top_assists, width='stretch')

with col3:
    st.subheader("⏱️ Most Minutes Played")
    top_minutes = filtered_players.nlargest(10, "minutes").drop_duplicates(subset='web_name')[
        ["web_name", "CLUB", "minutes"]]
    st.dataframe(top_minutes, width='stretch')

# -----------------------------------------------------
# 📈 VISUALIZATIONS
# -----------------------------------------------------
st.markdown("---")
st.subheader("📊 Player Performance Comparison")

metric = st.selectbox("Choose Metric", ["goals_scored", "assists", "minutes", 'total_points', 'saves', 'starts',
                                        'expected_goals', 'expected_assists', 'expected_goal_involvements',
                                        "goals_conceded", 'expected_goals_conceded', 'own_goals', 'penalties_saved',
                                        'penalties_missed', 'yellow_cards', 'red_cards',
                                        "bonus", 'bps', 'influence', 'creativity', 'threat', 'ict_index',
                                        'transfers_in_event', 'transfers_out_event', 'transfers_in',
                                        "transfers_out",
                                        'Gameweek_Points', 'clean_sheets'])
fig = px.bar(
    filtered_players.nlargest(15, metric),
    x="web_name",
    y=metric,
    color="CLUB",
    title=f"Top 15 Players by {metric.capitalize()}",
)
st.plotly_chart(
    fig,
    config={
        "displayModeBar": True,
        "displaylogo": False,
        "responsive": True
    },
)

# -----------------------------------------------------
# 🏟️ MATCH RESULTS
# -----------------------------------------------------
st.markdown("---")
st.subheader("📅 Recent Match Results")

recent_matches = filtered_matches.sort_values("kickoff_time", ascending=False).head(10)
for _, match in recent_matches.iterrows():
    st.markdown(
        f"**{match['kickoff_time']}** — {match['HOME_TEAM']} {match['HOME_TEAM_GOALS']} - {match['AWAY_TEAM_GOALS']} {match['AWAY_TEAM']}"
    )

# -----------------------------------------------------
# 🩺 INJURY NEWS
# -----------------------------------------------------
st.markdown("---")
st.subheader("🩺 Latest Injury News")

if not injuries.empty:
    recent_injuries = injuries.sort_values("NEWS_DATED", ascending=False).head(5)
    for _, row in recent_injuries.iterrows():
        st.markdown(f"**{row['NAME']}** ({row['CLUB']}): {row['NEWS']} — *{row['NEWS_DATED']}*")
else:
    st.info("No recent injury news available.")

# ----------------------------------------------
#
st.markdown("---")
st.subheader("🩺 Transfers")


col1, col2 = st.columns(2)

with col1:
    st.subheader("⚽ Top Transfers In")
    top_goals = filtered_players[["web_name", "CLUB", "transfers_in_event"]].sort_values('transfers_in_event', ascending=False).head(10)
    st.dataframe(top_goals, width='stretch')

with col2:
    st.subheader("🎯 Top Transfers Out")
    top_assists = filtered_players[["web_name", "CLUB", "transfers_out_event"]].sort_values('transfers_out_event', ascending=False).head(10)
    st.dataframe(top_assists, width='stretch')
