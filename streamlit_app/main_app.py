import streamlit as st

st.title('Fantasy Premier League Dashboard')

home = st.page_link(
    label="HOME", icon="💥", page="app2.py"
)
game_week = st.page_link(page="app3.py", label="Gameweek Info", )

import streamlit as st
import duckdb
import plotly.express as px
import pandas as pd

st.title("Football Dashboard")

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(
    ["Standings", "Players", "Injuries", "Results", "Transfers", "Gameweek Info"])


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

with tab1:
    st.subheader("League Standings")


    # -----------------------------------------------------
    # 🏅 LEAGUE STANDINGS
    # -----------------------------------------------------
    st.subheader(f"📊  {season} Standings")
    st.dataframe(
        filtered_standings.sort_values(by=['Points', 'GD'], ascending=False, inplace=False),
        width='stretch',
    )

with tab2:
    st.subheader("Players Stats")
    st.write("Show players data here")
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

with tab3:
    st.subheader("Injury News")

    # -----------------------------------------------------
    # 🩺 INJURY NEWS
    # -----------------------------------------------------

    if not injuries.empty:
        recent_injuries = injuries.sort_values("NEWS_DATED", ascending=False).head(20)
        for _, row in recent_injuries.iterrows():
            st.markdown(f"**{row['NAME']}** ({row['CLUB']}): {row['NEWS']} — *{row['NEWS_DATED']}*")
    else:
        st.info("No recent injury news available.")

    # ----------------------------------------------
    #

with tab4:
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

with tab5:
    st.markdown("---")
    st.subheader("🩺 Transfers")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("⚽ Top Transfers In")
        top_goals = filtered_players[["web_name", "CLUB", "transfers_in_event"]].sort_values('transfers_in_event',
                                                                                             ascending=False).head(10)
        st.dataframe(top_goals, width='stretch')

    with col2:
        st.subheader("🎯 Top Transfers Out")
        top_assists = filtered_players[["web_name", "CLUB", "transfers_out_event"]].sort_values('transfers_out_event',
                                                                                                ascending=False).head(
            10)
        st.dataframe(top_assists, width='stretch')

with tab6:
    gameweek = st.sidebar.selectbox("Gameweek",
                                    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
                                     24, 25, 26, 27, 28,
                                     29, 30, 31, 32, 33, 34, 35, 6, 37, 38], index=3)

    col1, col2 = st.columns(2)
    conn = duckdb.connect("epl_duckdb.duckdb", read_only=True)
    df = conn.execute("""select * from dim_gameweek_info order by id desc""").df()
    df1 = conn.execute("""select transfers_made, Season, finished, id from dim_events 
                        order by id desc ; """).df()
    # print(df.columns)
    df1 = df1[df1["Season"] == '2025-2026']
    df = df[df["Season"] == '2025-2026']
    transfers_made = df1.loc[df1['id'] == gameweek, 'transfers_made'].values[0]
    most_captained = df.loc[df['id'] == gameweek, 'Most_Captained'].values[0]
    most_transferred_in = df.loc[df['id'] == gameweek, 'Most_Transferred_In'].values[0]
    player_of_Week = df.loc[df['id'] == gameweek, 'Most_Points'].values[0]

    average_points = df.loc[df['id'] == gameweek, 'Average_Points'].values[0]
    most_points = df.loc[df['id'] == gameweek, 'Highest_Points'].values[0]
    with col1:
        st.markdown(
            f"""
            <div style="background-color:#f0f2f6;padding:15px;border-radius:10px;text-align:center;">
                <h3 style="color:#008000;">Transfers Made</h3>
                 <b>{transfers_made}</b>
                
            </div>
            """, unsafe_allow_html=True
        )

    with col2:
        st.markdown(
            f"""
            <div style="background-color:#f0f2f6;padding:15px;border-radius:10px;text-align:center;">
                <h3 style="color:#FF4B4B;">Most Captained</h3>
                <b>{most_captained}</b>
                
            </div>
            """, unsafe_allow_html=True
        )

    col3, col4 = st.columns(2)

    with col3:
        st.markdown(
            f"""
            <div style="background-color:#f0f2f6;padding:15px;border-radius:10px;text-align:center;">
                <h3 style="color:#1E90FF;">Most Transferred In</h3>
                <b>{most_transferred_in}</b>
               
            </div>
            """, unsafe_allow_html=True
        )

    with col4:
        st.markdown(
            f"""
            <div style="background-color:#f0f2f6;padding:15px;border-radius:10px;text-align:center;">
                <h3 style="color:#1E90FF;">Player Of Week</h3>
                <b>{player_of_Week}</b>
               
            </div>
            """, unsafe_allow_html=True
        )

    df_team = conn.execute("""select * from dim_team_of_gameweek """).df()

    coll1, coll2 = st.columns(2)

    with coll1:
        st.markdown(
            f"""
            <div style="background-color:#f0f2f6;padding:15px;border-radius:10px;text-align:center;">
                <h3 style="color:#008000;">Average Points</h3>
                 <b>{average_points}</b>
                
            </div>
            """, unsafe_allow_html=True
        )

    with coll2:
        st.markdown(
            f"""
            <div style="background-color:#f0f2f6;padding:15px;border-radius:10px;text-align:center;">
                <h3 style="color:#FF4B4B;">Most Points</h3>
                <b>{most_points}</b>
                
            </div>
            """, unsafe_allow_html=True
        )

    df3 = conn.execute("select * from dim_chip_played").df()

    st.subheader("Chip Played")

    result = df3.loc[(df3['id'] == gameweek) & (df3['chip_name'] == '3xc'), 'num_played']

    triple_captain = result.iloc[0] if not result.empty else 0

    wildcard_played = df3.loc[(df3['id'] == gameweek) & (df3['chip_name'] == 'wildcard'), 'num_played'].iloc[0]
    bboost = df3.loc[(df3['id'] == gameweek) & (df3['chip_name'] == 'bboost'), 'num_played'].iloc[0]

    freehit = df3.loc[(df3['id'] == gameweek) & (df3['chip_name'] == 'freehit'), 'num_played'].iloc[0]

    col3, col4 = st.columns(2)

    with col3:
        st.markdown(
            f"""
            <div style="background-color:#f0f2f6;padding:15px;border-radius:10px;text-align:center;">
                <h3 style="color:#1E90FF;">Triple Captained Played</h3>
                <b>{triple_captain}</b>
               
            </div>
            """, unsafe_allow_html=True
        )

    with col4:
        st.markdown(
            f"""
            <div style="background-color:#f0f2f6;padding:15px;border-radius:10px;text-align:center;">
                <h3 style="color:#1E90FF;">Wildcards Played</h3>
                <b>{wildcard_played}</b>
               
            </div>
            """, unsafe_allow_html=True
        )

    coll1, coll2 = st.columns(2)

    with coll1:
        st.markdown(
            f"""
            <div style="background-color:#f0f2f6;padding:15px;border-radius:10px;text-align:center;">
                <h3 style="color:#008000;">Bench Boost</h3>
                 <b>{bboost}</b>
                
            </div>
            """, unsafe_allow_html=True
        )

    with coll2:
        st.markdown(
            f"""
            <div style="background-color:#f0f2f6;padding:15px;border-radius:10px;text-align:center;">
                <h3 style="color:#FF4B4B;">Freehit</h3>
                <b>{freehit}</b>
                
            </div>
            """, unsafe_allow_html=True
        )

    st.subheader("Team of Week")
    st.dataframe(df_team)
