from textwrap import dedent

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st


# -----------------------------
# App config
# -----------------------------


st.markdown(
    """
    <style>
        .stApp {
            background-color: #F8EFEA;
        }
    </style>
""",
    unsafe_allow_html=True,
)

st.set_page_config(page_title="⚽ FPL Dashboard", layout="wide", page_icon="⚽")

# Dark mode styling (simple CSS overrides)
DARK_CSS = """
<style>
:root {
  --bg: #0d1117;
  --panel: #161b22;
  --text: #e6edf3;
  --accent: #58a6ff;
  --accent-hover: #79c0ff;
  --muted: #8b949e;
  --border: #30363d;
}
body, .stApp {
  background-color: var(--bg);
  color: var(--text);
}
.block-container {
  padding-top: 1rem;
}
.card {
  background-color: var(--panel);
  padding: 14px 18px;
  border-radius: 10px;
  border: 1px solid var(--border);
  box-shadow: 0 2px 10px rgba(0,0,0,0.35);
}
.card h3 {
  color: var(--accent);
  margin-bottom: 6px;
}
.metric-value {
  font-size: 34px;
  font-weight: 700;
  color: var(--text);
}
.small-muted {
  color: var(--muted);
  font-size: 12px;
}
/* Button styling */
.stButton>button {
  background-color: var(--accent);
  border-radius: 6px;
  color: #000;
  border: none;
}
.stButton>button:hover {
  background-color: var(--accent-hover);
}
/* Dataframe styling */
.dataframe tbody tr:hover {
  background-color: rgba(88, 166, 255, 0.1);
}
</style>
"""

st.markdown(DARK_CSS, unsafe_allow_html=True)


# -----------------------------
# Helper: load data
# -----------------------------
@st.cache_data
def load_data(path="epl_duckdb.duckdb"):
    conn = duckdb.connect(path, read_only=True)
    try:
        standings = conn.execute("SELECT * FROM dim_standings").df()
    except Exception:
        standings = pd.DataFrame()
    try:
        players = conn.execute(""" WITH elements AS (
SELECT * FROM epl_data.epl_raw_table__elements WHERE _dlt_parent_id = (SELECT _dlt_id FROM epl_data.epl_raw_table ORDER BY load_date DESC LIMIT 1) )
SELECT web_name,  goals_scored, assists, minutes, second_name, total_points, starts,event_points as Gameweek_Points, clean_sheets, Saves, cast(expected_goals as DOUBLE) as expected_goals, cast(expected_assists as DOUBLE) as expected_assists ,cast(expected_goal_involvements as DOUBLE) as expected_goal_involvements, goals_conceded,cast(expected_goals_conceded as DOUBLE) as expected_goals_conceded , own_goals, penalties_saved, penalties_missed, yellow_cards, red_cards, bonus, bps, cast(influence as DOUBLE) as influence, cast(creativity as DOUBLE) as creativity,  cast(threat as DOUBLE) as threat, cast(ict_index as DOUBLE) as ict_index, transfers_in_event, transfers_out_event, transfers_in, transfers_out ,  dt.name as CLUB,dt.Season as Season, FROM elements inner JOIN epl_duckdb.main.dim_teams dt   on elements.team_code = dt.code
       """).df()
    except Exception:
        players = pd.DataFrame()
    try:
        matches = conn.execute("SELECT * FROM dim_results").df()
    except Exception:
        matches = pd.DataFrame()
    try:
        injuries = conn.execute("SELECT * FROM dim_injury").df()
    except Exception:
        injuries = pd.DataFrame(
            columns=["NAME", "CLUB", "NEWS", "NEWS_DATED", "Season"]
        )
    try:
        gameweek_info = conn.execute("SELECT * FROM dim_gameweek_info").df()
    except Exception:
        gameweek_info = pd.DataFrame()
    try:
        events = conn.execute("SELECT * FROM dim_events").df()
    except Exception:
        events = pd.DataFrame()
    try:
        team_of_week = conn.execute("select * from dim_team_of_gameweek").df()
    except Exception:
        team_of_week = pd.DataFrame()
    try:
        chip_played = conn.execute("select * from dim_chip_played").df()
    except Exception:
        chip_played = pd.DataFrame()
    try:
        fixtures = conn.execute("""with raw_matches as (
select * from epl_duckdb.epl_data.matches m  where finished !=TRUE
AND load_date =(SELECT max(load_date) FROM epl_duckdb.epl_data.matches m 
)
),
teams AS (
SELECT * FROM epl_duckdb.main.dim_teams dt),
teams2 AS (
SELECT * FROM epl_duckdb.main.dim_teams dt)


select r.event AS 'Gameweek', r.kickoff_time,t.name AS 'HOME_TEAM',t1.name AS 'AWAY_TEAM',case when month(kickoff_time) in (8,
                                     9,10,12,11) then year(kickoff_time):: varchar || '-' || (year(kickoff_time) + 1):: varchar when
                                     month(kickoff_time) in (1,2,3,4,5) then
                                     (year(kickoff_time) - 1):: varchar || '-' ||  year(kickoff_time):: varchar
                                     end as Season from raw_matches r JOIN teams t ON r.team_h = t.id 
                                     JOIN teams2 t1 ON r.team_a = t1.id  order by r.event,r.id
    """).df()
    except Exception:
        fixtures = pd.DataFrame()


    conn.close()
    return {
        "standings": standings,
        "players": players,
        "matches": matches,
        "injuries": injuries,
        "gameweek_info": gameweek_info,
        "events": events,
        "team_of_week": team_of_week,
        "chip_played": chip_played,
        "fixtures": fixtures
    }


DATA = load_data()

standings = DATA["standings"]
players = DATA["players"]
matches = DATA["matches"]
injuries = DATA["injuries"]
gameweek_info = DATA["gameweek_info"]
events = DATA["events"]
team_of_week = DATA["team_of_week"]
chip_played = DATA["chip_played"]
fixtures = DATA['fixtures']

# --- Normalize club names across all tables ---
if not players.empty and "CLUB" in players.columns:
    players["CLUB"] = players["CLUB"].str.title()

if not injuries.empty and "CLUB" in injuries.columns:
    injuries["CLUB"] = injuries["CLUB"].str.title()

if not team_of_week.empty and "club" in team_of_week.columns:
    team_of_week["club"] = team_of_week["club"].str.title()

if not matches.empty:
    if "HOME_TEAM" in matches.columns:
        matches["HOME_TEAM"] = matches["HOME_TEAM"].str.title()
    if "AWAY_TEAM" in matches.columns:
        matches["AWAY_TEAM"] = matches["AWAY_TEAM"].str.title()

# -----------------------------
# Sidebar: Navigation & Filters
# -----------------------------


st.markdown(
    """
    <style>
        @keyframes gradient {
            0% {background-position: 0% 0%;}
            50% {background-position: 100% 100%;}
            100% {background-position: 0% 0%;}
        }
        [data-testid="stSidebar"] {
            background: linear-gradient(45deg, #ff512f, #dd2476, #24c6dc, #514a9d);
            background-size: 600% 600%;
            animation: gradient 15s ease infinite;
        }
        [data-testid="stSidebar"] * {
            color: white !important;
        }
    </style>
""",
    unsafe_allow_html=True,
)

st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Go to",
    [
        "Overview",
        "Standings",
        "Players",
        "Gameweek",
        "Injuries",
        "Results",
        "Transfers",
        "Fixtures"
    ],
    index=0,
)

st.sidebar.markdown("---")
# Common filters
season = None
if not standings.empty:
    season = st.sidebar.selectbox(
        "Season", sorted(standings["Season"].unique(), reverse=True)
    )
elif not players.empty:
    season = st.sidebar.selectbox(
        "Season", sorted(players["Season"].unique(), reverse=True)
    )
else:
    season = st.sidebar.text_input("Season", value="2025-2026")

team_list = []
if not players.empty:
    team_list = sorted(players["CLUB"].dropna().unique())
team = st.sidebar.selectbox("Team (optional)", ["All"] + team_list)

# Quick metrics on top in sidebar
st.sidebar.markdown("---")
if not events.empty:
    latest_event = events.sort_values("id", ascending=False).iloc[0]
    st.sidebar.metric("Current GW", latest_event.get("id", "-"), delta=None)


# -----------------------------
# Utility functions
# -----------------------------


def filter_season(df):
    if df is None or df.empty:
        return df
    if "Season" in df.columns:
        return df[df["Season"] == season]
    return df


# -----------------------------
# Overview Page
# -----------------------------


if page == "Overview":
    st.markdown(
        """
    <style>
        h1, h2, h3 {
            color: #EEEBBC ; /* Arsenal Deep Navy */
            font-weight: 700;
        }
    </style>
""",
        unsafe_allow_html=True,
    )
    st.header("Overview")
    col1, col2, col3 = st.columns([1, 2, 1])

    # Top metrics
    with col1:
        st.markdown("### Top Goal Scorers")
        if not players.empty:
            top = (
                filter_season(players).nlargest(5, "goals_scored")[
                    ["web_name", "CLUB", "goals_scored"]
                ]
                if "goals_scored" in players.columns
                else None
            )
            if top is not None and not top.empty:
                st.table(top)
            else:
                st.write("No data")
        else:
            st.write("No players data")

    with col2:
        st.markdown("### Top 10 Players by Points")
        dfp = filter_season(players)
        if not dfp.empty and "total_points" in dfp.columns:
            fig = px.bar(
                dfp.nlargest(10, "total_points"),
                x="web_name",
                y="total_points",
                color="CLUB",
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No player points data to plot")

    with col3:
        st.markdown("### Recent Matches")
        dfm = filter_season(matches)
        if not dfm.empty and "kickoff_time" in dfm.columns:
            recent = dfm.sort_values("kickoff_time", ascending=False).head(5)
            for _, r in recent.iterrows():
                st.markdown(
                    f"**{r.get('kickoff_time', '-')}** — {r.get('HOME_TEAM', '-')} {r.get('HOME_TEAM_GOALS', '-')} - {r.get('AWAY_TEAM_GOALS', '-')} {r.get('AWAY_TEAM', '-')}"
                )
        else:
            st.write("No matches available")

    st.markdown("---")
    st.subheader("Standings snapshot")
    if not standings.empty:
        st.dataframe(
            filter_season(standings)
            .sort_values(by=["Points", "GD"], ascending=[False, False])
            .reset_index(drop=True)
        )
    else:
        st.info("Standings not available")

# -----------------------------
# Standings Page
# -----------------------------
elif page == "Standings":
    st.markdown(
        """
    <style>
        h1, h2, h3 {
            color: #EEEBBC ; /* Arsenal Deep Navy */
            font-weight: 700;
        }
    </style>
    """,
        unsafe_allow_html=True,
    )
    st.header("League Standings")
    if standings.empty:
        st.info("Standings table not found in DB")
    else:
        df = filter_season(standings)
        df = df.fillna(0)
        float_cols = df.select_dtypes(include=["float"])

        df[float_cols.columns] = float_cols.astype(int)

        st.dataframe(
            df.sort_values(by=["Points", "GD"], ascending=[False, False])
            .reset_index(drop=True)
            .style.set_properties(
                **{
                    "background-color": "#879345",  # Light blue background
                    "color": "black",
                }
            )
        )

# -----------------------------
# Players Page
# -----------------------------
elif page == "Players":
    st.markdown(
        """
    <style>
        h1, h2, h3 {
            color: #EEEBBC ; /* Arsenal Deep Navy */
            font-weight: 700;
        }
    </style>
""",
        unsafe_allow_html=True,
    )
    st.header("Players")
    dfp = filter_season(players)
    if team != "All" and not dfp.empty:
        dfp = dfp[dfp["CLUB"] == team]

    if dfp.empty:
        st.info("No players data for the selected filters")
    else:
        # Search & metric selector
        left, right = st.columns([3, 1])
        with right:
            metric = st.selectbox(
                "Metric",
                [
                    "total_points",
                    "goals_scored",
                    "assists",
                    "minutes",
                    "Gameweek_Points",
                    "expected_goals",
                    "expected_assists",
                    "ict_index",
                ],
            )
            topn = st.slider("Top N", 5, 30, 10)
        with left:
            query = st.text_input("Search player (name)")

        if query:
            dfp = dfp[dfp["web_name"].str.contains(query, case=False, na=False)]

        # Table and chart
        st.dataframe(
            dfp.sort_values(by=metric, ascending=False)
            .head(200)
            .style.set_table_styles(
                [
                    {
                        "selector": "th",
                        "props": [
                            ("background-color", "#FFFFFF"),
                            ("color", "#041434"),
                            ("border-bottom", "2px solid #EF0107"),
                            ("font-weight", "bold"),
                            ("padding", "8px"),
                        ],
                    },
                ]
            )
            .set_properties(
                **{
                    "background-color": "#AFAEFF",
                    "color": "#041434",
                    "border": "1px solid #F0F0F0",
                    "padding": "6px",
                }
            )
        )
        fig = px.bar(
            dfp.nlargest(topn, metric),
            x="web_name",
            y=metric,
            color="CLUB",
            title=f"Top {topn} by {metric}",
        )
        st.plotly_chart(fig, use_container_width=True)

        # Download
        csv = dfp.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download players CSV", data=csv, file_name="players.csv", mime="text/csv"
        )

# -----------------------------
# Gameweek Page
# -----------------------------
elif page == "Gameweek":
    st.markdown(
        """
    <style>
        h1, h2, h3 {
            color: #EEEBBC ; /* Arsenal Deep Navy */
            font-weight: 700;
        }
    </style>
""",
        unsafe_allow_html=True,
    )
    st.header("Gameweek Info")
    df_gw = filter_season(gameweek_info)
    df_events = filter_season(events)
    df_chip = chip_played

    gw_choices = []
    if not df_events.empty and "id" in df_events.columns:
        gw_choices = sorted(df_events["id"].unique())
    else:
        gw_choices = list(range(1, 39))

    gw = st.selectbox("Select Gameweek", gw_choices, index=0)


    # pull safe values
    def safe_get(df, cond_col, cond_val, col_name):
        if df is None or df.empty:
            return None
        sel = df.loc[df[cond_col] == cond_val, col_name]
        return sel.iloc[0] if not sel.empty else None


    transfers_made = safe_get(df_events, "id", gw, "transfers_made")
    most_captained = safe_get(df_gw, "id", gw, "Most_Captained")
    most_transferred_in = safe_get(df_gw, "id", gw, "Most_Transferred_In")
    player_of_week = safe_get(df_gw, "id", gw, "Most_Points")
    average_points = safe_get(df_gw, "id", gw, "Average_Points")
    most_points = safe_get(df_gw, "id", gw, "Highest_Points")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown(
            f"""
            <div style="background-color:#000000;padding:15px;border-radius:10px;text-align:center;">
                <h3 style="color:#008000;">Transfers Made</h3>
                 <b>{transfers_made}</b>
            
            </div>
         """,
            unsafe_allow_html=True,
        )

    with col2:
        st.markdown(
            f"""
            <div style="background-color:#000000;padding:15px;border-radius:10px;text-align:center;">
                <h3 style="color:#FF4B4B;">Most Captained</h3>
                <b>{most_captained}</b>
            
            </div>
         """,
            unsafe_allow_html=True,
        )

    col3, col4 = st.columns(2)

    with col3:
        st.markdown(
            f"""
        <div style="background-color:#000000;padding:15px;border-radius:10px;text-align:center;">
            <h3 style="color:#1E90FF;">Most Transferred In</h3>
            <b>{most_transferred_in}</b>
           
        </div>
        """,
            unsafe_allow_html=True,
        )

    with col4:
        st.markdown(
            f"""
            <div style="background-color:#000000;padding:15px;border-radius:10px;text-align:center;">
                <h3 style="color:#1E90FF;">Player Of Week</h3>
                <b>{player_of_week}</b>
           
            </div>
            """,
            unsafe_allow_html=True,
        )

    coll1, coll2 = st.columns(2)

    with coll1:
        st.markdown(
            f"""
            <div style="background-color:#000000;padding:15px;border-radius:10px;text-align:center;">
                <h3 style="color:#008000;">Average Points</h3>
                <b>{average_points}</b>
            
            </div>
            """,
            unsafe_allow_html=True,
        )

    with coll2:
        st.markdown(
            f"""
            <div style="background-color:#000000;padding:15px;border-radius:10px;text-align:center;">
                <h3 style="color:#FF4B4B;">Most Points</h3>
                <b>{most_points}</b>
            
            </div>
            """,
            unsafe_allow_html=True,
        )

    result = df_chip.loc[
        (df_chip["id"] == gw) & (df_chip["chip_name"] == "3xc"), "num_played"
    ]

    triple_captain = result.iloc[0] if not result.empty else 0

    wildcard_played = df_chip.loc[
        (df_chip["id"] == gw) & (df_chip["chip_name"] == "wildcard"), "num_played"
    ]
    wildcard_played = wildcard_played.iloc[0] if not wildcard_played.empty else 0
    bboost = df_chip.loc[
        (df_chip["id"] == gw) & (df_chip["chip_name"] == "bboost"), "num_played"
    ].iloc[0]

    freehit = df_chip.loc[
        (df_chip["id"] == gw) & (df_chip["chip_name"] == "freehit"), "num_played"
    ]
    freehit = freehit.iloc[0] if not freehit.empty else 0

    col3, col4 = st.columns(2)

    with col3:
        st.markdown(
            f"""
            <div style="background-color:#000000;padding:15px;border-radius:10px;text-align:center;">
                <h3 style="color:#1E90FF;">Triple Captained Played</h3>
                <b>{triple_captain}</b>
               
            </div>
            """,
            unsafe_allow_html=True,
        )

    with col4:
        st.markdown(
            f"""
            <div style="background-color:#000000;padding:15px;border-radius:10px;text-align:center;">
                <h3 style="color:#1E90FF;">Wildcards Played</h3>
                <b>{wildcard_played if wildcard_played is not None else "-"}</b>
               
            </div>
            """,
            unsafe_allow_html=True,
        )

    coll1, coll2 = st.columns(2)

    with coll1:
        st.markdown(
            f"""
            <div style="background-color:#000000;padding:15px;border-radius:10px;text-align:center;">
                <h3 style="color:#008000;">Bench Boost</h3>
                 <b>{bboost}</b>
                
            </div>
            """,
            unsafe_allow_html=True,
        )

    with coll2:
        st.markdown(
            f"""
            <div style="background-color:#000000;padding:15px;border-radius:10px;text-align:center;">
                <h3 style="color:#FF4B4B;">Freehit</h3>
                <b>{freehit}</b>
                
            </div>
            """,
            unsafe_allow_html=True,
        )


    st.markdown("---")



# -----------------------------
# Injuries Page #041434
# ----------------------------
elif page == "Injuries":
    st.markdown(
        "<h2 style='color:#EEEBBC;'>Injury News 🔴</h2>", unsafe_allow_html=True
    )

    if injuries.empty:
        st.info("No recent injury news available")
    else:
        df_inj = filter_season(injuries)
        if team != "All":
            df_inj = df_inj[df_inj["CLUB"] == team]
        df_inj = df_inj.sort_values("NEWS_DATED", ascending=False).head(40)
        for _, r in df_inj.iterrows():
            st.markdown(
                dedent(f""" <div style="color:#FFFFFF; font-size:16px; line-height:1.55;">
                {r.get("NAME", "-")} ({r.get("CLUB", "-")}) — {r.get("NEWS", "-")}  
                {r.get("NEWS_DATED", "-")} </div>
                """),
                unsafe_allow_html=True,
            )

# -----------------------------
# Results Page
# -----------------------------
elif page == "Results":
    st.markdown(
        """
    <style>
        h1, h2, h3 {
            color: #EEEBBC ; /* Arsenal Deep Navy */
            font-weight: 700;
        }
    </style>
""",
        unsafe_allow_html=True,
    )
    st.header("Match Results")
    if matches.empty:
        st.info("No match results available")
    else:
        dfm = filter_season(matches)
        if team != "All":
            dfm = dfm[(dfm["HOME_TEAM"] == team) | (dfm["AWAY_TEAM"] == team)]
        dfm = dfm.sort_values("kickoff_time", ascending=False)
        st.dataframe(dfm)



elif page == "Fixtures":
    st.markdown(
        """
    <style>
        h1, h2, h3 {
            color: #EEEBBC ; /* Arsenal Deep Navy */
            font-weight: 700;
        }
    </style>
""",
        unsafe_allow_html=True,
    )
    st.header("Next Matches")
    # st.markdown("### Recent Matches")
    dfm = filter_season(fixtures)
    if team != "All" and not dfm.empty:
        dfm = dfm[(dfm["HOME_TEAM"] == team) | (dfm["AWAY_TEAM"] == team)]
    if not dfm.empty and "kickoff_time" in dfm.columns:
        recent = dfm.sort_values("kickoff_time", ascending=True).head(10)
        for _, r in recent.iterrows():
            st.markdown(
                f"GAMEWEEK {r.get('Gameweek', '-')} -  **{r.get('kickoff_time', '-')}** — {r.get('HOME_TEAM', '-')}  -  {r.get('AWAY_TEAM', '-')}"
            )
    else:
        st.write("No matches available")



# -----------------------------
# Transfers Page
# -----------------------------
elif page == "Transfers":
    st.header("Transfers")
    dfp = filter_season(players)
    if team != "All":
        dfp = dfp[dfp["CLUB"] == team]
    if dfp.empty:
        st.info("No transfer data available for this team/season")
    else:
        cols = st.columns(2)
        with cols[0]:
            st.subheader("Top Transfers In (event)")
            st.dataframe(
                dfp.sort_values("transfers_in_event", ascending=False).head(15)[
                    ["web_name", "CLUB", "transfers_in_event"]
                ]
            )
        with cols[1]:
            st.subheader("Top Transfers Out (event)")
            st.dataframe(
                dfp.sort_values("transfers_out_event", ascending=False).head(15)[
                    ["web_name", "CLUB", "transfers_out_event"]
                ]
            )


# -----------------------------
# Transfers Page
# -----------------------------
elif page == "Transfers":
    st.markdown(
        """
    <style>
        h1, h2, h3 {
            color: #EEEBBC ;
            font-weight: 700;
        }
    </style>
""",
        unsafe_allow_html=True,
    )
    st.header("Transfers")
    dfp = filter_season(players)
    if dfp.empty:
        st.info("No transfer data available")
    else:
        cols = st.columns(2)
        with cols[0]:
            st.subheader("Top Transfers In (event)")
            st.dataframe(
                dfp.sort_values("transfers_in_event", ascending=False).head(15)[
                    ["web_name", "CLUB", "transfers_in_event"]
                ]
            )
        with cols[1]:
            st.subheader("Top Transfers Out (event)")
            st.dataframe(
                dfp.sort_values("transfers_out_event", ascending=False)
                .head(15)[["web_name", "CLUB", "transfers_out_event"]]
                .style.set_table_styles(
                    [
                        {
                            "selector": "th",
                            "props": [
                                ("background-color", "#FFFFFF"),
                                ("color", "#041434"),
                                ("border-bottom", "2px solid #EF0107"),
                                ("font-weight", "bold"),
                                ("padding", "8px"),
                            ],
                        },
                    ]
                )
                .set_properties(
                    **{
                        "background-color": "#FFFFFF",
                        "color": "#041434",
                        "border": "1px solid #F0F0F0",
                        "padding": "6px",
                    }
                )
            )

# -----------------------------
# Footer
# -----------------------------
st.markdown("---")
