import streamlit as st
import duckdb
st.title("🏆 Football Dashboard")

# Sidebar filters
st.sidebar.header("Filters")
gameweek = st.sidebar.selectbox("Gameweek", [1, 2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,
                                           29,30,31,32,33,34,35,6,37,38], index=3)

col1, col2, col3, col4= st.columns(4)
conn = duckdb.connect("epl_duckdb.duckdb", read_only=True)
df = conn.execute("""select * from dim_gameweek_info order by id desc""").df()
df1 = conn.execute("""select transfers_made, Season, finished, id from dim_events 
                    order by id desc ; """).df()
# print(df.columns)
df1 = df1[df1["Season"] == '2025-2026']
df = df[df["Season"] == '2025-2026']
transfers_made = df1.loc[df1['id'] == gameweek , 'transfers_made'].values[0]
most_captained = df.loc[df['id'] == gameweek, 'Most_Captained'].values[0]
most_transferred_in = df.loc[df['id'] == gameweek, 'Most_Transferred_In'].values[0]
player_of_Week = df.loc[df['id'] == gameweek, 'Most_Points'].values[0]
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

st.subheader("Team of Week")
st.dataframe(df_team)