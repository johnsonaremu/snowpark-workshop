# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from datetime import date
######################## Application Logic ########################

# Grab the current credentials
session = get_active_session()



# Query table for data
#query = """select * from nhl order by conference, division, pts desc, gp   """
#sql_data = session.sql(query)

query = """call nhl_standing(\'"""+date.today().strftime('%Y-%m-%d')+"""\')   """

sql_data = session.sql(query)
# Execute the query and then convert the results to a Pandas DataFrame

sql_data = session.sql(query)

# Select the columns you want for your data analysis
sql_data = sql_data[['CONFERENCE', 'DIVISION', 'TEAM', 'TEAM_ABBV', 'GP', 'W', 'L', 'OT', 'PTS', 'PTS_PCT']]

# Filter the df for conferences
ec = sql_data[sql_data['CONFERENCE']=='Eastern']
wc = sql_data[sql_data['CONFERENCE']=='Western']

# Filter the df for divisions
md = sql_data[sql_data['DIVISION']=='Metropolitan']
ad = sql_data[sql_data['DIVISION']=='Atlantic']
cd = sql_data[sql_data['DIVISION']=='Central']
pd = sql_data[sql_data['DIVISION']=='Pacific']

######################## Streamlit Application ########################
# Write directly to the app

# Page Title
st.title('NHL Overview of this year Season (YTD)')

# Single Page Layout
tab1, tab2, tab3 = st.tabs(['League', 'Conference', 'Division'])

# League
with tab1:
    st.bar_chart(data=sql_data, x="TEAM_ABBV", y="W")
    st.dataframe(sql_data, use_container_width=True)

# Conference
with tab2:
    # Tab2 Layout: Tabs
    tab4, tab5 = st.tabs(['Eastern', 'Western'])

    # Eastern Conference
    with tab4:
        st.bar_chart(data=ec, x="TEAM_ABBV", y="W")
        st.dataframe(ec, use_container_width=True)
    # Western Conference
    with tab5:
        st.bar_chart(data=wc, x="TEAM_ABBV", y="W")
        st.dataframe(wc, use_container_width=True)

# Division
with tab3:
    # Tab3 Layout: Tabs
    tab6, tab7, tab8, tab9 = st.tabs(['Metropolitan', 'Atlantic', 'Central', 'Pacific'])

    # Metropolitan Division
    with tab6:
        st.bar_chart(data=md, x="TEAM_ABBV", y="W")
        st.dataframe(md, use_container_width=True)

    # Atlantic Division
    with tab7:
        st.bar_chart(data=ad, x="TEAM_ABBV", y="W")
        st.dataframe(ad, use_container_width=True)

    # Central Division
    with tab8:
        st.bar_chart(data=cd, x="TEAM_ABBV", y="W")
        st.dataframe(cd, use_container_width=True)

    # Pacific Division
    with tab9:
        st.bar_chart(data=pd, x="TEAM_ABBV", y="W")
        st.dataframe(pd, use_container_width=True)
