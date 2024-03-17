import hashlib
import psycopg2
from datetime import datetime, timedelta
from typing import List

import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu


st.set_page_config(
    page_title='ArcheRage Insights',
    page_icon='logo.ico',
    layout='wide',
    initial_sidebar_state='expanded'
)


def generate_hash(string):
    return hashlib.md5(string.encode()).hexdigest()


def connect_to_database():
    return psycopg2.connect(
        dbname="user_logs",
        user="adm",
        password="supersecret",
        host="db",
        port="5432"
    )


def create_tables(conn):
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                        user_hash TEXT PRIMARY KEY,
                        user_name TEXT,
                        faction TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS logs (
                        log_type TEXT,
                        time TEXT,
                        character TEXT,
                        receiver TEXT,
                        total INTEGER,
                        location TEXT,
                        log_id TEXT PRIMARY KEY,
                        character_id TEXT,
                        receiver_id TEXT)''')
    conn.commit()


def save_user_faction(conn, user_name, faction):
    cursor = conn.cursor()
    user_hash = generate_hash(user_name)
    cursor.execute("""
        INSERT INTO users (user_hash, user_name, faction) 
        VALUES (%s, %s, %s) 
        ON CONFLICT (user_hash) DO UPDATE SET faction = EXCLUDED.faction;
    """, (user_hash, user_name, faction))
    conn.commit()


def get_locations(_conn):
    cursor = _conn.cursor()
    cursor.execute("SELECT DISTINCT location FROM logs")
    locations = cursor.fetchall()
    return [loc[0] for loc in locations]


def summarize_logs_filtered(conn, faction_filter, location_filter, start_datetime, end_datetime, log_type_filter, only_pvp=True):
    cursor = conn.cursor()
    query = """
        SELECT logs.log_id, users.faction, logs.location, logs.log_type, logs.time, logs.character, logs.receiver, SUM(logs.total) AS total
        FROM logs 
        """
    filters = []
    faction = [] 
    if only_pvp is True:
        query += """ 
        JOIN users ON user_hash = logs.character_id AND faction <> 'Mob'
        JOIN users AS recv_users ON recv_users.user_hash = logs.receiver_id AND recv_users.faction <> 'Mob'"""
    else:
        query += """ 
        JOIN users ON user_hash = logs.character_id AND faction <> 'Mob' """
    if "*" in faction_filter:
        faction = ["East", "West", "Pirate"]
    else:
        for filter in faction_filter:
            if filter in ["East", "West", "Pirate"]:
                filters.append(faction)
    if faction_filter and len(faction) > 0:
        filters.append(f"users.faction IN {tuple(faction)}")
    if location_filter:
        if len(location_filter) == 1:
            filters.append(f"logs.location = '{location_filter[0]}'")
        else:
            filters.append(f"logs.location IN {tuple(location_filter)}")
    if start_datetime:
        filters.append(f"logs.time >= '{start_datetime}'")
    if end_datetime:
        filters.append(f"logs.time <= '{end_datetime}'")
    if log_type_filter:
        filters.append(f"logs.log_type = '{log_type_filter}'")

    if filters:
        query += " WHERE " + " AND ".join(filters)

    query += " GROUP BY logs.log_id, users.faction, logs.location, logs.log_type, logs.time, logs.character, logs.receiver"

    cursor.execute(query)
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=["Log ID", "Faction", "Location", "Log Type", "Time",  "Character",  "Target",  "Total"])

    return df



def summarize_logs_filtered_on_mobs(conn, faction_filter, location_filter, start_datetime, end_datetime, log_type_filter):
    cursor = conn.cursor()
    query = """
        SELECT logs.log_id, char_users.faction, logs.location, logs.log_type, logs.time, logs.character, 
	    logs.receiver, logs.total
        FROM logs
        JOIN users AS char_users ON char_users.user_hash = logs.character_id AND char_users.faction <> 'Mob'
        JOIN users AS recv_users ON recv_users.user_hash = logs.receiver_id
        """
    filters = []

    faction = []
    if "*" in faction_filter:
        faction = ["East", "West", "Pirate"]
    else:
        for filter in faction_filter:
            if filter in ["East", "West", "Pirate"]:
                filters.append(faction)
    if faction_filter and len(faction) > 0:
        filters.append(f"char_users.faction IN {tuple(faction)}")
    for filter in location_filter:
        if filter:
            filters.append(f"logs.location = %s")
    if start_datetime:
        filters.append(f"logs.time >= %s")
    if end_datetime:
        filters.append(f"logs.time <= %s")
    if log_type_filter:
        filters.append(f"logs.log_type = '{log_type_filter}'")
    if filters:
        query += " WHERE " + " AND ".join(filters)
    cursor.execute(query, [*location_filter, start_datetime, end_datetime]) 
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=["Log ID", "Faction", "Location", "Log Type", "Time", "Character", "Target", "Total"])
    return df


def format_number(n):
    suffixes = ['', 'k', 'M', 'B', 'T']
    i = 0
    while n >= 1000 and i < len(suffixes)-1:
        n /= 1000.0
        i += 1
    if i == 0:
        return str(int(n))
    else:
        return str('{:,.1f}{}'.format(n, suffixes[i]))


def get_totalizers(df):
    totalizers = df.groupby(["Faction", "Log Type"]).agg(
        Total=('Total', 'sum'),
        Unique_Players=('Character', 'nunique')
    ).reset_index()
    totalizers['Total'] = totalizers['Total'].apply(format_number)
    return totalizers


def get_default_start_time():
    current_date = datetime.now()
    default_start_time = current_date - timedelta(minutes=15)
    return default_start_time


def summarize_logs(conn, faction_filter, location_filter, start_datetime, end_datetime, log_type_filter=None, only_pvp=True):
    cursor = conn.cursor()
    query = """
        SELECT logs.log_id, users.faction, logs.location, logs.log_type, logs.time, logs.character, logs.receiver, SUM(logs.total) AS total
        FROM logs
        """
    filters = []
    factions = []
    if only_pvp is True:
        query += """ 
        JOIN users ON user_hash = logs.character_id AND faction <> 'Mob'
        JOIN users AS recv_users ON recv_users.user_hash = logs.receiver_id AND recv_users.faction <> 'Mob'"""
    else:
        query += """ 
        JOIN users ON user_hash = logs.character_id AND faction <> 'Mob' """
    for filter in faction_filter:
        if "*" in filter:
            factions = ['East', 'West', 'Pirate']
        else:
            factions.append(filter)
    if len(factions) > 0:
        if len(factions) == 1:
            filters.append(f"users.faction = '{factions[0]}'")
        else:
            filters.append(f"users.faction IN {tuple(factions)}")
    for filter in location_filter:
        if filter:
            filters.append(f"location = '{filter}'")
    if start_datetime:
        filters.append(f"time >= '{start_datetime}'")
    if end_datetime:
        filters.append(f"time <= '{end_datetime}'")
    if log_type_filter:
        filters.append(f"log_type = '{log_type_filter}'")
    if filters:
        query += " WHERE " + " AND ".join(filters)
    query += " GROUP BY logs.log_id, users.faction, logs.location, logs.log_type, logs.time, logs.character, logs.receiver"
    cursor.execute(query)
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=["Log ID", "Faction", "Location", "Log Type", "Time",  "Character",  "Target",  "Total"])
    return df


def get_total_counts(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM users")
    total_users = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM logs")
    total_logs = cursor.fetchone()[0]

    return total_users, total_logs


def summarize_logs_paginated(conn, faction_filter, location_filter, start_datetime, end_datetime, page_number, page_size, log_type, only_pvp):
    cursor = conn.cursor()
    query = """
        SELECT logs.log_id, users.faction, logs.location, logs.log_type, logs.time, logs.character, logs.receiver, SUM(logs.total) AS total
        FROM logs
        """ 
    filters = []
    factions = []
    if only_pvp is True:
        query += """ 
        JOIN users ON user_hash = logs.character_id AND faction <> 'Mob'
        JOIN users AS recv_users ON recv_users.user_hash = logs.receiver_id AND recv_users.faction <> 'Mob'"""
    else:
        query += """ 
        JOIN users ON user_hash = logs.character_id AND faction <> 'Mob' """
    for filter in faction_filter:
        if "*" in filter:
            factions = ['East', 'West', 'Pirate']
        else:
            factions.append(filter)
    if len(factions) > 0:
        if len(factions) == 1:
            filters.append(f"users.faction = '{factions[0]}'")
        else:
            filters.append(f"users.faction IN {tuple(factions)}")
    for filter in location_filter:
        if filter:
            filters.append(f"location = '{filter}'")
    if start_datetime:
        filters.append(f"time >= '{start_datetime}'")
    if end_datetime:
        filters.append(f"time <= '{end_datetime}'")
    if log_type:
        filters.append(f"log_type = '{log_type}'")
    if filters:
        query += " WHERE " + " AND ".join(filters)

    offset = (page_number - 1) * page_size

    query += f" GROUP BY logs.log_id, users.faction, logs.location, logs.log_type, logs.time, logs.character, logs.receiver ORDER BY logs.time DESC LIMIT {page_size} OFFSET {offset}"

    cursor.execute(query)
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=["Log ID", "Faction", "Location", "Log Type", "Time",  "Character",  "Target",  "Total"])
    return df


def paginate(page, page_size):
    page = max(1, page) 
    offset = (page - 1) * page_size
    return page, offset


def query_users_by_faction(conn, faction_filter, location_filter, start_datetime, end_datetime):
    cursor = conn.cursor()
    query = "SELECT DISTINCT logs.character, logs.time FROM logs JOIN users ON logs.character_id = users.user_hash WHERE 1=1"
    filters = []
    faction = []
    if "*" in faction_filter:
        faction = ["East", "West", "Pirate"]
    else:
        for filter in faction_filter:
            if filter in ["East", "West", "Pirate"]:
                filters.append(faction)
    if faction_filter and len(faction) > 0:
        filters.append(f"users.faction IN {tuple(faction)}")
    for filter in location_filter:
        if filter:
            filters.append(f"location = '{filter}'")
    if start_datetime:
        filters.append(f"time >= '{start_datetime}'")
    if end_datetime:
        filters.append(f"time <= '{end_datetime}'")
    if filters:
        query += " AND " + " AND ".join(filters)
    query += " ORDER BY time"
    cursor.execute(query)
    return cursor.fetchall()


def create_report_filter_sidebar(locations: List[str]):
    filter_sidebar = st.sidebar
    filter_sidebar.title("Report filters")
    sidebar_fields = {}
    sidebar_fields['location_filter'] = filter_sidebar.multiselect(
        "Select Location", [""] + locations, [])
    sidebar_fields['faction_filter'] = filter_sidebar.multiselect(
        "Select Faction", ["East", "West", "Pirate", "*"], "*")
    sidebar_fields['start_date'] = filter_sidebar.date_input(
        "Start Date", get_default_start_time())
    sidebar_fields['start_time'] = filter_sidebar.time_input("Start Time", step=300, value=get_default_start_time())
    sidebar_fields['end_date'] = filter_sidebar.date_input("End Date")
    sidebar_fields['end_time'] = filter_sidebar.time_input("End Time", step=300)
    return filter_sidebar, sidebar_fields


def get_top_users_by_faction(cursor, log_type, faction_filter, location_filter=[], start_datetime=None, end_datetime=None):
    factions = []
    if "*" in faction_filter:
        factions = ['East', 'West', 'Pirate']
    else:
        factions.append(filter)
    top_users_by_faction = {}
    for faction in factions:
        query = """SELECT users.user_name, COUNT(*), SUM(logs.total) as t FROM logs JOIN users ON logs.character_id = users.user_hash """
        filters = [f"logs.log_type = '{log_type}'"]
        if log_type == "Damage":
           query += "JOIN users AS recv_users ON recv_users.user_hash = logs.receiver_id "
           filters.append("recv_users.faction <> 'Mob'")
        
        filters.append(f"users.faction = '{faction}'")
        if len(location_filter) > 0:
            for location in location_filter:
                if location:
                    filters.append(f"logs.location = '{location}'")
        if start_datetime:
            filters.append(f"logs.time >= '{start_datetime}'")
        if end_datetime:
            filters.append(f"logs.time <= '{end_datetime}'")
        if filters:
            query += " WHERE " + " AND ".join(filters)
        query += " GROUP BY users.user_name ORDER BY t DESC LIMIT 20"
        cursor.execute(query)
        top_users_by_faction[faction] = cursor.fetchall()
    return top_users_by_faction

def get_users(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users")
    user_data = cursor.fetchall()
    df_user = pd.DataFrame(user_data, columns=[
                            "User Hash", "User Name", "Faction"])
    return df_user


def get_users_filtered(conn, faction_filter, name_filter):
    filters = []
    factions = []
    empty_filter = False
    all_factions = False
    if len(faction_filter) > 0:
        if "Empty" in faction_filter:
            empty_filter = True
        if "*" not in faction_filter:
            for faction in faction_filter:
                factions.append(faction)
        if "*" in faction_filter:
            all_factions = True
    cursor = conn.cursor()
    query = "SELECT * FROM users WHERE 1=1"
    if name_filter != '':
        filters.append(f"user_name = '{name_filter}'")
    elif len(factions) > 0 and not empty_filter and not all_factions:
        if len(factions) == 1:
            filters.append(f"faction = '{factions[0]}'")
        elif len(factions) > 1:
            filters.append(f"faction IN {tuple(factions)}")
    elif empty_filter is True:
        filters.append("faction IS NULL")
    if len(filters) > 0:
        query += " AND " + " AND ".join(filters)
        query += " order by faction desc, user_name"
    cursor.execute(query)
    user_data = cursor.fetchall()
    df_user = pd.DataFrame(user_data, columns=[
                            "User Hash", "User Name", "Faction"])
    return df_user

def check_users_faction(conn):
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT 
            COUNT(CASE WHEN faction = 'East' THEN 1 END) AS East_Count,
            COUNT(CASE WHEN faction = 'West' THEN 1 END) AS West_Count,
            COUNT(CASE WHEN faction = 'Pirate' THEN 1 END) AS Pirate_Count,
            COUNT(CASE WHEN faction = 'Mob' THEN 1 END) AS Mob_Count,
            COUNT(CASE WHEN faction is null THEN 1 END) AS Empty_Count
        FROM users;
    """
    )
    result = cursor.fetchall()
    if len(result) > 0:
        result = dict(
            east=result[0][0],
            west=result[0][1],
            pirate=result[0][2],
            mob=result[0][3],
            empty=result[0][4],
            asv=0,
        )
        return result
    return {}

def error_faction_modal():
    st.error('#### ⚠️ You need to set at least one user for each faction to see other reports properlly. Go to *users* -> *user table* page and set at least one user for each faction.')
        
def validate_users_in_factions(conn):
    with st.container():
        faction_info = check_users_faction(conn)
        if faction_info.get('mob') == 0 or faction_info.get('east') == 0 or faction_info.get('west') == 0 or faction_info.get('pirate') == 0:
            error_faction_modal()

def main():
    conn = connect_to_database()
    create_tables(conn)
    locations = get_locations(conn)
    page = option_menu(
        menu_title="",
        options=["Main", "Users", "Logs"],
        default_index=0,
        orientation='horizontal'
    )

    if page == "Main":
        validate_users_in_factions(conn)
        
        with st.container():
            left_co, cent_co,last_co = st.columns(3)
            with left_co:
                st.image("logo.png", width=300)
            with cent_co:
                total_users, total_logs = get_total_counts(conn)

                st.write("### Your current database overview")
                st.write(f"##### - Total users: {total_users}.")
                st.write(f"##### - Total logs: {total_logs}.")

    elif page == "Users":
        report_option = st.selectbox('Select a report', ['User table', 'Faction distribution', 'User logs by location',
                                     'Attendence'], index=0, placeholder="Choose an option", disabled=False, label_visibility="visible")
        if report_option == 'User table':
            st.sidebar.header("Save User Faction")
            user_name = st.sidebar.text_input("Enter User Name")
            faction = st.sidebar.selectbox(
                "Select Faction", ["East", "West", "Pirate", 'Mob'])

            if user_name:
                if st.sidebar.button("Save Faction"):
                    save_user_faction(conn, user_name.rstrip(), faction)
                    st.sidebar.success("Faction saved successfully!")
            
            with st.container():
                st.subheader("User data")
                col_faction, col_user = st.columns(2)
                with col_faction:
                    faction_filter = st.multiselect(
                        "Select Faction", 
                        ["East", "West", "Pirate", "*", "Empty"],
                        "*",
                    )
                with col_user:
                    user_name = st.text_input(label='User name', value='', placeholder='Enter a user name')
                df_users = get_users_filtered(conn, faction_filter, user_name)
                st.table(df_users)
        elif report_option == 'Faction distribution':
            df_user = get_users(conn)
            with st.container():
                st.write("### User Faction Distribution")
                total_users = len(df_user['User Hash'])
                st.write(f"##### Total unique users: {total_users}.")

                df1 = df_user['Faction'].value_counts()
                st.bar_chart(data=df1)
        elif report_option == 'User logs by location':
            _, sidebar_fields = create_report_filter_sidebar(locations)
            start_datetime = None if not sidebar_fields[
                'start_date'] else f"{sidebar_fields['start_date']} {sidebar_fields['start_time']}"
            end_datetime = None if not sidebar_fields[
                'end_date'] else f"{sidebar_fields['end_date']} {sidebar_fields['end_time']}"

            cursor = conn.cursor()
            filters = []
            if '*' in sidebar_fields['faction_filter']:
                factions = ["East", "West", "Pirate"]
                filters.append(f"faction IN {tuple(factions)}") 
            elif len(sidebar_fields['faction_filter']) > 1:
                filters.append(f"faction IN {tuple(sidebar_fields['faction_filter'])}")
            else:
                filters.append(f"faction = '{sidebar_fields['faction_filter'][0]}'")
            
            if len(sidebar_fields['location_filter']) > 0:
                for location in sidebar_fields['location_filter']:
                    if location:
                        filters.append(f"logs.location = '{location}'")
            if start_datetime:
                filters.append(f"logs.time >= '{start_datetime}'")
            if end_datetime:
                filters.append(f"logs.time <= '{end_datetime}'")
            filters.append("faction <> 'Mob'")
            if filters:
                query = """
                    SELECT DISTINCT users.user_name, users.faction, logs.location
                    FROM users
                    JOIN logs ON users.user_hash = logs.character_id OR users.user_hash = logs.receiver_id
                    WHERE """ + " AND ".join(filters)
                cursor.execute(query)
                user_logs = cursor.fetchall()
                user_logs_df = pd.DataFrame(
                    user_logs, columns=["User Name", "Faction", "Location"])

                df_count_by_faction = user_logs_df['Faction'].value_counts()
                with st.container():
                    total_users_by_faction = dict(East=0, West=0, Pirate=0)

                    for faction, count in df_count_by_faction.items():
                        total_users_by_faction[faction] = count

                    east, west, pirate = st.columns(3)
                    with east:
                        st.subheader("East")
                        st.metric(label="Total", value=str(
                            total_users_by_faction.get("East")))
                    with west:
                        st.subheader("West")
                        st.metric(label="Total", value=str(
                            total_users_by_faction.get("West")))
                    with pirate:
                        st.subheader("Pirate")
                        st.metric(label="Total", value=str(
                            total_users_by_faction.get("Pirate")))

                    st.subheader("User Logs by Location and Faction")
                    with st.container():
                        if not user_logs_df.empty:
                            st.table(user_logs_df)
                        else:
                            st.write(
                                "No data available for the selected filters.")
        elif report_option == 'Attendence':
            _, sidebar_fields = create_report_filter_sidebar(locations)
            start_datetime = None if not sidebar_fields[
                'start_date'] else f"{sidebar_fields['start_date']} {sidebar_fields['start_time']}"
            end_datetime = None if not sidebar_fields[
                'end_date'] else f"{sidebar_fields['end_date']} {sidebar_fields['end_time']}"
            time_count = {}
            factions = sidebar_fields['faction_filter']
            if "*" in sidebar_fields['faction_filter']:
                factions = ['East', 'West', 'Pirate']
            for faction in factions:
                data = query_users_by_faction(
                    conn, faction, sidebar_fields['location_filter'], start_datetime, end_datetime
                )
                for row in data:
                    time = datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S')
                    if time in time_count:
                        if faction in time_count[time]:
                            time_count[time][faction] += 1
                        else:
                            time_count[time][faction] = 1
                    else:
                        time_count[time] = {faction: 1}

            plot_data = pd.DataFrame.from_dict(time_count, orient='index')
            plot_data.index = pd.to_datetime(plot_data.index)
            st.subheader('Body count by faction')
            st.bar_chart(plot_data, use_container_width=True)

    elif page == "Logs":
        _, sidebar_fields = create_report_filter_sidebar(locations)
        start_datetime = None if not sidebar_fields[
            'start_date'] else f"{sidebar_fields['start_date']} {sidebar_fields['start_time']}"
        end_datetime = None if not sidebar_fields[
            'end_date'] else f"{sidebar_fields['end_date']} {sidebar_fields['end_time']}"

        report_option = st.selectbox('Select a report', ['Overview', 'Pvp damage', 'Heals', 'Pve damage',
                                     'Top users by faction'], index=0, placeholder="Choose an option", disabled=False)
        if report_option == 'Overview':
            logs_summary = summarize_logs(
                conn, 
                sidebar_fields['faction_filter'],
                sidebar_fields['location_filter'], 
                start_datetime, 
                end_datetime,
                log_type_filter=None,
                only_pvp=True
            )
            totalizers = get_totalizers(logs_summary)

            st.write("## Totalizers")
            st.table(totalizers)

            st.write("## Logs")
            column_page_number, column_page_size, column_page_navigation = st.columns(
                3)
            with column_page_number:
                page = st.number_input("Page", 1, step=1, value=1)
            with column_page_size:
                page_size = st.number_input("Page Size", 1, step=1, value=20)
            with column_page_navigation:
                prev_page, _ = paginate(page - 1, page_size)
                next_page, _ = paginate(page + 1, page_size)

                st.write('Paginated view of logs navigation:')
                column_prev, column_next = st.columns(2)
                with column_prev:
                    if st.button('Prev'):
                        page, _ = paginate(page - 1, page_size)
                with column_next:
                    if st.button('Next'):
                        page, _ = paginate(page + 1, page_size)

            # Get logs for current page
            logs_table = summarize_logs_paginated(
                conn=conn, 
                faction_filter=sidebar_fields['faction_filter'],
                location_filter=sidebar_fields['location_filter'], 
                start_datetime=start_datetime, 
                end_datetime=end_datetime, 
                page_number=page, 
                page_size=page_size, 
                log_type=None,
                only_pvp=False
            )
            
            st.table(logs_table)

        elif report_option == "Pvp damage":
            st.write("### PVP Damage by Faction")
            st.write("Timechart")
            dmg_df = summarize_logs_filtered(conn, sidebar_fields['faction_filter'],
                                             sidebar_fields['location_filter'], start_datetime, end_datetime, 'Damage')
            if not dmg_df.empty:
                dmg_df['Time'] = pd.to_datetime(dmg_df['Time'])
                dmg_df = dmg_df.groupby(['Faction', pd.Grouper(key='Time')])[
                    'Total'].sum().reset_index()
                st.bar_chart(dmg_df, x='Time', y='Total',
                             color='Faction', use_container_width=True)
                
                column_page_number, column_page_size, column_page_navigation = st.columns(
                3)
                with column_page_number:
                    page = st.number_input("Page", 1, step=1, value=1)
                with column_page_size:
                    page_size = st.number_input("Page Size", 1, step=1, value=20)
                with column_page_navigation:
                    prev_page, _ = paginate(page - 1, page_size)
                    next_page, _ = paginate(page + 1, page_size)

                    st.write('Paginated view of logs navigation:')
                    column_prev, column_next = st.columns(2)
                    with column_prev:
                        if st.button('Prev'):
                            page, _ = paginate(page - 1, page_size)
                    with column_next:
                        if st.button('Next'):
                            page, _ = paginate(page + 1, page_size)
                table = summarize_logs_paginated(
                    conn=conn, 
                    faction_filter=sidebar_fields['faction_filter'],
                    location_filter=sidebar_fields['location_filter'], 
                    start_datetime=start_datetime, 
                    end_datetime=end_datetime, 
                    page_number=page, 
                    page_size=page_size, 
                    log_type='Damage',
                    only_pvp=True
                )
                st.table(table)

        elif report_option == "Heals":
            st.write("### Heal to Players by Faction")
            st.write("Timechart")
            heal_df = summarize_logs_filtered(conn, sidebar_fields['faction_filter'],
                                              sidebar_fields['location_filter'], start_datetime, end_datetime, 'Heal')
            if not heal_df.empty:
                heal_df['Time'] = pd.to_datetime(
                    heal_df['Time'])
                heal_df = heal_df.groupby(['Faction', pd.Grouper(key='Time')])[
                    'Total'].sum().reset_index()
                st.bar_chart(heal_df, x='Time', y='Total',
                             color='Faction', use_container_width=True)
                
                column_page_number, column_page_size, column_page_navigation = st.columns(
                3)
                with column_page_number:
                    page = st.number_input("Page", 1, step=1, value=1)
                with column_page_size:
                    page_size = st.number_input("Page Size", 1, step=1, value=20)
                with column_page_navigation:
                    prev_page, _ = paginate(page - 1, page_size)
                    next_page, _ = paginate(page + 1, page_size)

                    st.write('Paginated view of logs navigation:')
                    column_prev, column_next = st.columns(2)
                    with column_prev:
                        if st.button('Prev'):
                            page, _ = paginate(page - 1, page_size)
                    with column_next:
                        if st.button('Next'):
                            page, _ = paginate(page + 1, page_size)
                table = summarize_logs_paginated(
                    conn=conn, 
                    faction_filter=sidebar_fields['faction_filter'],
                    location_filter=sidebar_fields['location_filter'], 
                    start_datetime=start_datetime, 
                    end_datetime=end_datetime, 
                    page_number=page, 
                    page_size=page_size, 
                    log_type='Heal',
                    only_pvp=True
                )
                st.table(table)
                
        elif report_option == "Pve damage":
            st.write("### Pve Damage by Faction")
            st.write("Timechart")
            pve_df = summarize_logs_filtered_on_mobs(
                conn, sidebar_fields['faction_filter'], sidebar_fields['location_filter'], start_datetime, end_datetime, 'Damage')
            if not pve_df.empty:
                pve_df['Time'] = pd.to_datetime(
                    pve_df['Time'])
                pve_df = pve_df.groupby(['Faction', pd.Grouper(key='Time')])[
                    'Total'].sum().reset_index()
                st.bar_chart(pve_df, x='Time', y='Total',
                             color='Faction', use_container_width=True)
                
                column_page_number, column_page_size, column_page_navigation = st.columns(
                3)
                with column_page_number:
                    page = st.number_input("Page", 1, step=1, value=1)
                with column_page_size:
                    page_size = st.number_input("Page Size", 1, step=1, value=20)
                with column_page_navigation:
                    prev_page, _ = paginate(page - 1, page_size)
                    next_page, _ = paginate(page + 1, page_size)

                    st.write('Paginated view of logs navigation:')
                    column_prev, column_next = st.columns(2)
                    with column_prev:
                        if st.button('Prev'):
                            page, _ = paginate(page - 1, page_size)
                    with column_next:
                        if st.button('Next'):
                            page, _ = paginate(page + 1, page_size)
                table = summarize_logs_paginated(
                    conn=conn, 
                    faction_filter=sidebar_fields['faction_filter'],
                    location_filter=sidebar_fields['location_filter'], 
                    start_datetime=start_datetime, 
                    end_datetime=end_datetime, 
                    page_number=page, 
                    page_size=page_size, 
                    log_type='Damage',
                    only_pvp=False
                )
                st.table(table)

        elif report_option == "Top users by faction":
            st.title("Top users by faction")

            log_types = ['Heal', 'Damage']
            selected_log_type = st.selectbox(
                "Select log type:", log_types, index=1)

            cursor = conn.cursor()
            top_users_by_faction = get_top_users_by_faction(
                cursor,
                selected_log_type,
                sidebar_fields['faction_filter'],
                sidebar_fields['location_filter'],
                start_datetime,
                end_datetime
            )

            for faction, top_users in top_users_by_faction.items():
                st.subheader(f"Top 20 users of {faction}.")
                table_data = [(f"{i}. {user_name}", log_count, t)
                              for i, (user_name, log_count, t) in enumerate(top_users, 1)]
                df_top_pvp = pd.DataFrame(
                    table_data, columns=["User", "Log Count", "Total"])
                st.table(df_top_pvp)
        
    conn.close()


if __name__ == "__main__":
    main()
