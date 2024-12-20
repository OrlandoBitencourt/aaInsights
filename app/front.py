import hashlib
import re
import psycopg2
from datetime import datetime, timedelta
from typing import List

import pandas as pd
import pytz
import streamlit as st
from streamlit_option_menu import option_menu
from streamlit_extras.metric_cards import style_metric_cards
from streamlit_extras.dataframe_explorer import dataframe_explorer
from io import StringIO
import plotly.figure_factory as ff
import plotly.colors


DEFAULT_TIMEZONE = 'America/Sao_Paulo'


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
    cursor.execute('''CREATE TABLE IF NOT EXISTS location_logs (
                    location_hash TEXT PRIMARY KEY,
                    location TEXT,
                    enter TEXT,
                    exit TEXT
                )''')
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
        for f in faction_filter:
            if f in ["East", "West", "Pirate"]:
                faction.append(f)
    if faction_filter and len(faction) > 1:
        filters.append(f"users.faction IN {tuple(faction)}")
    elif faction_filter and len(faction) == 1:
        filters.append(f"users.faction = '{faction[0]}'")
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
        for f in faction_filter:
            if f in ["East", "West", "Pirate"]:
                faction.append(f)
    if faction_filter and len(faction) > 1:
        filters.append(f"char_users.faction IN {tuple(faction)}")
    elif faction_filter and len(faction) == 1:
        filters.append(f"char_users.faction = '{faction[0]}'")
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


def create_report_filter_sidebar(locations: List[str], faction=True):
    filter_sidebar = st.sidebar
    filter_sidebar.title("Report filters")
    sidebar_fields = {}
    sidebar_fields['location_filter'] = filter_sidebar.multiselect(
        "Select Location", [""] + locations, [])
    if faction:
        sidebar_fields['faction_filter'] = filter_sidebar.multiselect(
            "Select Faction", ["East", "West", "Pirate", "*"], "*")
    sidebar_fields['start_date'] = filter_sidebar.date_input(
        "Start Date")
    sidebar_fields['start_time'] = filter_sidebar.time_input("Start Time", step=300)
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

def convert_timezone(timestamp, from_tz, to_tz):
    timestamp = timestamp.replace(tzinfo=None)
    from_zone = pytz.timezone(from_tz)
    to_zone = pytz.timezone(to_tz)
    timestamp = from_zone.localize(timestamp).astimezone(to_zone)
    return timestamp

def parse_combat(log_file, start_time=None, end_time=None, target_name=None):
    """
    Parses combat logs.
    """
    combat_logs = []
    damage_regex = re.compile(r"<(?P<log_time_str>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})(?P<character>.*?)\|r attacked (?P<receiver>.*?)\|r using \|cff25fcff(.*?)\|r and caused \|cffff0000\-(?P<total>\d+)")
    heal_regex = re.compile(r"<(?P<log_time_str>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})(?P<character>.*?)\|r targeted (?P<receiver>[^|]+)\|[^|]+\|cff25fcff(?P<ability>[^|]+)\|[^|]+\|cff00ff00(?P<restored>[^|]+)\|r health.")

    #with open(log_file, "r", encoding="utf8") as file:
    for line in log_file.splitlines():
        try:
            if "attacked" in line:
                match_damage = damage_regex.search(line)
                if match_damage:
                    log_time_str, character, receiver, _, total = match_damage.groups()
                    log_type = "Damage"
                    timestamp = datetime.strptime(log_time_str, '%Y-%m-%d %H:%M:%S')
                    combat_logs.append((log_type, timestamp, character.strip(), receiver.strip(), total.strip()))
                    continue
            
            if "targeted" in line:
                match_heal = heal_regex.search(line)
                if match_heal:
                    log_time_str, character, receiver, _, restored = match_heal.groups()
                    log_type = "Heal"
                    timestamp = datetime.strptime(log_time_str, '%Y-%m-%d %H:%M:%S')
                    combat_logs.append((log_type, timestamp, character.strip(), receiver.strip(), restored.strip()))
                    continue
        except UnicodeDecodeError:
            pass
    
    # Filter logs based on start_time, end_time, and target_name
    if start_time:
        combat_logs = [log for log in combat_logs if log[1] >= start_time]
    if end_time:
        combat_logs = [log for log in combat_logs if log[1] <= end_time]
    if target_name:
        combat_logs = [log for log in combat_logs if log[3] == target_name]

    return combat_logs

def parse_location(misc_log_file):
    """
    Parses location logs.
    """
    location_logs = {}
    regex_enter = r"<(?P<log_timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})Entering Chat: \d+\.(?P<filter>Shout)\. (?P<log_location>[\w\s]+)"
    regex_leave = r"<(?P<log_timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})Leaving Chat: \d+\.(?P<filter>Shout)\. (?P<log_location>[\w\s]+)"

    #with open(misc_log_file, "r", encoding='ISO-8859-1') as file:
    for line in misc_log_file.splitlines():
        try:
            if line.startswith('BackupNameAttachment'):
                continue
            match_enter = re.match(regex_enter, line)
            if match_enter:
                log_timestamp, _, log_location = match_enter.groups()
                timestamp = datetime.strptime(log_timestamp, '%Y-%m-%d %H:%M:%S')
                log_location = log_location.strip()  # Remove leading and trailing whitespace
                if log_location in location_logs:
                    location_logs[log_location]['enter'] = timestamp
                else:
                    location_logs[log_location] = {'enter': timestamp}
                continue
            
            match_leave = re.match(regex_leave, line)
            if match_leave:
                log_timestamp, _, log_location = match_leave.groups()
                timestamp = datetime.strptime(log_timestamp, '%Y-%m-%d %H:%M:%S')
                log_location = log_location.strip()  # Remove leading and trailing whitespace
                if log_location in location_logs:
                    location_logs[log_location]['exit'] = timestamp
                else:
                    location_logs[log_location] = {'exit': timestamp}
        except UnicodeDecodeError:
            pass
    return location_logs

def merge_logs(combat_log_file, misc_log_file):
    """
    Merges combat and location logs.
    """
    location_logs = parse_location(misc_log_file)
    combat_logs = parse_combat(combat_log_file)

    merged_logs = []
    for combat_log in combat_logs:
        log_time = combat_log[1]
        for location, times in location_logs.items():
            if is_within_duration(log_time, times.get('enter'), times.get('exit')):
                merged_logs.append(combat_log + (location,))
                break

    return merged_logs


def insert_batch_user_data(conn, batch_users):
    cursor = conn.cursor()
    try:
        args_str = ','.join(cursor.mogrify("(%s,%s)", x).decode() for x in batch_users)
        insert_query = "INSERT INTO users (user_hash, user_name) VALUES " + args_str + " ON CONFLICT (user_hash) DO NOTHING;"
        cursor.execute(insert_query)
        conn.commit()
    except psycopg2.Error as e:
        print("Error inserting batch user data:", e)
        conn.rollback()
        
def is_within_duration(log_time, enter_time, exit_time):
    """
    Checks if a log time falls within the duration of entry and exit time for a location.
    """
    if enter_time is None or exit_time is None:
        return False
    return enter_time <= log_time <= exit_time

def insert_batch_log_data_single(conn, batch_logs):
    cursor = conn.cursor()
    try:
        args_str = ','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s)", x).decode() for x in batch_logs)
        insert_query = "INSERT INTO logs (log_type, time, character, receiver, total, location, log_id, character_id, receiver_id) VALUES " + args_str + " ON CONFLICT (log_id) DO NOTHING;"
        cursor.execute(insert_query)
        conn.commit()
    except psycopg2.Error as e:
        print("Error inserting batch log data:", e)
        conn.rollback()

def import_logs(combat_log_file, misc_log_file, log_timezone, db_timezone, db_connection):
    now = datetime.now()
    st.write(f"> {now.strftime('%Y-%m-%d %H:%M:%S')} : importing logs.")
    merged_logs = merge_logs(combat_log_file, misc_log_file)

    # Initialize batches for user data and log data
    batch_users = set()
    batch_logs = []

    for l in merged_logs:
        log = list(l)
        log_time = convert_timezone(log[1], log_timezone, db_timezone)
        log[1] = str(log_time.strftime('%Y-%m-%d %H:%M:%S'))
        log_data = (log[0], log[1], log[2], log[3], int(log[4]), log[5], generate_hash(",".join(log)), generate_hash(log[2]), generate_hash(log[3]))
        batch_logs.append(log_data)
        batch_users.add((log_data[7], log_data[2]))  # Add user data to the batch_users set

    try:
        conn = connect_to_database()
        with conn:
            insert_batch_user_data(conn, batch_users)
            insert_batch_log_data_single(conn, batch_logs)
    except Exception as e:
        st.error(f"Error importing logs: {e}")
    else:
        now = datetime.now()
        st.write(f"> {now.strftime('%Y-%m-%d %H:%M:%S')} : finished.")
        
def calculate_user_faction_percentage(faction_counts):
    total_users = sum(faction_counts.values())
    faction_percentages = {}
    
    for faction, count in faction_counts.items():
        if total_users == 0:
            faction_percentages[faction] = 0
            continue
        percentage = (count / total_users) * 100
        faction_percentages[faction] = percentage
    
    return faction_percentages


def calculate_continuous_presence(group):
    start_time = group['time'].iloc[0]
    end_time = group['time'].iloc[-1]
    return pd.Series({'start_time': start_time, 'end_time': end_time})


def main():
    conn = connect_to_database()
    create_tables(conn)
    locations = get_locations(conn)
    page = option_menu(
        menu_title="",
        options=["🛸 Main", "🐒 Users", "📑 Logs", "💾 Import"],
        default_index=0,
        orientation='horizontal'
    )
    
    if page == "🛸 Main":
        validate_users_in_factions(conn)
        
        with st.container():
            left_co, cent_co, last_co = st.columns(3)
            with left_co:
                st.image("logo300x300_test.png", width=250, caption="ArcheRage Insights")
                _left_co, _cent_co, _last_co = st.columns(3)
                with _left_co:
                    st.image("barcode_logo.png", width=80, caption="Barcode™️")
                with _cent_co:
                    st.image("united_east_logo.png", width=80, caption="United East™️")
            with cent_co:
                total_users, total_logs = get_total_counts(conn)

                st.markdown("<h1 style='text-align: center;'>Database overview</h1>", unsafe_allow_html=True)
                overview_table = {
                    'Total': {
                        '🐒 Users': f'{format_number(total_users)}',
                        '📑 Logs': f'{format_number(total_logs)}'
                    }
                }
                st.table(overview_table)
    elif page == "🐒 Users":
        report_option = st.selectbox('Select a report', ['User table', 'Faction distribution', 'User logs by location',
                                     'Body count', 'Timeline'], index=0, placeholder="Choose an option", disabled=False, label_visibility="visible")
        if report_option == 'User table':
            st.sidebar.header("Save User Faction")
            user_name = st.sidebar.text_input("Enter User Name")
            faction = st.sidebar.selectbox(
                "Select Faction", ["East", "West", "Pirate", 'Mob'])

            if user_name:
                if st.sidebar.button("Save Faction"):
                    save_user_faction(conn, user_name.rstrip(), faction)
                    st.sidebar.success("Faction saved successfully!")
            
            df_user = get_users(conn)
            with st.container():
                df_user['Faction'] = df_user['Faction'].fillna('Empty')
                df1 = df_user['Faction'].value_counts().rename_axis('Faction').reset_index(name='Total')
                st.table(data=df1)
            
            with st.container():
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
                df_user['Faction'] = df_user['Faction'].fillna('Empty')
                df1 = df_user['Faction'].value_counts()
                st.bar_chart(data=df1)
        elif report_option == 'User logs by location':
            _, sidebar_fields = create_report_filter_sidebar(locations, faction=False)
            start_datetime = None if not sidebar_fields[
                'start_date'] else f"{sidebar_fields['start_date']} {sidebar_fields['start_time']}"
            end_datetime = None if not sidebar_fields[
                'end_date'] else f"{sidebar_fields['end_date']} {sidebar_fields['end_time']}"

            cursor = conn.cursor()
            filters = []
            if len(sidebar_fields['location_filter']) > 0:
                for location in sidebar_fields['location_filter']:
                    if location:
                        filters.append(f"logs.location = '{location}'")
            if start_datetime:
                filters.append(f"logs.time >= '{start_datetime}'")
            if end_datetime:
                filters.append(f"logs.time <= '{end_datetime}'")
            filters.append("users.user_name <> ''")
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
                user_logs_df['Faction'] = user_logs_df['Faction'].fillna('Empty')

                df_count_by_faction = user_logs_df['Faction'].value_counts()
                with st.container():
                    st.subheader("Distribuition")
                    total_users_by_faction = dict(East=0, West=0, Pirate=0, Empty=0)

                    for faction, count in df_count_by_faction.items():
                        total_users_by_faction[faction] = count
                    
                    faction_percentages = calculate_user_faction_percentage(total_users_by_faction)

                    east, west, pirate, empty = st.columns(4)
                    with east:
                        st.metric(
                            label="East", 
                            value=str(total_users_by_faction.get("East")), 
                            delta=f'{round(faction_percentages.get("East"), 2)}%',
                            delta_color='off',
                        )
                    with west:
                        st.metric(
                            label="West", 
                            value=str(total_users_by_faction.get("West")), 
                            delta=f'{round(faction_percentages.get("West"), 2)}%',
                            delta_color='off',
                        )
                    with pirate:
                        st.metric(
                            label="Pirate", value=str(total_users_by_faction.get("Pirate")), 
                            delta=f'{round(faction_percentages.get("Pirate"), 2)}%',
                            delta_color='off',
                        )
                    with empty:
                        st.metric(
                            label="Empty", 
                            value=str(total_users_by_faction.get("Empty")), 
                            delta=f'{round(faction_percentages.get("Empty"), 2)}%',
                            delta_color='off',
                        )
                        
                    style_metric_cards(background_color='#262730', border_color='#FF4B4B', border_left_color='#FF4B4B')

                    st.subheader("Logs by location")
                    with st.container():
                        if not user_logs_df.empty:
                            user_logs_df = user_logs_df[user_logs_df['Faction'] != 'Mob']
                            st.table(user_logs_df)
                        else:
                            st.write(
                                "No data available for the selected filters.")
        elif report_option == 'Body count':
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
        elif report_option == 'Timeline':
            cursor = conn.cursor()
            _, sidebar_fields = create_report_filter_sidebar(locations, faction=False)
            location_filter = sidebar_fields['location_filter'] 
            start_datetime = f"{sidebar_fields['start_date']} {sidebar_fields['start_time']}" if sidebar_fields['start_date'] else None
            end_datetime = f"{sidebar_fields['end_date']} {sidebar_fields['end_time']}" if sidebar_fields['end_date'] else None
            filters = []
            if len(sidebar_fields['location_filter']) > 0:
                for location in sidebar_fields['location_filter']:
                    if location:
                        filters.append(f"logs.location = '{location}'")
            if start_datetime:
                filters.append(f"enter >= '{start_datetime}'")
            if end_datetime:
                filters.append(f"exit <= '{end_datetime}'")
            sql_query ="""SELECT
                            location,
                            enter AS Start,
                            exit AS Finish
                        FROM
                            location_logs WHERE 1=1 AND """ + " AND ".join(filters)
            if location_filter:
                location_filter_str = "','".join(location_filter)
                sql_query += f" AND location IN ('{location_filter_str}')"
            if start_datetime:
                sql_query += f" AND enter >= '{start_datetime}'"
            if end_datetime:
                sql_query += f" AND exit <= '{end_datetime}'" 
            sql_query +=""" 
                        ORDER BY enter;"""
            cursor.execute(sql_query)
            data = cursor.fetchall()
            if not data:
                st.write('No logs for current filter.')
            else:
                columns = ['location', 'Start', 'Finish']
                df = pd.DataFrame(data, columns=columns)
                df['Start'] = pd.to_datetime(df['Start'])
                df['Finish'] = pd.to_datetime(df['Finish'])
                df.sort_values(by=['Start', 'location'], inplace=True)
                tasks = []
                for location, group in df.groupby('location'):
                    for start, end in zip(group['Start'], group['Finish']):
                        tasks.append(dict(Task=location, Start=start, Finish=end))
                num_locations = df['location'].nunique()
                colorscale = plotly.colors.sequential.Viridis
                if num_locations > len(colorscale):
                    colorscale = colorscale * (num_locations // len(colorscale) + 1)
                colorscale = colorscale[:num_locations]
                fig = ff.create_gantt(
                    tasks,
                    title='Event timeline',
                    index_col='Task',
                    show_colorbar=True,
                    group_tasks=True,
                    colors=colorscale
                )
                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(df, use_container_width=True)
                
                options = []
                for index, row in df.iterrows():
                    option = f"{index} - {row['location']} - {row['Start']} to {row['Finish']}"
                    options.append(option)
                selected_option = st.selectbox('Select event', options, index=None)

                if selected_option:
                    selected_index = int(selected_option.split()[0])
                    selected_row = df.iloc[selected_index]
                    selected_location = selected_row['location']
                    selected_start = selected_row['Start']
                    selected_end = selected_row['Finish']
                    query = f"""
                        SELECT DISTINCT users.user_name, users.faction, logs.location
                        FROM users
                        JOIN logs ON users.user_hash = logs.character_id OR users.user_hash = logs.receiver_id
                        WHERE logs.location = '{selected_location}'
                        AND logs.time >= '{selected_start}'
                        AND logs.time <= '{selected_end}'
                        ORDER by 3,2,1
                    """
                    cursor.execute(query)
                    filtered_data = cursor.fetchall()
                    st.subheader("Logs by location")
                    with st.container():
                        if filtered_data:
                            filtered_df = pd.DataFrame(filtered_data, columns=["User Name", "Faction", "Location"])
                            filtered_df['Faction'] = filtered_df['Faction'].fillna('Empty')
                            filtered_df = filtered_df[filtered_df['Faction'] != 'Mob']
                            st.table(filtered_df)

    elif page == "📑 Logs":
        _, sidebar_fields = create_report_filter_sidebar(locations)
        start_datetime = None if not sidebar_fields[
            'start_date'] else f"{sidebar_fields['start_date']} {sidebar_fields['start_time']}"
        end_datetime = None if not sidebar_fields[
            'end_date'] else f"{sidebar_fields['end_date']} {sidebar_fields['end_time']}"

        report_option = st.selectbox('Select a report', ['Overview', 'Pvp damage', 'Heals', 'Pve damage',
                                     'Top users by faction', 'Explorer'], index=0, placeholder="Choose an option", disabled=False)
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

        elif report_option == "Explorer":
            dmg_df = summarize_logs_filtered(conn, sidebar_fields['faction_filter'],
                                             sidebar_fields['location_filter'], start_datetime, end_datetime, 'Damage')
            heal_df = summarize_logs_filtered(conn, sidebar_fields['faction_filter'],
                                             sidebar_fields['location_filter'], start_datetime, end_datetime, 'Heal')
            frames = [dmg_df, heal_df]
            df_merged = pd.concat(frames)
            if not df_merged.empty:
                filtered_df = dataframe_explorer(df_merged, case=False)
                st.dataframe(filtered_df, use_container_width=True)
                
    elif page == "💾 Import":
        st.title("Log File Importer")

        st.write("Upload your Combat.log and Misc.log files below:")
        combat_log_file = st.file_uploader("Upload Combat.log", type=["log"])
        misc_log_file = st.file_uploader("Upload Misc.log", type=["log"])

        timezones = [DEFAULT_TIMEZONE] + pytz.all_timezones
        log_timezone = st.selectbox("Select the timezone of the logs:", timezones, index=0)

        if st.button("Import Logs"):
            if combat_log_file is not None and misc_log_file is not None:
                combat_file = StringIO(combat_log_file.getvalue().decode("ISO-8859-1"))
                misc_file = StringIO(misc_log_file.getvalue().decode("ISO-8859-1"))
                import_logs(combat_file.read(), misc_file.read(), log_timezone, DEFAULT_TIMEZONE, conn)
            else:
                st.write("Please upload both Combat.log and Misc.log files.")
    
    conn.close()


if __name__ == "__main__":
    main()
