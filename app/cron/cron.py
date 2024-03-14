import csv
import os
import re
import sqlite3
import hashlib
from datetime import datetime, timedelta
import psycopg2
import schedule
import time

COMBAT_LOG = "C:\\Users\\orlan\\Documents\\ArcheRage\\Combat.log"
MISC_LOG = "C:\\Users\\orlan\\Documents\\ArcheRage\\Misc.log"
OUTPUT_DIR = "output"


def connect_to_database():
    # return sqlite3.connect("user_logs.db")
    return psycopg2.connect(
        dbname="user_logs",
        user="adm",
        password="supersecret",
        host="localhost",
        port="5432"
    )

# Function to generate a unique hash for a given string
def generate_hash(string):
    return hashlib.md5(string.encode()).hexdigest()

# Function to parse datetime from string
def parse_datetime(date_str):
    if date_str == 'N/A':
        return None
    return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')

# Function to check if log time falls within the duration of entry and exit time for a location
def is_within_duration(log_time, enter_time, exit_time):
    if enter_time is None or exit_time is None:
        return False
    return enter_time <= log_time <= exit_time

# Function to parse combat logs
def parse_combat(start_time=None, end_time=None, target_name=None):
    combat_logs = []
    damage_regex = re.compile(r"<(?P<log_time_str>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})(?P<character>.*?)\|r attacked (?P<receiver>.*?)\|r using \|cff25fcff(.*?)\|r and caused \|cffff0000\-(?P<total>\d+)")
    heal_regex = re.compile(r"<(?P<log_time_str>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})(?P<character>.*?)\|r targeted (?P<receiver>[^|]+)\|[^|]+\|cff25fcff(?P<ability>[^|]+)\|[^|]+\|cff00ff00(?P<restored>[^|]+)\|r health.")

    with open(COMBAT_LOG, "r", encoding="utf8") as file:
        for line in file:
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

# Function to parse location logs
def parse_location():
    location_logs = {}
    regex_enter = r"<(?P<log_timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})Entering Chat: \d+\.(?P<filter>Shout)\. (?P<log_location>[\w\s]+)"
    regex_leave = r"<(?P<log_timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})Leaving Chat: \d+\.(?P<filter>Shout)\. (?P<log_location>[\w\s]+)"

    with open(MISC_LOG, "r", encoding='ISO-8859-1') as file:
        for line in file:
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

# Function to merge combat and location logs
def merge_logs():
    location_logs = parse_location()
    combat_logs = parse_combat()

    merged_logs = []
    for combat_log in combat_logs:
        log_time = combat_log[1]
        for location, times in location_logs.items():
            if is_within_duration(log_time, times.get('enter'), times.get('exit')):
                merged_logs.append(combat_log + (location,))
                break

    return merged_logs

# Function to create a SQLite database and tables if they don't exist
def create_database():
    conn = connect_to_database()
    cursor = conn.cursor()

    # Create users table if it doesn't exist
    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                        user_hash TEXT PRIMARY KEY,
                        user_name TEXT,
                        faction TEXT)''')

    # Create logs table if it doesn't exist
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
    conn.close()

# Function to insert user data into the database
def insert_user_data(user_hash, user_name, faction=None):
    if ' ' in user_name:
        faction = 'Mob'
    conn = connect_to_database()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO users (user_hash, user_name, faction)
        VALUES (%s, %s, %s)
        ON CONFLICT (user_hash) DO NOTHING;
        """, (user_hash, user_name, faction)
    )
    conn.commit()
    conn.close()

# Function to insert log data into the database
def insert_log_data(log_data):
    conn = connect_to_database()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO logs (log_type, time, character, receiver, total, location, log_id, character_id, receiver_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (log_id) DO NOTHING;
        """, log_data)
    conn.commit()
    conn.close()

# Function to import log data into the database
def import_logs():
    now = datetime.now()
    print("> ", now.strftime("%Y-%m-%d %H:%M:%S"), ": importing logs.")
    merged_logs = merge_logs()

    for l in merged_logs:
        log = list(l)
        log[1] = str(log[1].strftime('%Y-%m-%d %H:%M:%S'))
        log_data = (log[0], log[1], log[2], log[3], int(log[4]), log[5], generate_hash(",".join(log)), generate_hash(log[2]), generate_hash(log[3]))
        insert_log_data(log_data)
        insert_user_data(log_data[7], log[2])
        insert_user_data(log_data[8], log[3])
    
    now = datetime.now()
    print("> ", now.strftime("%Y-%m-%d %H:%M:%S"), ": finished.")

def schedule_import():
    schedule.every().hour.do(import_logs) 

    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    now = datetime.now()
    print("> ", now.strftime("%Y-%m-%d %H:%M:%S"), ": log import running.")
    create_database()
    schedule_import()
    #import_logs()
