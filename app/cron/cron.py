import functools
import re
import hashlib
from datetime import datetime
import psycopg2
import schedule
import time

COMBAT_LOG = "C:\\Users\\orlan\\Documents\\ArcheRage\\Combat.log"
MISC_LOG = "C:\\Users\\orlan\\Documents\\ArcheRage\\Misc.log"
OUTPUT_DIR = "output"

def log_function_call(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        now = datetime.now()
        print(f"> {now.strftime('%Y-%m-%d %H:%M:%S')}: {func.__name__}.", flush=True)
        return func(*args, **kwargs)
    return wrapper

def connect_to_database():
    """
    Connects to the PostgreSQL database.
    """
    return psycopg2.connect(
        dbname="user_logs",
        user="adm",
        password="supersecret",
        host="localhost",
        port="5432"
    )

def generate_hash(string):
    """
    Generates a unique hash for a given string.
    """
    return hashlib.md5(string.encode()).hexdigest()

def parse_datetime(date_str):
    """
    Parses a datetime object from a string.
    """
    if date_str == 'N/A':
        return None
    return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')

def is_within_duration(log_time, enter_time, exit_time):
    """
    Checks if a log time falls within the duration of entry and exit time for a location.
    """
    if enter_time is None or exit_time is None:
        return False
    return enter_time <= log_time <= exit_time

@log_function_call
def parse_combat(start_time=None, end_time=None, target_name=None):
    """
    Parses combat logs.
    """
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

@log_function_call
def parse_location():
    """
    Parses location logs.
    """
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

@log_function_call
def merge_logs():
    """
    Merges combat and location logs.
    """
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

@log_function_call
def create_database():
    """
    Creates the database and tables if they don't exist.
    """
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

@log_function_call
def insert_user_data(user_hash, user_name, faction=None):
    """
    Inserts user data into the database.
    """
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

@log_function_call
def insert_log_data(log_data):
    """
    Inserts log data into the database.
    """
    conn = connect_to_database()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO logs (log_type, time, character, receiver, total, location, log_id, character_id, receiver_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (log_id) DO NOTHING;
        """, log_data)
    conn.commit()
    conn.close()
    
@log_function_call
def import_logs():
    """
    Imports log data into the database in batches.
    """
    merged_logs = merge_logs()
    batch_logs = []
    batch_users = set()
    for l in merged_logs:
        log = list(l)
        log[1] = str(log[1].strftime('%Y-%m-%d %H:%M:%S'))
        log_hash = generate_hash(",".join(log))
        if log_hash in batch_logs:
            continue
        else:
            batch_logs.append(log_hash)
        log_data = (log[0], log[1], log[2], log[3], int(log[4]), log[5], log_hash, generate_hash(log[2]), generate_hash(log[3]))
        batch_users.add((log_data[7], log[2]))
        batch_users.add((log_data[8], log[3]))

    insert_batch_user_data(batch_users)
    insert_batch_log_data(merged_logs)
    now = datetime.now()
    print("> ", now.strftime("%Y-%m-%d %H:%M:%S"), ": finished.", flush=True)

@log_function_call
def insert_batch_user_data(batch_users):
    """
    Inserts batch user data into the database using prepared statements.
    """
    conn = connect_to_database()
    cursor = conn.cursor()
    try:
        args_str = ','.join(cursor.mogrify("(%s,%s)", x).decode() for x in batch_users)
        insert_query = " INSERT INTO users (user_hash, user_name) VALUES " + args_str + "  ON CONFLICT (user_hash) DO NOTHING;"
        cursor.execute(insert_query)
        conn.commit()
    except Exception as e:
        print("Error inserting batch log data:", e, flush=True)
        conn.rollback()

@log_function_call
def insert_batch_log_data(merged_logs):
    """
    Inserts batch log data into the database using prepared statements.
    """
    conn = connect_to_database()
    batch_size = 1000
    batch = []
    for l in merged_logs:
        log = list(l)
        log[1] = str(log[1].strftime('%Y-%m-%d %H:%M:%S'))
        log_hash = generate_hash(",".join(log))
        log_data = (log[0], log[1], log[2], log[3], int(log[4]), log[5], log_hash, generate_hash(log[2]), generate_hash(log[3]))
        batch.append(log_data)
        if len(batch) >= batch_size:
            insert_batch_log_data_single(conn, batch)
            batch = []
    if batch:
        insert_batch_log_data_single(conn, batch)
    conn.close()

@log_function_call
def insert_batch_log_data_single(conn, batch):
    """
    Inserts batch log data into the database using prepared statements (single insert).
    """
    cursor = conn.cursor()
    try:
        args_str = ','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s)", x).decode() for x in batch)
        insert_query = "INSERT INTO logs (log_type, time, character, receiver, total, location, log_id, character_id, receiver_id) VALUES " + args_str + " ON CONFLICT (log_id) DO NOTHING;"
        cursor.execute(insert_query)
        conn.commit()
    except Exception as e:
        print("Error inserting batch log data:", e, flush=True)
        conn.rollback()
    
        
def process_log_file():
    pattern = re.compile(r'<\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(.*?) has killed (.*?), totaling \d+ kill\(s\)!')
    conn = connect_to_database()
    users = dict(
        Pirate = [],
        East = [],
        West = []
    )
    nation_to_faction = dict(Nuia='West', Haranya='East', Pirate='Pirate')
    with open(MISC_LOG, 'r', encoding='ISO-8859-1') as file:
        for line in file:
            match = pattern.search(line)
            if match:
                faction1 = nation_to_faction[match.group(1).split(' ')[0].strip()]
                player1 = match.group(1).split(' ')[1].strip()
                faction2 = nation_to_faction[match.group(2).split(' ')[0].strip()]
                player2 = match.group(2).split(' ')[1].strip()
                user_hash1 = generate_hash(player1)
                user_hash2 = generate_hash(player2)
                users[faction1].append((user_hash1))
                users[faction2].append((user_hash2))
    for faction, hashes in users.items():
        batch = []
        for item in hashes:
            batch.append(item)
            if len(batch) >= 100:
                execute_batch_update(conn, batch, faction)
                batch = []
        if len(batch) > 0:
            execute_batch_update(conn, batch, faction)
            batch = []
    conn.close()


def execute_batch_update(conn, user_hashes, faction):
    try:
        cursor = conn.cursor()
        user_hashes_str = ','.join(map(lambda x: f"'{x}'", user_hashes))
        update_query = f"UPDATE users SET faction = '{faction}' WHERE user_hash IN ({user_hashes_str})"
        cursor.execute(update_query)
        conn.commit()
    except Exception as e:
        print(f"Error updating users for faction '{faction}':", e, flush=True)
        conn.rollback()


@log_function_call
def import_users():
    """
    Imports user data into the database at regular intervals.
    """
    process_log_file()
    now = datetime.now()
    print(f"> {now.strftime('%Y-%m-%d %H:%M:%S')}: finished importing users.", flush=True)
    
@log_function_call
def update_mob_users():
    """
    Updates the faction column to 'Mob' for users with whitespace in their names and null faction.
    """
    conn = connect_to_database()
    cursor = conn.cursor()
    try:
        select_uppercase_query = "SELECT user_hash FROM users WHERE faction IS NULL AND user_name ~ '^[A-Z\s]+$'"
        cursor.execute(select_uppercase_query)
        uppercase_users = [row[0] for row in cursor.fetchall()]
        select_whitespace_query = "SELECT user_hash FROM users WHERE faction IS NULL AND user_name LIKE '% %'"
        cursor.execute(select_whitespace_query)
        whitespace_users = [row[0] for row in cursor.fetchall()]
        users_to_update = set(uppercase_users + whitespace_users)
        users_updated = 0
        for user_hash in users_to_update:
            update_query = "UPDATE users SET faction = 'Mob' WHERE user_hash = %s"
            cursor.execute(update_query, (user_hash,))
            users_updated += 1
        conn.commit()
        print(f"Updated {users_updated} users with entirely uppercase names or whitespace in their names to faction 'Mob'.", flush=True)
    except Exception as e:
        print("Error updating users:", e, flush=True)
        conn.rollback()
    finally:
        conn.close()


@log_function_call
def schedule_import():
    """
    Schedules the import of logs and users at regular intervals.
    """
    schedule.every(interval=30).minutes.do(import_logs) 
    schedule.every().hour.do(import_users) 
    schedule.every().hour.do(update_mob_users) 
    schedule.run_all(delay_seconds=360)
    while True:
        schedule.run_pending()
        time.sleep(60)
        
        
if __name__ == "__main__":
    now = datetime.now()
    print(f"> {now.strftime('%Y-%m-%d %H:%M:%S')}: log import running.", flush=True)
    create_database()
    schedule_import()
    #update_mob_users()
    #import_users()
    #import_logs()
