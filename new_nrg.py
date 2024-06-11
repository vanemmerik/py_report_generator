import os
import pandas as pd
import time
from datetime import datetime
import logging.config
import asyncio
import aiohttp
import aiosqlite
import aiofiles
import ssl
import certifi
from dotenv import load_dotenv
import json
from tqdm.asyncio import tqdm
from colorama import Fore, Style, init as colorama_init
import sqlite3

# Initialise colorama
colorama_init(autoreset=True)

# Load .env file
load_dotenv()

# Progress bar style in use
ascii = "⣀⣄⣤⣦⣶⣷⣿"
# ascii = "─┄┈┉┅━"
# ascii = "▁▂▃▄▅▆▇█"
# ascii = "░▏▎▍▌▋▊▉█"
# ascii = "░▒▓█"

# Get credentials from .env
AC_NAME = os.getenv('AC_NAME')
PUB_ID = os.getenv('PUB_ID')
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
LOGS = os.getenv('LOG_PATH')
DB_PATH = os.getenv('SQL_DB_PATH')
CSV_PATH = os.getenv('CSV_PATH')
BASE_URL = 'https://cms.api.brightcove.com/v1/accounts/{}'
ENDPOINTS = {
    'video_count': '/counts/videos',
    'video_info': '/videos?limit={}&offset={}',
    'dynamic_renditions': '/videos/{}/assets/dynamic_renditions',
    'digital_master': '/videos/{}/digital_master'
}

# Semaphore to control concurrency
semaphore = asyncio.Semaphore(10)

# Create the CSV for later
csv_file = f'{PUB_ID}_{datetime.now().strftime("%Y.%m.%d_%H:%M:%S")}.csv'
csv_dir = os.path.join(CSV_PATH, PUB_ID)
os.makedirs(csv_dir, exist_ok=True)
csv_path = os.path.join(csv_dir, csv_file)

# Logging stuff
log_file = f'{PUB_ID}_{datetime.now().strftime("%Y.%m.%d_%H:%M:%S")}.log'
log_path = os.path.join(LOGS, log_file)
os.makedirs(LOGS, exist_ok=True)
logging_config = {
    'version': 1,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - %(levelname)s - %(message)s'
        },
    },
    'handlers': {
        'file_handler': {
            'class': 'logging.FileHandler',
            'filename': log_path,
            'mode': 'a',
            'formatter': 'standard',
            'level': 'DEBUG',
        },
    },
    'root': {
        'handlers': ['file_handler'],
        'level': 'DEBUG',
    },
}

logging.config.dictConfig(logging_config)

def log_event(event_message, level='INFO'):
    if level == 'DEBUG':
        logging.debug(event_message)
    elif level == 'INFO':
        logging.info(event_message)
    elif level == 'WARNING':
        logging.warning(event_message)
    elif level == 'ERROR':
        logging.error(event_message)
    elif level == 'CRITICAL':
        logging.critical(event_message)
    else:
        logging.info(event_message)

# Metadata table schema
TABLE_SCHEMAS = {
    "accounts": {
        "columns": {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "account_id": "TEXT UNIQUE NOT NULL"
        },
        "indexes": ["account_id"]
    },
    "videos": {
        "columns": {
            "id": "TEXT PRIMARY KEY",
            "account_id": "TEXT",
            "created_at": "TEXT",
            "updated_at": "TEXT",
            "name": "TEXT",
            "duration": "INTEGER",
            "state": "TEXT",
            "delivery_type": "TEXT",
            "has_master": "TEXT",
            "json_response": "TEXT",
            "master_id": "TEXT",
            "master_size": "INTEGER",
            "master_width": "INTEGER",
            "master_height": "INTEGER",
            "master_duration": "INTEGER",
            "master_encoding_rate": "INTEGER",
            "master_json": "TEXT"
        },
        "indexes": ["account_id", "id"]
    },
    "renditions": {
        "columns": {
            "video_id": "TEXT",
            "rendition_id": "TEXT",
            "frame_width": "INTEGER",
            "frame_height": "INTEGER",
            "media_type": "TEXT",
            "size": "INTEGER",
            "created_at": "TEXT",
            "updated_at": "TEXT",
            "encoding_rate": "INTEGER",
            "duration": "INTEGER",
            "variant": "TEXT",
            "codec": "TEXT",
            "audio_configuration": "TEXT",
            "language": "TEXT",
            "rendition_json": "TEXT"
        },
        "indexes": ["video_id", "rendition_id"]
    }
}

# Global DB var
global_db_conn = None

def db_connection():
    global global_db_conn
    if global_db_conn is None:
        global_db_conn = sqlite3.connect(DB_PATH)
    return global_db_conn

def setup_database():
    with db_connection() as conn:
        cursor = conn.cursor()
        for table, schema in TABLE_SCHEMAS.items():
            columns = ", ".join(f"{col} {dtype}" for col, dtype in schema["columns"].items())
            if table == "renditions":
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table} (
                        {columns},
                        PRIMARY KEY (video_id, rendition_id)
                    )
                """)
            else:
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} ({columns})")
            for index in schema["indexes"]:
                cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_{index} ON {table}({index})")
        conn.commit()

setup_database()

# Store the token
token_info = {'token': None, 'expires_at': 0}
token_lock = asyncio.Lock()
token_event = asyncio.Event()

# SSL CA bundle
ssl_context = ssl.create_default_context(cafile=certifi.where())

def progress_bar(total, desc):
    bar_format = f"{Style.BRIGHT}{Fore.MAGENTA}{AC_NAME}: {Fore.WHITE}{PUB_ID}: {Fore.GREEN}{desc}{Style.RESET_ALL} |{Fore.RED}{{bar}}{Style.RESET_ALL}| {Style.BRIGHT}{{n_fmt}}/{{total_fmt}} [{Fore.CYAN}{{elapsed}}<{Fore.CYAN}{{remaining}}, {Fore.MAGENTA}{{rate_fmt}}{Style.RESET_ALL}]"
    return tqdm(total=total, desc=desc, bar_format=bar_format, ascii=ascii, ncols=None)

async def get_token():
    url = 'https://oauth.brightcove.com/v4/access_token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {'grant_type': 'client_credentials'}
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, data=data, auth=aiohttp.BasicAuth(CLIENT_ID, CLIENT_SECRET), ssl=ssl_context) as response:
            if response.status == 200:
                token_data = await response.json()
                token_info['token'] = token_data['access_token']
                token_info['expires_at'] = time.time() + token_data['expires_in'] - 10
                # token_info['expires_at'] = time.time() + 30  # Simulate ## seconds expiration time
                # print("New token fetched")
            else:
                raise Exception('Failed to get token: {}'.format(await response.text()))

async def get_valid_token():
    async with token_lock:
        if time.time() >= token_info['expires_at']:
            await get_token()
        token_event.set()
    await token_event.wait()
    return token_info['token']

async def api_request(session, account_id, endpoint_key, *args):
    try:
        url = BASE_URL.format(account_id) + ENDPOINTS[endpoint_key].format(*args)
        token = await get_valid_token()
        headers = {
            'Authorization': f'Bearer {token}'
        }
        f_args = ', '.join(map(str, args))
        async with session.get(url, headers=headers, ssl=ssl_context) as response:
            if response.status in range(200, 299):
                try:
                    return await response.json()
                except aiohttp.ContentTypeError:
                    log_event(f'Non-JSON response for video ID: {f_args} - URL: {url}', level='ERROR')
                    return None
            if response.status == 429:
                        retry_after = response.headers.get('Retry-After', 1)
                        log_event(f'Rate limit exceeded. Retrying after {retry_after} seconds', level='WARNING')
                        await asyncio.sleep(int(retry_after))
                        return await api_request(session, account_id, endpoint_key, *args)        
            else:
                log_event(f'Request failed for URL: {url}, status: {response.status}', level='ERROR')
                return None
    except IndexError as e:
        log_event(f"Error formatting URL with args: {args}, Error: {e}", level="ERROR")
    except Exception as e:
        log_event(f"Unexpected error: {e}", level="ERROR")

def insert_into_table(conn, table_name, data):
    if table_name not in TABLE_SCHEMAS:
        raise ValueError(f"Table {table_name} does not exist in the schema metadata.")
    
    columns = TABLE_SCHEMAS[table_name]["columns"]
    keys = data.keys()
    if not all(key in columns for key in keys):
        raise ValueError(f"Some keys in data are not in the schema for table {table_name}.")
    
    cols = ", ".join(keys)
    placeholders = ", ".join("?" for _ in keys)
    sql = f"INSERT OR REPLACE INTO {table_name} ({cols}) VALUES ({placeholders})"
    values = [data[key] for key in keys]
    with conn:
        conn.execute(sql, values)

async def ret_videos(account_id):
    limit = 90
    db_lock = asyncio.Lock()

    async with aiohttp.ClientSession() as session:
        count = await api_request(session, account_id, 'video_count')
        total_videos = count['count']
        iteration = (total_videos + limit - 1) // limit

        async def fetch_video_info(session, account_id, endpoint_key, *args):
            async with semaphore:
                return await api_request(session, account_id, endpoint_key, *args)

        tasks = [fetch_video_info(session, account_id, 'video_info', limit, offset) for offset in range(0, iteration * limit, limit)]
        
        videos_processed = 0
        pbar = progress_bar(total_videos, "Processing video data")
        
        with db_connection() as conn:
            insert_into_table(conn, "accounts", {"account_id": account_id})
            conn.commit()
    
        batch_size = 200 # Batch size - writing blocks to DB
        video_batch = []

        with pbar:
            try:
                for future in asyncio.as_completed(tasks):
                    response = await future
                    try:
                        async with db_lock:
                            with db_connection() as conn:
                                for video in response:
                                    videos_processed += 1
                                    pbar.update(1)
                                    video_data = {
                                        "id": video.get('id'),
                                        "account_id": account_id,
                                        "created_at": video.get('created_at'),
                                        "updated_at": video.get('updated_at'),
                                        "name": video.get('name'),
                                        "duration": video.get('duration'),
                                        "state": video.get('state'),
                                        "delivery_type": video.get('delivery_type'),
                                        "has_master": str(video.get('has_digital_master', False)),
                                        "json_response": json.dumps(video)
                                    }
                                    
                                    video_batch.append(video_data)
                                    if len(video_batch) >= batch_size:
                                        for v_data in video_batch:
                                            insert_into_table(conn, "videos", v_data)
                                        video_batch = []
                                if video_batch:
                                    for v_data in video_batch:
                                        insert_into_table(conn, "videos", v_data)
                                conn.commit()
                    except Exception as e:
                        log_event(f'Error: {e}', level='ERROR')
            finally:
                pbar.close()

async def ret_masters(account_id):
    async with aiohttp.ClientSession() as session:
        db_lock = asyncio.Lock()

        with db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id, has_master FROM videos WHERE account_id = ?", (account_id,))
            video_data = cursor.fetchall()

        total_videos = len(video_data)
        pbar = progress_bar(total_videos, "Getting master data")
        tasks = []

        async def fetch_master_info(video_id):
            async with semaphore:
                return await update_master_info(session, db_lock, account_id, video_id)

        for video_id, has_master in video_data:
            if not has_master or has_master.lower() == 'false':
                log_event(f"Skipping video ID: {video_id} because it has no master data.", level="DEBUG")
                pbar.update(1)
                continue

            tasks.append(fetch_master_info(video_id))

        if not tasks:
            log_event("No tasks to process as all videos were skipped.", level="INFO")
            pbar.close()
            return

        try:
            with pbar:
                for future in asyncio.as_completed(tasks):
                    await future
                    pbar.update(1)
        finally:
            pbar.close()

async def update_master_info(session, db_lock, account_id, video_id):
    try:
        response = await api_request(session, account_id, 'digital_master', video_id)
        if response:
            master_data = {
                "id": video_id,
                "master_id": response.get('id', None),
                "master_size": response.get('size', None),
                "master_width": response.get('width', None),
                "master_height": response.get('height', None),
                "master_duration": response.get('duration', None),
                "master_encoding_rate": response.get('encoding_rate', None),
                "master_json": json.dumps(response) if response else None
            }

            async with db_lock:
                with db_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        UPDATE videos
                        SET master_id = ?,
                            master_size = ?,
                            master_width = ?,
                            master_height = ?,
                            master_duration = ?,
                            master_encoding_rate = ?,
                            master_json = ?
                        WHERE id = ?
                    """, (
                        master_data["master_id"],
                        master_data["master_size"],
                        master_data["master_width"],
                        master_data["master_height"],
                        master_data["master_duration"],
                        master_data["master_encoding_rate"],
                        master_data["master_json"],
                        master_data["id"]
                    ))
                    conn.commit()
        else:
            pass
    except Exception as e:
        log_event(f'Error processing video ID {video_id}: {e}', level="ERROR")

async def ret_renditions(account_id):
    async with aiohttp.ClientSession() as session:
        db_lock = asyncio.Lock()

        with db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id, delivery_type FROM videos WHERE account_id = ?", (account_id,))
            video_data = cursor.fetchall()

        total_videos = len(video_data)
        pbar = progress_bar(total_videos, "Getting rendition data")
        tasks = []

        async def fetch_rendition_info(video_id):
            async with semaphore:
                return await update_rendition_info(session, db_lock, account_id, video_id)

        for video_id, delivery_type in video_data:
            if delivery_type not in ['static_origin', 'dynamic_origin']:
                log_event(f"Skipping video ID: {video_id} due to unsupported delivery type: {delivery_type}.", level="DEBUG")
                pbar.update(1)
                continue

            tasks.append(fetch_rendition_info(video_id))

        if not tasks:
            log_event(f"No tasks to process.", level="INFO")
            pbar.close()
            return

        try:
            with pbar:
                for future in asyncio.as_completed(tasks):
                    await future
                    pbar.update(1)
        finally:
            pbar.close()

async def update_rendition_info(session, db_lock, account_id, video_id):
    try:
        response = await api_request(session, account_id, 'dynamic_renditions', video_id)
        if response:
            async with db_lock:
                with db_connection() as conn:
                    cursor = conn.cursor()
                    for rendition in response:
                        rendition_data = {
                            "video_id": video_id,
                            "rendition_id": rendition.get('rendition_id'),
                            "frame_height": rendition.get('frame_height'),
                            "frame_width": rendition.get('frame_width'),
                            "media_type": rendition.get('media_type'),
                            "size": rendition.get('size'),
                            "created_at": rendition.get('created_at'),
                            "updated_at": rendition.get('updated_at'),
                            "encoding_rate": rendition.get('encoding_rate'),
                            "duration": rendition.get('duration'),
                            "variant": rendition.get('variant'),
                            "codec": rendition.get('codec'),
                            "audio_configuration": rendition.get('audio_configuration'),
                            "language": rendition.get('language'),
                            "rendition_json": json.dumps(rendition) if rendition else None
                        }
                        cursor.execute("""
                            INSERT INTO renditions (video_id, rendition_id, frame_height, frame_width, media_type, size, created_at, updated_at, encoding_rate, duration, variant, codec, audio_configuration, language, rendition_json)
                            VALUES (:video_id, :rendition_id, :frame_height, :frame_width, :media_type, :size, :created_at, :updated_at, :encoding_rate, :duration, :variant, :codec, :audio_configuration, :language, :rendition_json)
                            ON CONFLICT(video_id, rendition_id) DO UPDATE SET
                                frame_height = excluded.frame_height,
                                frame_width = excluded.frame_width,
                                media_type = excluded.media_type,
                                size = excluded.size,
                                created_at = excluded.created_at,
                                updated_at = excluded.updated_at,
                                encoding_rate = excluded.encoding_rate,
                                duration = excluded.duration,
                                variant = excluded.variant,
                                codec = excluded.codec,
                                audio_configuration = excluded.audio_configuration,
                                language = excluded.language,
                                rendition_json = excluded.rendition_json
                        """, rendition_data)
                    conn.commit()
        else:
            log_event(f"No rendition info for video ID: {video_id}", level="INFO")
    except Exception as e:
        log_event(f"Error processing rendition for video ID {video_id}: {e}", level="ERROR")

async def build_main_csv(account_id):
    if isinstance(account_id, str):
        account_id = int(account_id)
    
    account_id_tuple = (account_id,)

    async with aiosqlite.connect(DB_PATH) as conn:
        async with conn.execute('SELECT * FROM videos WHERE account_id = ?', account_id_tuple) as cursor:
            rows = await cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            data_frame = pd.DataFrame(rows, columns=columns)
    
    columns_to_ignore = ['json_response', 'master_json']
    data_frame = data_frame.drop(columns=columns_to_ignore, errors='ignore')

    total_rows = len(data_frame)
    pbar = progress_bar(total=total_rows, desc="Writing to CSV")

    async with aiofiles.open(csv_path, mode='w', newline='', encoding='utf-8') as csv_file:
        await csv_file.write(data_frame.to_csv(index=False, header=True))
        for _ in range(total_rows):
            pbar.update(1)

    pbar.close()

async def main():
    account_ids = [PUB_ID]
    for account_id in account_ids:
        await ret_videos(account_id)
        await ret_masters(account_id)
        await ret_renditions(account_id)
        await build_main_csv(account_id)

if __name__ == '__main__':
    asyncio.run(main())