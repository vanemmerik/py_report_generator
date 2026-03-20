import os
import zipfile
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
import subprocess
from urllib.parse import quote

# Run caffeinate process to prevent sleep - Mac only
caffeinate = subprocess.Popen(['caffeinate', '-s'])

# Initialise colorama
colorama_init(autoreset=True)

# Load .env file
load_dotenv()

# Progress bar style in use
ascii = "⣀⣄⣤⣦⣶⣷⣿"

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
    'video_count': '/counts/videos{}',
    'video_info': '/videos?limit={}&offset={}{}',
    'dynamic_renditions': '/videos/{}/assets/dynamic_renditions',
    'digital_master': '/videos/{}/digital_master'
}

# Semaphore to control concurrency default set to 10 RPS
semaphore = asyncio.Semaphore(10)

# CSV timestamp
timestamp = datetime.now().strftime("%Y.%m.%d_%H%M%S")


def csv_paths(csv_name):
    csv_file = f'{PUB_ID}_{timestamp}_{csv_name}.csv'
    csv_dir = os.path.join(CSV_PATH, PUB_ID, f'{timestamp}')
    os.makedirs(csv_dir, exist_ok=True)
    csv_path = os.path.join(csv_dir, csv_file)
    return csv_path


async def write_to_csv(csv_name, data_frame):
    csv_path = csv_paths(csv_name)
    async with aiofiles.open(csv_path, mode='w', newline='', encoding='utf-8') as csv_file:
        await csv_file.write(data_frame.to_csv(index=False, header=True))


def zip_dir():
    csv_dir = os.path.join(CSV_PATH, PUB_ID, f'{timestamp}')
    zip_file = os.path.join(CSV_PATH, PUB_ID, f'{PUB_ID}_{timestamp}.zip')
    os.makedirs(os.path.dirname(zip_file), exist_ok=True)

    with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(csv_dir):
            for file in files:
                file_path = os.path.join(root, file)
                if os.path.abspath(file_path) == os.path.abspath(zip_file):
                    continue
                arcname = os.path.relpath(file_path, start=csv_dir)
                zipf.write(file_path, arcname)


# Logging
log_file = f'{PUB_ID}_{timestamp}.log'
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


# Table structure
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
        "indexes": ["account_id", "id", "created_at"]
    },
    "renditions": {
        "columns": {
            "video_id": "TEXT",
            "account_id": "TEXT",
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
        "indexes": ["video_id", "rendition_id", "account_id"]
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
    bar_format = (
        f"{Style.BRIGHT}{Fore.MAGENTA}{AC_NAME}: {Fore.WHITE}{PUB_ID}: "
        f"{Fore.GREEN}{desc}{Style.RESET_ALL} |{Fore.RED}{{bar}}{Style.RESET_ALL}| "
        f"{Style.BRIGHT}{{n_fmt}}/{{total_fmt}} "
        f"[{Fore.CYAN}{{elapsed}}<{Fore.CYAN}{{remaining}}, "
        f"{Fore.MAGENTA}{{rate_fmt}}{Style.RESET_ALL}]"
    )
    return tqdm(total=total, desc=desc, bar_format=bar_format, ascii=ascii, ncols=None)


async def get_token():
    url = 'https://oauth.brightcove.com/v4/access_token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {'grant_type': 'client_credentials'}

    async with aiohttp.ClientSession() as session:
        async with session.post(
            url,
            headers=headers,
            data=data,
            auth=aiohttp.BasicAuth(CLIENT_ID, CLIENT_SECRET),
            ssl=ssl_context
        ) as response:
            if response.status == 200:
                token_data = await response.json()
                token_info['token'] = token_data['access_token']
                token_info['expires_at'] = time.time() + token_data['expires_in'] - 10
            else:
                raise Exception('Failed to get token: {}'.format(await response.text()))


async def get_valid_token():
    async with token_lock:
        if time.time() >= token_info['expires_at']:
            await get_token()
        token_event.set()
    await token_event.wait()
    return token_info['token']


async def api_request(session, account_id, endpoint_key, *args, retry_count=5):
    retry_status_codes = {500, 502, 503, 504}
    retry_wait = 3

    try:
        url = BASE_URL.format(account_id) + ENDPOINTS[endpoint_key].format(*args)
        token = await get_valid_token()
        headers = {'Authorization': f'Bearer {token}'}
        f_args = ', '.join(map(str, args))

        async with session.get(url, headers=headers, ssl=ssl_context) as response:
            if response.status in range(200, 299):
                try:
                    return await response.json()
                except aiohttp.ContentTypeError:
                    log_event(f'Non-JSON response for args: {f_args} - URL: {url}', level='ERROR')
                    return None

            elif response.status in retry_status_codes or response.status == 429:
                if retry_count > 0:
                    retry_after = retry_wait
                    if response.status == 429:
                        retry_after = int(response.headers.get('Retry-After', retry_wait))
                    log_event(
                        f'Status {response.status} for URL: {url}. Retrying in {retry_after} seconds...',
                        level='WARNING'
                    )
                    await asyncio.sleep(retry_after)
                    return await api_request(session, account_id, endpoint_key, *args, retry_count=retry_count - 1)
                else:
                    log_event(f'Request failed after retries for URL: {url}', level='ERROR')
                    return None
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


def prompt_date_filter():
    while True:
        mode = input("Process entire library or a date range? (all/range): ").strip().lower()

        if mode == 'all':
            return {
                "mode": "all",
                "start": None,
                "end": None,
                "api_q": None
            }

        if mode == 'range':
            start = input("Start date (YYYY-MM-DD): ").strip()
            end = input("End date (YYYY-MM-DD): ").strip()

            try:
                start_dt = datetime.strptime(start, "%Y-%m-%d")
                end_dt = datetime.strptime(end, "%Y-%m-%d")
            except ValueError:
                print("Invalid date format. Use YYYY-MM-DD.")
                continue

            if end_dt < start_dt:
                print("End date cannot be earlier than start date.")
                continue

            return {
                "mode": "range",
                "start": f"{start}T00:00:00.000Z",
                "end": f"{end}T23:59:59.999Z",
                "api_q": f"created_at:{start}T00:00:00.000Z..{end}T23:59:59.999Z"
            }

        print("Invalid input. Enter 'all' or 'range'.")


def get_sql_date_clause(date_filter, table_alias=None):
    if not date_filter or date_filter["mode"] == "all":
        return "", []

    prefix = f"{table_alias}." if table_alias else ""
    clause = f" AND {prefix}created_at >= ? AND {prefix}created_at <= ?"
    params = [date_filter["start"], date_filter["end"]]
    return clause, params


def clear_account_data(account_id):
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM renditions WHERE account_id = ?", (account_id,))
        cursor.execute("DELETE FROM videos WHERE account_id = ?", (account_id,))
        conn.commit()


async def ret_videos(account_id, date_filter=None):
    limit = 90
    db_lock = asyncio.Lock()

    query_suffix_for_count = ""
    query_suffix_for_videos = "&sort=created_at"

    if date_filter and date_filter["api_q"]:
        encoded_q = quote(date_filter["api_q"], safe=":.")
        query_suffix_for_count = f"?q={encoded_q}"
        query_suffix_for_videos = f"&sort=created_at&q={encoded_q}"

    async with aiohttp.ClientSession() as session:
        count = await api_request(session, account_id, 'video_count', query_suffix_for_count)
        if not count or 'count' not in count:
            log_event(f"Unable to retrieve video count for account {account_id}", level="ERROR")
            return

        total_videos = count['count']
        iteration = (total_videos + limit - 1) // limit

        async def fetch_video_info(session_obj, acct_id, endpoint_key, *args):
            async with semaphore:
                return await api_request(session_obj, acct_id, endpoint_key, *args)

        tasks = [
            fetch_video_info(session, account_id, 'video_info', limit, offset, query_suffix_for_videos)
            for offset in range(0, iteration * limit, limit)
        ]

        pbar = progress_bar(total_videos, "Processing video data")

        with db_connection() as conn:
            insert_into_table(conn, "accounts", {"account_id": account_id})
            conn.commit()

        batch_size = 200
        video_batch = []

        with pbar:
            try:
                for future in asyncio.as_completed(tasks):
                    response = await future
                    if not response:
                        continue

                    try:
                        async with db_lock:
                            with db_connection() as conn:
                                for video in response:
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
                                    video_batch = []

                                conn.commit()

                    except Exception as e:
                        log_event(f'Error processing video batch: {e}', level='ERROR')
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
                "master_id": response.get('id'),
                "master_size": response.get('size'),
                "master_width": response.get('width'),
                "master_height": response.get('height'),
                "master_duration": response.get('duration'),
                "master_encoding_rate": response.get('encoding_rate'),
                "master_json": json.dumps(response)
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
                log_event(
                    f"Skipping video ID: {video_id} due to unsupported delivery type: {delivery_type}.",
                    level="DEBUG"
                )
                pbar.update(1)
                continue

            tasks.append(fetch_rendition_info(video_id))

        if not tasks:
            log_event("No tasks to process.", level="INFO")
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
                            "account_id": account_id,
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
                            INSERT INTO renditions (
                                video_id, account_id, rendition_id, frame_height, frame_width,
                                media_type, size, created_at, updated_at, encoding_rate,
                                duration, variant, codec, audio_configuration, language, rendition_json
                            )
                            VALUES (
                                :video_id, :account_id, :rendition_id, :frame_height, :frame_width,
                                :media_type, :size, :created_at, :updated_at, :encoding_rate,
                                :duration, :variant, :codec, :audio_configuration, :language, :rendition_json
                            )
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


async def build_main_csv(account_id, date_filter=None):
    csv_name = "master"

    async with aiosqlite.connect(DB_PATH) as conn:
        sql = "SELECT * FROM videos WHERE account_id = ?"
        params = [account_id]

        date_clause, date_params = get_sql_date_clause(date_filter)
        sql += date_clause
        sql += " ORDER BY created_at"

        params.extend(date_params)

        async with conn.execute(sql, tuple(params)) as cursor:
            rows = await cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            data_frame = pd.DataFrame(rows, columns=columns)

    integer_columns = [
        'duration',
        'master_size',
        'master_width',
        'master_height',
        'master_duration',
        'master_encoding_rate'
    ]

    for column in integer_columns:
        if column in data_frame.columns:
            data_frame[column] = pd.to_numeric(
                data_frame[column],
                downcast='integer',
                errors='coerce'
            ).fillna(0).astype(int)

    columns_to_ignore = ['json_response', 'master_json']
    data_frame = data_frame.drop(columns=columns_to_ignore, errors='ignore')

    total_rows = len(data_frame)
    pbar = progress_bar(total=total_rows, desc="Writing main CSV")

    await write_to_csv(csv_name, data_frame)

    for _ in range(total_rows):
        pbar.update(1)

    pbar.close()


async def build_renditions_csv(account_id, date_filter=None):
    csv_name = "renditions"

    async with aiosqlite.connect(DB_PATH) as conn:
        sql = """
            SELECT r.*
            FROM renditions r
            INNER JOIN videos v
                ON v.id = r.video_id
               AND v.account_id = r.account_id
            WHERE r.account_id = ?
        """
        params = [account_id]

        date_clause, date_params = get_sql_date_clause(date_filter, table_alias="v")
        sql += date_clause
        sql += " ORDER BY v.created_at, r.video_id"

        params.extend(date_params)

        async with conn.execute(sql, tuple(params)) as cursor:
            rows = await cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            data_frame = pd.DataFrame(rows, columns=columns)

    integer_columns = ['frame_width', 'frame_height', 'size', 'encoding_rate', 'duration']
    for column in integer_columns:
        if column in data_frame.columns:
            data_frame[column] = pd.to_numeric(
                data_frame[column],
                downcast='integer',
                errors='coerce'
            ).fillna(0).astype(int)

    columns_to_ignore = ['created_at', 'updated_at', 'rendition_json']
    data_frame = data_frame.drop(columns=columns_to_ignore, errors='ignore')

    total_rows = len(data_frame)
    pbar = progress_bar(total=total_rows, desc="Writing renditions CSV")

    await write_to_csv(csv_name, data_frame)

    for _ in range(total_rows):
        pbar.update(1)

    pbar.close()


async def main():
    account_ids = [PUB_ID]

    for account_id in account_ids:
        date_filter = prompt_date_filter()

        while True:
            response = input(f"Have you already stored the video data for {AC_NAME}? (y/n): ").strip().lower()

            if response == 'y':
                # Use existing DB content only, but filter CSV output by date range if requested
                await build_main_csv(account_id, date_filter)
                await build_renditions_csv(account_id, date_filter)
                zip_dir()
                break

            elif response == 'n':
                # Fresh load from API: clear previous rows for this account first
                clear_account_data(account_id)

                await ret_videos(account_id, date_filter)
                await ret_masters(account_id)
                await ret_renditions(account_id)

                await build_main_csv(account_id, date_filter)
                await build_renditions_csv(account_id, date_filter)
                zip_dir()
                break

            else:
                print("Invalid input, please enter 'y' for Yes or 'n' for No.")


if __name__ == '__main__':
    try:
        asyncio.run(main())
    finally:
        caffeinate.terminate()