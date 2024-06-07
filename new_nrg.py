import os
import time
import asyncio
import aiohttp
import ssl
import certifi
from dotenv import load_dotenv
import json
from tqdm.asyncio import tqdm
from colorama import Fore, Style, init as colorama_init
import sqlite3

# Initialize colorama
colorama_init(autoreset=True)

# Load .env file
load_dotenv()

# Uncomment the progress bar style you would like to use
ascii = "⣀⣄⣤⣦⣶⣷⣿"
# ascii = "─┄┈┉┅━"
# ascii = "▁▂▃▄▅▆▇█"
# ascii = "░▏▎▍▌▋▊▉█"
# ascii = "░▒▓█"

# Get credentials from .env
PUB_ID = os.getenv('PUB_ID')
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
DB_PATH = os.getenv('SQL_DB_PATH')
BASE_URL = 'https://cms.api.brightcove.com/v1/accounts/{}'
ENDPOINTS = {
    'video_count': '/counts/videos',
    'video_info': '/videos?limit={}&offset={}',
    'dynamic_renditions': '/videos/{}/assets/dynamic_renditions',
    'digital_master': '/videos/{}/digital_master'
}

# Metadata for table schemas
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
            "description": "TEXT",
            "duration": "INTEGER",
            "state": "TEXT",
            "tags": "TEXT",
            "json_response": "TEXT",
            "master_id": "TEXT",
            "master_size": "INTEGER",
            "master_width": "INTEGER",
            "master_height": "INTEGER",
            "master_duration": "INTEGER",
            "master_json": "TEXT"
        },
        "indexes": ["account_id", "name", "state"]
    }
}

def db_connection():
    return sqlite3.connect(DB_PATH)

def setup_database():
    """Set up the database with tables and indexes based on metadata."""
    with db_connection() as conn:
        cursor = conn.cursor()
        for table, schema in TABLE_SCHEMAS.items():
            columns = ", ".join(f"{col} {dtype}" for col, dtype in schema["columns"].items())
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} ({columns})")
            for index in schema["indexes"]:
                cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_{index} ON {table}({index})")
        conn.commit()

setup_database()  # Set up the database

# Global variable to store the token 
token_info = {'token': None, 'expires_at': 0}

# SSL context to use certifi's CA bundle
ssl_context = ssl.create_default_context(cafile=certifi.where())

def progress_bar(total, desc):
    bar_format = f"{Style.BRIGHT}{Fore.GREEN}{desc}{Style.RESET_ALL} |{Fore.RED}{{bar}}{Style.RESET_ALL}| {Style.BRIGHT}{{n_fmt}}/{{total_fmt}} [{Fore.CYAN}{{elapsed}}<{Fore.CYAN}{{remaining}}, {Fore.MAGENTA}{{rate_fmt}}{Style.RESET_ALL}]"
    return tqdm(total=total, desc=desc, bar_format=bar_format, ascii=ascii, ncols=None)

async def get_token():
    """Get a new token from Brightcove OAuth API."""
    url = f'https://oauth.brightcove.com/v4/access_token'
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = {
        'grant_type': 'client_credentials'
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, data=data, auth=aiohttp.BasicAuth(CLIENT_ID, CLIENT_SECRET), ssl=ssl_context) as response:
            if response.status == 200:
                token_data = await response.json()
                token_info['token'] = token_data['access_token']
                # Set the token expiry time (current time + expires_in seconds - 10 seconds for buffer)
                token_info['expires_at'] = time.time() + token_data['expires_in'] - 10
                print("New token fetched")
            else:
                raise Exception('Failed to get token: {}'.format(await response.text()))

async def get_valid_token():
    """Get a valid token, refreshing it if necessary."""
    if time.time() >= token_info['expires_at']:
        await get_token()
    return token_info['token']

async def api_request(session, account_id, endpoint_key, *args):
    url = BASE_URL.format(account_id) + ENDPOINTS[endpoint_key].format(*args)
    token = await get_valid_token()
    headers = {
        'Authorization': f'Bearer {token}'
    }

    async with session.get(url, headers=headers, ssl=ssl_context) as response:
        if response.status in range(200, 299):
            return await response.json()
        else:
            raise Exception(f'Request failed: {response.status}, {await response.text()}')

def insert_into_table(conn, table_name, data):
    """Insert data into a table dynamically based on metadata."""
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

async def fetch_video_info(account_id):
    limit = 90
    db_lock = asyncio.Lock()

    async with aiohttp.ClientSession() as session:
        count = await api_request(session, account_id, 'video_count')
        total_videos = count['count']
        iteration = (total_videos + limit - 1) // limit
        semaphore = asyncio.Semaphore(10)  # Limit the number of concurrent requests

        async def fetch_with_sem(session, account_id, endpoint_key, *args):
            async with semaphore:
                return await api_request(session, account_id, endpoint_key, *args)

        tasks = [fetch_with_sem(session, account_id, 'video_info', limit, offset) for offset in range(0, iteration * limit, limit)]
        
        videos_processed = 0
        pbar = progress_bar(total_videos, "Processing videos")
        
        # Insert account info into the accounts table if it doesn't exist
        with db_connection() as conn:
            cursor = conn.cursor()
            insert_into_table(conn, "accounts", {"account_id": account_id})
            conn.commit()

        batch_size = 1000  # Adjust batch size based on your requirements
        video_batch = []

        with pbar:
            for future in asyncio.as_completed(tasks):
                response = await future
                try:
                    async with db_lock:
                        with db_connection() as conn:
                            for video in response:
                                videos_processed += 1
                                pbar.update(1)
                                # Prepare video data
                                video_data = {
                                    "id": video.get('id'),
                                    "account_id": account_id,
                                    "created_at": video.get('created_at'),
                                    "updated_at": video.get('updated_at'),
                                    "name": video.get('name'),
                                    "description": video.get('description'),
                                    "duration": video.get('duration'),
                                    "state": video.get('state'),
                                    "tags": ','.join(video.get('tags', [])),
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
                    print(f'Error: {e}')

async def fetch_master_info(account_id):
    async with aiohttp.ClientSession() as session:
        db_lock = asyncio.Lock()

        with db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM videos WHERE account_id = ?", (account_id,))
            video_ids = [row[0] for row in cursor.fetchall()]

        total_videos = len(video_ids)
        pbar = progress_bar(total_videos, "Fetching master info")
        
        tasks = []
        for video_id in video_ids:
            try:
                endpoint = ENDPOINTS['digital_master'].format(video_id)
                # print(f"Fetching URL: {endpoint}")
                tasks.append(api_request(session, account_id, 'digital_master', video_id))
            except KeyError as e:
                print(f"Key error while formatting endpoint for video ID {video_id}: {e}")
                continue

        with pbar:
            for future in asyncio.as_completed(tasks):
                try:
                    response = await future
                    if response:  # Ensure response is not None or empty
                        async with db_lock:
                            with db_connection() as conn:
                                master_data = {
                                    "id": video_id,
                                    "master_id": response.get('id', None),
                                    "master_size": response.get('size', None),
                                    "master_width": response.get('width', None),
                                    "master_height": response.get('height', None),
                                    "master_duration": response.get('duration', None),
                                    "master_json": json.dumps(response) if response else None
                                }
                                cursor.execute("""
                                    UPDATE videos
                                    SET master_id = :master_id,
                                        master_size = :master_size,
                                        master_width = :master_width,
                                        master_height = :master_height,
                                        master_duration = :master_duration,
                                        master_json = :master_json
                                    WHERE id = :id
                                """, master_data)
                                conn.commit()
                    else:
                        print(f'No master info for video ID: {video_id}')
                except KeyError as e:
                    print(f"Key error for {video_id}: {e}")
                except Exception as e:
                    print(f'Error processing video ID {video_id}: {e}')
                finally:
                    pbar.update(1)

async def main():
    account_ids = [PUB_ID]
    for account_id in account_ids:
        await fetch_video_info(account_id)
        await fetch_master_info(account_id)

if __name__ == '__main__':
    asyncio.run(main())
