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

def db_connection():
    return sqlite3.connect(DB_PATH)

import sqlite3

def db_connection():
    return sqlite3.connect(DB_PATH)

def setup_database():
    """Set up the database with the required tables and indexes."""
    with db_connection() as conn:
        cursor = conn.cursor()
        # Create accounts table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id TEXT UNIQUE NOT NULL
            )
        """)
        # Create videos table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS videos (
                id TEXT PRIMARY KEY,
                account_id TEXT,
                created_at TEXT,
                updated_at TEXT,
                name TEXT,
                description TEXT,
                duration INTEGER,
                state TEXT,
                tags TEXT,
                json_response TEXT,
                FOREIGN KEY(account_id) REFERENCES accounts(account_id)
            )
        """)
        # Create indexes for better performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_videos_account_id ON videos(account_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_videos_name ON videos(name)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_videos_state ON videos(state)")
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
            cursor.execute("INSERT OR IGNORE INTO accounts (account_id) VALUES (?)", (account_id,))
            conn.commit()

        batch_size = 1000  # Adjust batch size based on your requirements
        video_batch = []

        with pbar:
            for future in asyncio.as_completed(tasks):
                response = await future
                try:
                    async with db_lock:
                        with db_connection() as conn:
                            cursor = conn.cursor()
                            for video in response:
                                videos_processed += 1
                                pbar.update(1)
                                # Extract fields
                                video_id = video.get('id')
                                created_at = video.get('created_at')
                                updated_at = video.get('updated_at')
                                name = video.get('name')
                                description = video.get('description')
                                duration = video.get('duration')
                                state = video.get('state')
                                tags = ','.join(video.get('tags', []))
                                json_response = json.dumps(video)
                                
                                video_batch.append((video_id, account_id, created_at, updated_at, name, description, duration, state, tags, json_response))
                                if len(video_batch) >= batch_size:
                                    cursor.executemany(
                                        "INSERT OR REPLACE INTO videos (id, account_id, created_at, updated_at, name, description, duration, state, tags, json_response) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 
                                        video_batch
                                    )
                                    video_batch = []
                            if video_batch:
                                cursor.executemany(
                                    "INSERT OR REPLACE INTO videos (id, account_id, created_at, updated_at, name, description, duration, state, tags, json_response) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 
                                    video_batch
                                )
                            conn.commit()
                except Exception as e:
                    print(f'Error: {e}')

async def main():
    account_ids = [PUB_ID]  # List of account IDs to process
    await asyncio.gather(*(fetch_video_info(account_id) for account_id in account_ids))

if __name__ == '__main__':
    asyncio.run(main())