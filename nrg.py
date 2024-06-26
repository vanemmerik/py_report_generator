import sys
import csv
import base64
import time
import re
import os
import requests
import aiohttp
import asyncio
import logging
import ssl
import certifi
import tracemalloc
import signal
import functools
from concurrent.futures import ThreadPoolExecutor
from tqdm.asyncio import tqdm
from colorama import Fore, Style, init
from datetime import datetime
from dotenv import load_dotenv
import json

# Load environment variables from .env file
load_dotenv()

# Enable tracemalloc to get detailed information about memory allocations
tracemalloc.start()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize colorama
init(autoreset=True)

# Brightcove API Credentials stored in .env
account_id = os.getenv('PUB_ID')
client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')

# CSV path based on .env file
csv_dir = os.getenv('CSV_PATH')

# Log path based on .env file
logs_dir = os.getenv('LOG_PATH')

# Last processed video ID based on .env path
last_processed_dir = os.getenv('LAST_PROCESSED_PATH')
last_processed_file = os.path.join(last_processed_dir, 'last_processed.txt')

# Brightcove OAuth URL
oauth_url = 'https://oauth.brightcove.com/v4/access_token'

# Brightcove Ingest API endpoint templates
cms_api_video_count = 'https://cms.api.brightcove.com/v1/accounts/{}/counts/videos'
cms_api_video_info = 'https://cms.api.brightcove.com/v1/accounts/{}/videos?limit={}&offset={}'
cms_api_dr = 'https://cms.api.brightcove.com/v1/accounts/{}/videos/{}/assets/dynamic_renditions'
cms_api_master_info = 'https://cms.api.brightcove.com/v1/accounts/{}/videos/{}/digital_master'

# Fields info keys to ignore in API response
video_info_ignore = {
    'account_id', 'digital_master_id', 'clip_source_video_id', 'created_by',
    'cue_points', 'custom_fields', 'description', 'folder_id', 'images',
    'link', 'long_description', 'projection', 'published_at', 'schedule',
    'tags', 'text_tracks', 'transcripts', 'updated_at', 'updated_by',
    'playback_rights_id', 'labels'
}

master_keys_ignore = {
    'id', 'created_at', 'updated_at'
}

# Uncomment the progress bar style you would like to use
# ascii = "⣀⣄⣤⣦⣶⣷⣿"
ascii = "─┄┈┉┅━"
# ascii = "▁▂▃▄▅▆▇█"
# ascii = "░▏▎▍▌▋▊▉█"
# ascii = "░▒▓█"

def handle_interrupt(signal, frame, loop, tasks):
    global interrupted
    interrupted = True
    logging.info("Script interrupted. Saving state and exiting...")
    save_last_processed(current_csv_file, current_video_id)
    for task in tasks:
        task.cancel()
    loop.stop()

def save_last_processed(csv_file, video_id):
    with open(last_processed_file, 'w') as f:
        f.write(f"{csv_file}\n{video_id}\n")

def load_last_processed():
    if os.path.exists(last_processed_file):
        with open(last_processed_file, 'r') as f:
            lines = f.read().splitlines()
            if len(lines) == 2:
                return lines[0], lines[1]
    return None, None

# Stored variables for logging and continuation
current_csv_file = None
current_video_id = None
interrupted = False


# Global variable to store the access token and expiry time
token_info = {'access_token': None, 'expires_in': None, 'acquired_at': None}

# Function to get or refresh the OAuth token with Base64 encoding
def get_or_refresh_token():
    global token_info
    current_time = time.time()

    # Check if the token is still valid
    if token_info['access_token'] and (current_time - token_info['acquired_at']) < token_info['expires_in']:
        return token_info['access_token']
    else:
        # Encode client_id and client_secret in Base64
        credentials = f"{client_id}:{client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': f'Basic {encoded_credentials}'
        }

        response = requests.post(oauth_url, headers=headers, data='grant_type=client_credentials')
        if response.status_code == 200:
            token_data = response.json()
            token_info = {
                'access_token': token_data['access_token'],
                'expires_in': token_data.get('expires_in', 300) - 60,  # 1 minute buffer
                'acquired_at': current_time
            }
            # logging.info('New OAuth token acquired.')
            return token_info['access_token']
        else:
            logging.error(f"Failed to acquire token: {response.status_code} {response.text}")
            return None

def get_video_count(account_id, access_token):
    url = cms_api_video_count.format(account_id)
    headers = {'Authorization': f'Bearer {access_token}'}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        response_data = response.json()
        return response_data['count']
    else:
        logging.error(f'Failed to get video count: {response.status_code}')
        logging.error(response.text)
        return None

async def fetch_video_data(session, url, headers, ssl_context):
    async with session.get(url, headers=headers, ssl=ssl_context) as response:
        if response.status == 200:
            return await response.json()
        else:
            logging.error(f"Failed to get video info: {response.status}")
            logging.error(await response.text())
            return None

async def fetch_and_write_videos(account_id, access_token, csv_dir, video_info_ignore, created_files, max_videos=None):
    limit = 80
    offset = 0
    total_videos = get_video_count(account_id, access_token)
    if max_videos:
        total_videos = min(total_videos, max_videos)  # Use the minimum of total videos and max_videos
    headers = {'Authorization': f'Bearer {access_token}'}
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    current_time = datetime.now().strftime("%Y%m%d-%H%M%S")
    account_csv_dir = os.path.join(csv_dir, account_id)
    os.makedirs(account_csv_dir, exist_ok=True)

    async with aiohttp.ClientSession() as session:
        tasks = []
        while offset < total_videos:
            batch_size = min(limit, total_videos - offset)  # Adjust the batch size to not exceed total_videos
            url = cms_api_video_info.format(account_id, batch_size, offset)
            tasks.append(fetch_video_data(session, url, headers, ssl_context))
            offset += batch_size

        file_index = 1
        row_count = 0
        csv_path = os.path.join(account_csv_dir, f'{account_id}_{current_time}_{file_index}.csv')
        created_files.append(csv_path)  # Track created CSV files

        with tqdm(total=total_videos, desc=f"{Style.BRIGHT}Fetching Videos{Style.RESET_ALL}", bar_format=f"{Fore.GREEN}{{l_bar}}{Fore.RED}{{bar}}{Fore.RESET}{Fore.CYAN}{{r_bar}}{Fore.RESET}", ascii=ascii) as pbar:
            for task in asyncio.as_completed(tasks):
                video_data = await task
                if video_data:
                    if row_count == 0:
                        # Open a new CSV file
                        file = open(csv_path, mode='w', newline='')
                        writer = None

                    if not writer:
                        fieldnames = [key for key in video_data[0].keys() if key not in video_info_ignore]
                        writer = csv.DictWriter(file, fieldnames=fieldnames)
                        writer.writeheader()

                    for video in video_data:
                        if row_count == 5000: # Set row count for csv
                            # Close the current file and start a new one
                            file.close()
                            file_index += 1
                            csv_path = os.path.join(account_csv_dir, f'{account_id}_{current_time}_{file_index}.csv')
                            created_files.append(csv_path)  # Track created CSV files
                            file = open(csv_path, mode='w', newline='')
                            writer = csv.DictWriter(file, fieldnames=fieldnames)
                            writer.writeheader()
                            row_count = 0

                        filtered_video = {k: v for k, v in video.items() if k in fieldnames}
                        writer.writerow(filtered_video)
                        row_count += 1
                        pbar.update(1)
                        if max_videos and row_count >= max_videos:
                            break  # Stop processing once the max_videos limit is reached
                if max_videos and row_count >= max_videos:
                    break  # Stop processing once the max_videos limit is reached

            # Ensure the last file is closed
            if row_count > 0:
                file.close()

        pbar.n = row_count  # Update the progress bar to the correct count
        pbar.refresh()

    return created_files

def read_video_ids_from_csv(csv_path):
    video_ids = []
    try:
        with open(csv_path, mode='r', newline='') as file:
            reader = csv.reader(file)
            next(reader)  # Skip the header row
            for row in reader:
                if row:  # Ensure the row is not empty
                    video_ids.append(row[0])  # Assuming 'id' is in the first column
    except Exception as e:
        logging.error(f"Failed to read the CSV file: {e}")
    return video_ids

def csv_master_info(csv_path, video_id, data):
    with open(csv_path, mode='r', newline='') as csv_file:
        reader = csv.DictReader(csv_file)
        rows = list(reader)
        fieldnames = reader.fieldnames

    row_index = None
    for i, row in enumerate(rows):
        if row['id'] == video_id:
            row_index = i
            break

    if row_index is None:
        logging.error(f"Video ID {video_id} not found in the CSV.")
        return

    new_data = {f"master_{key}": value for key, value in data.items() if key not in master_keys_ignore}

    for new_key in new_data.keys():
        if new_key not in fieldnames:
            fieldnames.append(new_key)

    for new_key, new_value in new_data.items():
        rows[row_index][new_key] = new_value

    with open(csv_path, mode='w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

def flatten_rendition_data(rendition_data):
    flattened_data = {}
    rendition_id = rendition_data['rendition_id'].split('/')[-1]
    for key, value in rendition_data.items():
        if key in video_info_ignore or key == 'rendition_id' or value is None:
            continue
        if any(ignored_field in key for ignored_field in ['updated_at', 'created_at', 'duration']):
            continue
        new_key = f"{rendition_id}_{key}"
        flattened_data[new_key] = value
    return flattened_data

def update_csv_json(csv_path, video_id, flattened_renditions):
    with open(csv_path, mode='r', newline='') as csv_file:
        reader = csv.DictReader(csv_file)
        rows = list(reader)
        fieldnames = reader.fieldnames

    row_to_update = None
    for row in rows:
        if row['id'] == video_id:
            row_to_update = row
            break

    if row_to_update is None:
        logging.error(f"Video ID {video_id} not found in the CSV.")
        return

    if isinstance(flattened_renditions, str):
        flattened_renditions = json.loads(flattened_renditions)

    if not isinstance(flattened_renditions, list):
        logging.error("flattened_renditions should be a list.")
        return

    for rendition in flattened_renditions:
        if not isinstance(rendition, dict):
            logging.error(f"Expected dict but got {type(rendition)}")
            continue
        for key, value in rendition.items():
            row_to_update[key] = value
            if key not in fieldnames:
                fieldnames.append(key)

    with open(csv_path, mode='w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

async def fetch_rendition_details(csv_path, account_id, video_ids, failure_log_path, resume=False):
    global current_video_id  # Access the global variable to update the current video ID
    delay = 0.1  # 10 requests per second
    renditions_info = []
    error_count = 0

    ssl_context = ssl.create_default_context(cafile=certifi.where())
    progress_desc = "Interrupted - Resuming Process" if resume else "Fetching Rendition Details"

    async def fetch_and_process(session, video_id, pbar, failure_log):
        nonlocal error_count
        headers = {'Authorization': f'Bearer {get_or_refresh_token()}'}
        url = cms_api_dr.format(account_id, video_id)
        master_url = cms_api_master_info.format(account_id, video_id)

        global current_video_id
        current_video_id = video_id  # Update the current video ID being processed
        logging.debug(f"Processing video ID: {current_video_id}")

        try:
            async with session.get(master_url, headers=headers, ssl=ssl_context) as master_response:
                if master_response.status == 200:
                    master_data = await master_response.json()
                    csv_master_info(csv_path, video_id, master_data)
                elif master_response.status == 204:
                    error_count += 1
                    failure_log.write(f"Failed to fetch master data for video ID {video_id}: {master_response.status}\n")
                else:
                    error_count += 1
                    failure_log.write(f"Failed to fetch master data for video ID {video_id}: {master_response.status}\n")
                    failure_log.write(await master_response.text() + "\n")

            async with session.get(url, headers=headers, ssl=ssl_context) as response:
                if response.status == 200:
                    data = await response.json()
                    renditions_info.append(data)
                    flattened_rendition = json.dumps([flatten_rendition_data(rendition) for rendition in data])
                    update_csv_json(csv_path, video_id, flattened_rendition)
                elif response.status == 204:
                    error_count += 1
                    failure_log.write(f"Failed to fetch rendition data for video ID {video_id}: {response.status}\n")
                else:
                    error_count += 1
                    failure_log.write(f"Failed to fetch rendition data for video ID {video_id}: {response.status}\n")
                    failure_log.write(await response.text() + "\n")
        except Exception as e:
            logging.error(f"Error processing video ID {video_id}: {e}")
            error_count += 1
            failure_log.write(f"Error processing video ID {video_id}: {e}\n")

        await asyncio.sleep(delay)
        pbar.update(1)

    async with aiohttp.ClientSession() as session:
        tasks = []
        with open(failure_log_path, mode='a') as failure_log, tqdm(total=len(video_ids), desc=f"{Style.BRIGHT}{progress_desc}{Style.RESET_ALL}", bar_format=f"{Fore.GREEN}{{l_bar}}{Fore.RED}{{bar}}{Fore.RESET}{Fore.CYAN}{{r_bar}}{Fore.RESET}", ascii=ascii) as pbar:
            for video_id in video_ids:
                task = fetch_and_process(session, video_id, pbar, failure_log)
                tasks.append(task)
                if len(tasks) % 10 == 0:  # Adjust this value based on API rate limits
                    await asyncio.gather(*tasks)
                    logging.debug(f"Saving state: {current_csv_file}, {current_video_id}")
                    save_last_processed(current_csv_file, current_video_id)  # Save the state during processing
                    tasks = []

            if tasks:
                await asyncio.gather(*tasks)
                logging.debug(f"Final save state: {current_csv_file}, {current_video_id}")
                save_last_processed(current_csv_file, current_video_id)  # Save the state after processing

    return renditions_info, error_count

def reorder_csv(csv_path):
    with open(csv_path, mode='r', newline='') as csv_file:
        reader = csv.DictReader(csv_file)
        rows = list(reader)
        fieldnames = reader.fieldnames

    audio_columns = [col for col in fieldnames if col.startswith('audio')]
    other_columns = [col for col in fieldnames if not col.startswith('audio')]

    def sort_audio_columns(col):
        match = re.match(r'audio(\d+)_', col)
        return int(match.group(1)) if match else float('inf')

    audio_columns.sort(key=sort_audio_columns)

    new_fieldnames = other_columns + audio_columns

    with open(csv_path, mode='w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=new_fieldnames)
        writer.writeheader()
        writer.writerows(rows)

async def main(max_videos=None):
    global token_info, current_csv_file, current_video_id, interrupted
    created_files = []
    last_csv_file, last_video_id = load_last_processed()
    total_error_count = 0

    access_token = get_or_refresh_token()
    if access_token:
        account_csv_dir = os.path.join(csv_dir, account_id)
        os.makedirs(account_csv_dir, exist_ok=True)
        log_file = f'{account_id}_{datetime.now().strftime("%Y%m%d-%H%M%S")}.log'
        failure_log_path = os.path.join(logs_dir, log_file)
        os.makedirs(logs_dir, exist_ok=True)

        loop = asyncio.get_running_loop()
        tasks = []

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, handle_interrupt, sig, None, loop, tasks)

        # Get all existing CSV files in the account directory
        existing_csv_files = sorted([f for f in os.listdir(account_csv_dir) if f.endswith('.csv')])

        if last_csv_file and last_video_id:
            # Extract filename from the full path
            last_csv_filename = os.path.basename(last_csv_file)
            # Continue from last processed state
            try:
                resume_from_index = existing_csv_files.index(last_csv_filename)
            except ValueError:
                logging.error(f"Last processed file {last_csv_filename} not found in existing CSV files.")
                resume_from_index = 0
            existing_csv_files = existing_csv_files[resume_from_index:]
        else:
            # Start from scratch
            created_files = await fetch_and_write_videos(account_id, access_token, csv_dir, video_info_ignore, created_files, max_videos)
            existing_csv_files = created_files

        for csv_file in existing_csv_files:
            csv_path = os.path.join(account_csv_dir, csv_file)
            video_ids = read_video_ids_from_csv(csv_path)

            if last_csv_file and csv_file == last_csv_filename:
                try:
                    start_index = video_ids.index(last_video_id) + 1
                except ValueError:
                    logging.warning(f"Last processed video ID {last_video_id} not found in CSV {csv_file}. Starting from the beginning of this file.")
                    start_index = 0
                video_ids = video_ids[start_index:]

            if video_ids:
                current_csv_file = csv_file  # Update the current CSV file being processed
                task = asyncio.create_task(fetch_rendition_details(csv_path, account_id, video_ids, failure_log_path, resume=bool(last_csv_file)))
                tasks.append(task)
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, tuple) and len(result) == 2:
                        _, error_count = result
                        total_error_count += error_count
                save_last_processed(csv_path, video_ids[-1])  # Save the state after processing the batch
                logging.info(f"CSV file {Fore.CYAN}{csv_file}{Fore.RESET} updated. {Style.BRIGHT}Updating CSV order{Style.RESET_ALL}")
                reorder_csv(csv_path)
                logging.info(f"Job done: {Fore.CYAN}{csv_path}{Fore.RESET}")

    if not interrupted:
        if os.path.exists(last_processed_file):
            os.remove(last_processed_file)
        logging.info(f"{Fore.CYAN}{Style.BRIGHT}Processing complete. Exiting script.{Style.RESET_ALL}{Fore.RESET}")
        logging.info(f"Total errors encountered: {Fore.RED}{Style.BRIGHT}{total_error_count}{Style.RESET_ALL}{Fore.RESET}. Check the log file for details.")
    else:
        logging.info(f"Total errors encountered: {Fore.RED}{total_error_count}{Fore.RESET}")

    sys.exit(0)  # Terminate the script after processing

if __name__ == "__main__":
    max_videos = None # Set your limit here or None to process all videos
    try:
        asyncio.run(main(max_videos))
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt caught. Exiting gracefully...")
        save_last_processed(current_csv_file, current_video_id)
        sys.exit(0)

# snapshot = tracemalloc.take_snapshot()
# top_stats = snapshot.statistics('lineno')

# print("[ Top 10 memory allocations ]")
# for stat in top_stats[:10]:
#     print(stat)