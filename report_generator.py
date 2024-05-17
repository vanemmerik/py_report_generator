import csv
import re
import os
import requests
import json
import time
import base64
from dotenv import load_dotenv
from tqdm import tqdm
from colorama import Fore
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# Brightcove API Credentials stored in .env
account_id = os.getenv('PUB_ID')
client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')

# Timestamp used for logfile
current_time = datetime.now().strftime("%H%M%S")

# CSV path based on .env file
csv_dir = os.getenv('CSV_PATH')

# Failure log path based on .env file
failure_log_dir = os.getenv('LOG_PATH')

# Last processed id file path based on .env file
last_processed_id_path = os.getenv('LAST_PROCESSED_PATH')

# Brightcove OAuth URL
oauth_url = 'https://oauth.brightcove.com/v4/access_token'

# Brightcove Ingest API endpoint templates
cms_api_video_count_template = 'https://cms.api.brightcove.com/v1/accounts/{}/counts/videos'
cms_api_video_info_template = 'https://cms.api.brightcove.com/v1/accounts/{}/videos?limit={}&offset={}'
cms_api_dr_template = 'https://cms.api.brightcove.com/v1/accounts/{}/videos/{}/assets/dynamic_renditions'
cms_api_master_info = 'https://cms.api.brightcove.com/v1/accounts/{}/videos/{}/digital_master'

fields_to_ignore = {
    'account_id',
    'digital_master_id',
    'clip_source_video_id', 
    'created_by', 
    'cue_points', 
    'custom_fields', 
    'description', 
    'folder_id',
    'images', 
    'link', 
    'long_description', 
    'long_description', 
    'projection', 
    'published_at', 
    'schedule', 
    'tags', 
    'text_tracks', 
    'transcripts', 
    'updated_at', 
    'updated_by', 
    'playback_rights_id', 
    'labels'
    }

master_keys_to_ignore = {
    'id',
    'created_at',
    'updated_at'
}

# ascii = "⣀⣄⣤⣦⣶⣷⣿"
ascii = "┄─━"

# Global variable to store the access token and expiry time
token_info = {'access_token': None, 'expires_in': None, 'acquired_at': None}

# Function to get or refresh the OAuth token with Base64 encoding
def get_or_refresh_token(retries=3):
    global token_info
    current_time = time.time()
    
    # Check if the token is still valid
    if token_info['access_token'] and (current_time - token_info['acquired_at']) < token_info['expires_in']:
        return token_info['access_token']
    else:
        # print("Requesting new OAuth token...")
        # Encode client_id and client_secret in Base64
        credentials = f"{client_id}:{client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': f'Basic {encoded_credentials}'
        }
        
        for attempt in range(retries):
            response = requests.post(oauth_url, headers=headers, data='grant_type=client_credentials')
            if response.status_code == 200:
                token_data = response.json()
                token_info = {
                    'access_token': token_data['access_token'],
                    'expires_in': token_data.get('expires_in', 300) - 60,  # Increased buffer
                    'acquired_at': current_time
                }
                # print("New OAuth token acquired.")
                return token_info['access_token']
            else:
                print(f"Attempt {attempt + 1} failed to get OAuth token: {response.status_code} {response.text}")
                time.sleep(2)  # Wait before retrying

        print("Failed to acquire new token after retries.")
        return None

def get_video_count(account_id, access_token):
    url = cms_api_video_count_template.format(account_id)
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        response_data = response.json()
        return response_data['count']
    else:
        print(f'Failed to get video count: {response.status_code}')
        print(response.text)
        return None

def fetch_and_write_videos(account_id, access_token, csv_path, fields_to_ignore):
    limit = 60
    offset = 0
    total_videos = get_video_count(account_id, access_token)
    # Open the file outside the loop and manage it within the loop
    with open(csv_path, mode='w', newline='') as file:
        writer = None
        with tqdm(total=total_videos, desc="Fetching Videos", bar_format=f"{Fore.GREEN}{{l_bar}}{Fore.RED}{{bar}}{Fore.RESET}{Fore.CYAN}{{r_bar}}{Fore.RESET}", ascii = ascii) as pbar: 
            while True:
                url = cms_api_video_info_template.format(account_id, limit, offset)
                headers = {'Authorization': f'Bearer {access_token}'}
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    video_data = response.json()
                    if not writer:  # Initialize CSV DictWriter after fetching the first page
                        fieldnames = [key for key in video_data[0].keys() if key not in fields_to_ignore]
                        writer = csv.DictWriter(file, fieldnames=fieldnames)
                        writer.writeheader()

                    for video in video_data:
                        filtered_video = {k: v for k, v in video.items() if k in fieldnames}
                        writer.writerow(filtered_video)

                    pbar.update(len(video_data))
                    if len(video_data) < limit:
                        break  # Exit loop when last page is processed
                    offset += limit
                else:
                    print(f"Failed to get video info: {response.status_code}")
                    print(response.text)
                    break

def write_to_csv(csv_path, video_data, fields_to_ignore):
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)  
    # Collect headers from all video items, excluding ignored fields
    headers = set()

    for video in video_data:
        headers.update(video.keys())
    headers = [h for h in headers if h not in fields_to_ignore]

    with open(csv_path, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        
        for video in video_data:
            # Filter out ignored fields from the video dictionary
            filtered_video = {k: v for k, v in video.items() if k in headers}
            writer.writerow(filtered_video)

def update_csv_with_json(csv_path, video_id, flattened_renditions):
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
        print(f"Video ID {video_id} not found in the CSV.")
        return
    if isinstance(flattened_renditions, str):
        import json
        flattened_renditions = json.loads(flattened_renditions)
    if not isinstance(flattened_renditions, list):
        print("flattened_renditions should be a list.")
        return
    for rendition in flattened_renditions:
        if not isinstance(rendition, dict):
            print(f"Expected dict but got {type(rendition)}")
            continue
        for key, value in rendition.items():
            row_to_update[key] = value
            if key not in fieldnames:
                fieldnames.append(key)
    # Write the updated rows back to the CSV
    with open(csv_path, mode='w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

def csv_master_info(csv_path, video_id, data):
    # Read the existing CSV into a list of dictionaries
    with open(csv_path, mode='r', newline='') as csv_file:
        reader = csv.DictReader(csv_file)
        rows = list(reader)
        fieldnames = reader.fieldnames

    # Find the row corresponding to the given video_id
    row_index = None
    for i, row in enumerate(rows):
        if row['id'] == video_id:
            row_index = i
            break

    if row_index is None:
        print(f"Video ID {video_id} not found in the CSV.")
        return

    # Prepare the new data to be inserted, filtering out ignored keys and prefixing with 'master_'
    new_data = {f"master_{key}": value for key, value in data.items() if key not in master_keys_to_ignore}

    # Update fieldnames to include new data keys
    for new_key in new_data.keys():
        if new_key not in fieldnames:
            fieldnames.insert(4, new_key)

    # Insert new data into the corresponding row
    for new_key, new_value in new_data.items():
        rows[row_index][new_key] = new_value

    # Write the updated rows back to the CSV
    with open(csv_path, mode='w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

def read_video_ids_from_csv(csv_path):
    video_ids = []
    try:
        with open(csv_path, mode='r', newline='') as file:
            reader = csv.reader(file)
            next(reader)  # Skip the header row
            for row in reader:
                if row:  # Ensure the row is not empty
                    video_ids.append(row[0])  # Assuming 'video_id' is in the first column
    except Exception as e:
        print(f"Failed to read the CSV file: {e}")
    return video_ids

def flatten_rendition_data(rendition_data):
    flattened_data = {}
    rendition_id = rendition_data['rendition_id'].split('/')[-1]
    for key, value in rendition_data.items():
        # Skip keys that are specified to be ignored
        if key in fields_to_ignore or key == 'rendition_id':
            continue
        # Format the new key and check if it should include ignored patterns
        if any(ignored_field in key for ignored_field in ['updated_at', 'created_at', 'duration']):
            continue
        new_key = f"{rendition_id}_{key}"
        flattened_data[new_key] = value
    return flattened_data

def fetch_rendition_details(csv_path, account_id, access_token, video_ids):
    delay = 0.25
    renditions_info = []
    with tqdm(total=len(video_ids), desc="Fetching Rendition Details", bar_format=f"{Fore.GREEN}{{l_bar}}{Fore.RED}{{bar}}{Fore.RESET}{Fore.CYAN}{{r_bar}}{Fore.RESET}", ascii = ascii) as pbar:
        for video_id in video_ids:
            access_token = get_or_refresh_token()  # Refresh token as needed
            headers = {'Authorization': f'Bearer {access_token}'}
            url = cms_api_dr_template.format(account_id, video_id)
            master_url = cms_api_master_info.format(account_id, video_id)
            master_response = requests.get(master_url, headers=headers)
            response = requests.get(url, headers=headers)
            if master_response.status_code == 200:
                master_data = master_response.json()                
                csv_master_info(csv_path, video_id, master_data)
            if response.status_code == 200:
                data = response.json()
                renditions_info.append(data)
                flattened_rendition = json.dumps([flatten_rendition_data(rendition) for rendition in data])
                update_csv_with_json(csv_path, video_id, flattened_rendition)
            else:
                print(f"Failed to fetch rendition data for video ID {video_id}: {response.status_code}")
                print(response.text)  # This prints the API error message
            time.sleep(delay)  # Confirm delay is an integer; this ensures API rate limits are respected
            pbar.update(1)
    return flattened_rendition

def reorder_csv(csv_path):
    with open(csv_path, mode='r', newline='') as csv_file:
        reader = csv.DictReader(csv_file)
        rows = list(reader)
        fieldnames = reader.fieldnames

    # Separate audio columns and other columns
    audio_columns = [col for col in fieldnames if col.startswith('audio')]
    other_columns = [col for col in fieldnames if not col.startswith('audio')]

    # Sort audio columns numerically
    def sort_audio_columns(col):
        match = re.match(r'audio(\d+)_', col)
        return int(match.group(1)) if match else float('inf')

    audio_columns.sort(key=sort_audio_columns)

    # Define the new column order with audio columns at the end
    new_fieldnames = other_columns + audio_columns

    # Write the reordered columns back to the same CSV
    with open(csv_path, mode='w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=new_fieldnames)
        writer.writeheader()
        writer.writerows(rows)

def main():
    access_token = get_or_refresh_token()
    if access_token:
        csv_file = f'{account_id}_{current_time}.csv'
        csv_path = os.path.join(csv_dir, account_id, csv_file)
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        fetch_and_write_videos(account_id, access_token, csv_path, fields_to_ignore)
        video_ids = read_video_ids_from_csv(csv_path)
        fetch_rendition_details(csv_path, account_id, access_token, video_ids)
        print(f'CSV file updated. Updating CSV order')
        reorder_csv(csv_path)
        print(f'Job done: {csv_path}')
if __name__ == "__main__":
    main()