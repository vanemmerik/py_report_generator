import csv
import os
import requests
import time
import base64
from dotenv import load_dotenv
from tqdm import tqdm
from datetime import datetime
from colorama import Fore

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

#ascii = "⣀⣄⣤⣦⣶⣷⣿"
ascii = "┄─━"

# Brightcove OAuth URL
oauth_url = 'https://oauth.brightcove.com/v4/access_token'

# Brightcove Ingest API endpoint templates
cms_api_video_count_template = 'https://cms.api.brightcove.com/v1/accounts/{}/counts/videos'
cms_api_video_info_template = 'https://cms.api.brightcove.com/v1/accounts/{}/videos?limit={}&offset={}'
cms_api_dr_template = 'https://cms.api.brightcove.com/v1/accounts/{}/videos/{}/assets/dynamic_renditions'

fields_to_ignore = {
    'account_id', 
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

# Global variable to store the access token and expiry time
token_info = {'access_token': None, 'expires_in': None, 'acquired_at': None}

# Function to get or refresh the OAuth token with Base64 encoding
def get_or_refresh_token():
    global token_info
    current_time = time.time()
    
    if token_info.get('access_token') and (current_time - token_info.get('acquired_at', 0)) < token_info.get('expires_in', 0):
        # print("Token still valid, using existing token.")
        return token_info['access_token']
    else:
        print("Requesting new OAuth token or existing token expired...")
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
                'expires_in': token_data.get('expires_in', 300) - 30,  # Buffer
                'acquired_at': current_time
            }
            print("New OAuth token acquired successfully.")
            return token_info['access_token']
        else:
            print("Failed to get OAuth token:", response.status_code, response.text)
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

def fetch_and_write_videos(account_id, get_token, csv_path, fields_to_ignore):
    limit = 60
    offset = 0
    access_token = get_token()
    total_videos = get_video_count(account_id, access_token)
    
    with open(csv_path, mode='w', newline='') as file:
        writer = None
        with tqdm(total=total_videos, desc="Fetching Videos", 
                  bar_format=f"{Fore.CYAN}{{l_bar}}{Fore.RED}{{bar}}{Fore.RESET}{Fore.LIGHTGREEN_EX}{{r_bar}}{Fore.RESET}", ascii = ascii) as pbar: 
            while total_videos > 0:
                access_token = get_token()  # Refresh token for each cycle
                url = cms_api_video_info_template.format(account_id, limit, offset)
                headers = {'Authorization': f'Bearer {access_token}'}
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    video_data = response.json()
                    if not writer:
                        fieldnames = [key for key in video_data[0].keys() if key not in fields_to_ignore]
                        writer = csv.DictWriter(file, fieldnames=fieldnames)
                        writer.writeheader()

                    for video in video_data:
                        filtered_video = {k: v for k, v in video.items() if k in fieldnames}
                        writer.writerow(filtered_video)
                    pbar.update(len(video_data))
                    offset += limit
                else:
                    print(f"Failed to get video info: {response.status_code}")
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
        print(flattened_data)
    return flattened_data

def fetch_rendition_details(account_id, get_token, video_ids, delay=1):
    renditions_info = []
    for video_id in video_ids:
        access_token = get_token()  # Get fresh token for each request
        headers = {'Authorization': f'Bearer {access_token}'}
        url = cms_api_dr_template.format(account_id, video_id)
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            for rendition in data:
                flattened_rendition = flatten_rendition_data(rendition)
                print(flattened_rendition)
                renditions_info.append(flattened_rendition)
        else:
            print(f"Failed to fetch rendition data for video ID {video_id}: {response.status_code}")
            print(response.text)
        time.sleep(delay)
    return renditions_info

def main():
    # Do not fetch token here, pass the function instead
    csv_file = f'{account_id}_{current_time}.csv'
    csv_path = os.path.join(csv_dir, account_id, csv_file)
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    # Pass the token function, not the token itself
    fetch_and_write_videos(account_id, get_or_refresh_token, csv_path, fields_to_ignore)
    print(f'CSV file updated: {csv_path}')

    video_ids = read_video_ids_from_csv(csv_path)
    renditions = fetch_rendition_details(account_id, get_or_refresh_token, video_ids)
    print(renditions)

if __name__ == "__main__":
    main()