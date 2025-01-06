import os
import pandas as pd
import json
from googleapiclient.discovery import build
import time

# Initialize API
API_KEY = os.getenv("API_KEY")
if API_KEY is None:
    raise ValueError("API_KEY environment variable is not set")
youtube = build('youtube', 'v3', developerKey=API_KEY)

# Directories
TOKEN_DIR = 'data/tokens'
os.makedirs(TOKEN_DIR, exist_ok=True)
LAST_KEYWORD_FILE = 'data/last_keyword.json'
VIDEO_IDS_FILE = 'data/video_ids.json'


# Load the video IDs from the JSON file and return as a set (for fast lookups and uniqueness)
def load_existing_video_ids():
    if os.path.exists(VIDEO_IDS_FILE):
        with open(VIDEO_IDS_FILE, 'r') as f:
            return set(json.load(f))  # Use a set for fast lookups
    return set()


def save_video_ids(video_ids):
    existing_ids = load_existing_video_ids()  # Load the existing IDs
    new_ids = existing_ids.union(set(video_ids))  # Add new IDs to the existing set (avoids duplicates)

    # Save the updated set of video IDs to the file
    with open(VIDEO_IDS_FILE, 'w') as f:
        json.dump(list(new_ids), f)  # Convert set back to list for JSON serialization



# Token Management
def save_next_page_token(keyword, token):
    # Define the file path to save the nextPageToken for a specific keyword
    token_file = os.path.join(TOKEN_DIR, f'next_page_token_{keyword}.json')
    
    # Open the file in write mode and save the nextPageToken as a JSON object
    with open(token_file, 'w') as f:
        json.dump({'nextPageToken': token}, f)
    print(f"Saved nextPageToken for {keyword}: {token}")



# Define the file path for loading the nextPageToken for a specific keyword
def load_next_page_token(keyword):
    token_file = os.path.join(TOKEN_DIR, f'next_page_token_{keyword}.json')
    if os.path.exists(token_file):
        with open(token_file, 'r') as f:
            data = json.load(f)
            return data.get('nextPageToken')
    return None


def remove_next_page_token(keyword):
    token_file = os.path.join(TOKEN_DIR, f'next_page_token_{keyword}.json')
    if os.path.exists(token_file):
        os.remove(token_file)
        print(f"Removed nextPageToken file for {keyword}")


# Save/Load Last Processed Keyword
def save_last_keyword(keyword):
    with open(LAST_KEYWORD_FILE, 'w') as f:
        json.dump({'lastKeyword': keyword}, f)
    print(f"Saved last processed keyword: {keyword}")


def load_last_keyword():
    if os.path.exists(LAST_KEYWORD_FILE):
        with open(LAST_KEYWORD_FILE, 'r') as f:
            data = json.load(f)
            return data.get('lastKeyword')
    return None


# Main Data Fetching Function
def get_data(query, max_results=50):
    video_data = []
    next_page_token = load_next_page_token(query)
    existing_video_ids = load_existing_video_ids()  # Get the already fetched IDs
    page_count = 0
    MAX_PAGES_PER_KEYWORD = 50  # Safety limit
    video_ids = []

    try:
        while page_count < MAX_PAGES_PER_KEYWORD:
            try:
                search_response = youtube.search().list(
                    q=query,
                    part="id",  #getting only id to reduce response
                    type="video",
                    videoCategoryId="10",
                    regionCode="EG",
                    relevanceLanguage="ar",
                    safeSearch="none",
                    maxResults=max_results,
                    location="26.8206,30.8025", #central cordiniation for efypt
                    locationRadius="500km",
                    duration="medium",
                    order="viewCount",
                    pageToken=next_page_token
                ).execute()
            except Exception as api_error:
                print(f"API quota exceeded or error occurred: {api_error}")
                save_last_keyword(query)
                return  # Stop the script save last keyword and resume the next day
       
       
       
        # Collect video IDs, but check for duplicates first
            for item in search_response['items']:
                video_id = item['id']['videoId']
                if video_id and video_id not in existing_video_ids:
                    video_ids.append(video_id)

            # Get video details
            video_response = youtube.videos().list(
                part="snippet,statistics,contentDetails,recordingDetails",
                id=",".join(video_ids)
            ).execute()

            for video in video_response['items']:
                video_details = {
                    'Title': video['snippet']['title'],
                    'Description': video['snippet']['description'],
                    'Tags': video['snippet'].get('tags', []),
                    'Channel ID': video['snippet']['channelId'],
                    'Channel': video['snippet']['channelTitle'],
                    'Location': video['recordingDetails'].get('locationDescription', 'N/A'),
                    'Duration': video['contentDetails']['duration'],
                    'Definition': video['contentDetails']['definition'],
                    'Caption': video['contentDetails']['caption'],
                    'Licensed Content': video['contentDetails']['licensedContent'],
                    'Views': video['statistics'].get('viewCount', 'N/A'),
                    'Likes': video['statistics'].get('likeCount', 'N/A'),
                    'Comments': video['statistics'].get('commentCount', 'N/A'),
                    'Published At': video['snippet']['publishedAt'],
                    'Keyword': query
                }
                video_data.append(video_details)

            # Save data incrementally
            if video_data:
                df = pd.DataFrame(video_data)
                df.to_csv(
                    'data/youtube_music_data_egypt.csv',
                    index=False,
                    mode='a',
                    header=not os.path.exists('data/youtube_music_data_egypt.csv')
                )
                video_data = []  # Clear buffer

            # Update Token and Pagination
            next_page_token = search_response.get('nextPageToken')
            if next_page_token:
                save_next_page_token(query, next_page_token)
            else:
                remove_next_page_token(query)
                print(f"Finished all pages for {query}.")
                break

            page_count += 1

    except Exception as e:
        print(f"An error occurred: {e}")
        save_last_keyword(query)


# List of Keywords
keywords = ["Music", "اغاني", "مهرجانات", "rap", "Trap"]

# Start from last processed keyword if available
last_keyword = load_last_keyword()
if last_keyword and last_keyword in keywords:
    start_index = keywords.index(last_keyword)
else:
    start_index = 0

# Process keywords
for keyword in keywords[start_index:]:
    print(f"Starting data fetch for keyword: {keyword}")
    get_data(keyword)
    print(f"Completed keyword: {keyword}")
    save_last_keyword(keyword)  # Update last keyword after each completion

# Clear last keyword if everything is done
if os.path.exists(LAST_KEYWORD_FILE):
    os.remove(LAST_KEYWORD_FILE)
    print("All keywords processed. Last keyword file removed.")
