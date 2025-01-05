import os as os
import pandas as pd
from googleapiclient.discovery import build


## cr
API_KEY = os.getenv("API_KEY")
if API_KEY is None:
    raise ValueError("API_KEY environment variable is not set")


youtube = build('youtube', 'v3', developerKey=API_KEY)



def get_test_data(query, max_results=2):
    try:
        response = youtube.search().list(
        q=query,
        part="snippet",
        type="video",
        maxResults=max_results
        ).execute()

        print(response)
    except Exception as e:
        print(f"An error occurred: {e}")

get_test_data("test")
