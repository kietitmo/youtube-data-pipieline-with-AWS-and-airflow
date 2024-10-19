import requests
import json
import os

YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')
BUCKET_NAME = os.getenv('BUCKET_NAME')

def get_data_per_page(regionCode, pageToken=None):
    url = f"https://www.googleapis.com/youtube/v3/videos?part=id,snippet,statistics&chart=mostPopular&regionCode={regionCode}&key={YOUTUBE_API_KEY}"
    if pageToken:
        url += f"&pageToken={pageToken}"
    res = requests.get(url)
    data = json.loads(res.content)
    return (data.get('nextPageToken'), data['items'])

def get_trending_video_data(regionCode):
    all_items = []
    next_page_token, items = get_data_per_page(regionCode)
    all_items.extend(items)
    while next_page_token:
        next_page_token, items = get_data_per_page(regionCode, next_page_token)
        all_items.extend(items)
    return all_items

def get_video_categories(regionCode):
    res = requests.get(f"https://www.googleapis.com/youtube/v3/videoCategories?part=snippet&regionCode={regionCode}&key={YOUTUBE_API_KEY}")
    return json.loads(res.content)