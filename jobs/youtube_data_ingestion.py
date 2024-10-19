import json
import os
import boto3
from datetime import datetime
from fetch_data_from_api import get_trending_video_data, get_video_categories

# Define your environment variables for YouTube API key and S3 bucket
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')
BUCKET_NAME = os.getenv('BUCKET_NAME')

def fetch_and_upload_youtube_data(): 
    youtube_regions = [
        "AR",  # Argentina
        "AU",  # Australia
        "AT",  # Austria
        "BE",  # Belgium
        "BR",  # Brazil
        "CA",  # Canada
        "CL",  # Chile
        "CO",  # Colombia
        "CZ",  # Czech Republic
        "DK",  # Denmark
        "EG",  # Egypt
        "FI",  # Finland
        "FR",  # France
        "DE",  # Germany
        "GH",  # Ghana
        "GR",  # Greece
        "HK",  # Hong Kong
        "HU",  # Hungary
        "IN",  # India
        "ID",  # Indonesia
        "IE",  # Ireland
        "IL",  # Israel
        "IT",  # Italy
        "JP",  # Japan
        "KE",  # Kenya
        "KR",  # South Korea
        "MY",  # Malaysia
        "MX",  # Mexico
        "NL",  # Netherlands
        "NZ",  # New Zealand
        "NG",  # Nigeria
        "NO",  # Norway
        "PE",  # Peru
        "PH",  # Philippines
        "PL",  # Poland
        "PT",  # Portugal
        "RO",  # Romania
        "RU",  # Russia
        "SA",  # Saudi Arabia
        "SG",  # Singapore
        "ZA",  # South Africa
        "ES",  # Spain
        "SE",  # Sweden
        "CH",  # Switzerland
        "TW",  # Taiwan
        "TH",  # Thailand
        "TR",  # Turkey
        "UG",  # Uganda
        "UA",  # Ukraine
        "AE",  # United Arab Emirates
        "GB",  # United Kingdom
        "US",  # United States
        "VN",  # Vietnam
        "ZW"  # Zimbabwe
    ]

    s3 = boto3.client('s3')

    for region in youtube_regions:
        items_json = json.dumps(get_trending_video_data(regionCode=region))
        categories_json = json.dumps(get_video_categories(regionCode=region))

        upload_date = datetime.now().strftime('year=%Y/month=%m/day=%d')
        key_prefix_video = f'trending-videos/region={region}/{upload_date}/data.json'   
        key_prefix_categories = f'video-categories/region={region}/{upload_date}/data.json'   

        print(f"Dữ liệu khu vực {region} đang được tải lên S3")
        s3.put_object(Bucket=BUCKET_NAME, Key=key_prefix_video, Body=items_json)
        s3.put_object(Bucket=BUCKET_NAME, Key=key_prefix_categories, Body=categories_json)

        print(f"Dữ liệu khu vực {region} đã tải lên S3: s3://{BUCKET_NAME}/{key_prefix_video} và  s3://{BUCKET_NAME}/{key_prefix_categories}")

if __name__ == "__main__":
    fetch_and_upload_youtube_data()