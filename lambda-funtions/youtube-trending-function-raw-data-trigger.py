import json
import boto3
import awswrangler as wr
import pandas as pd
from urllib.parse import unquote_plus

s3 = boto3.client('s3')

def lambda_handler(event, context):
    output_bucket = 'kietitmo-youtube-clean-data'
    
    # Lấy thông tin bucket và key từ event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = unquote_plus(event['Records'][0]['s3']['object']['key'])

    print(bucket)
    print(key)
    
    # Đọc file JSON từ S3
    s3_object = s3.get_object(Bucket=bucket, Key=key)
    file_content = s3_object['Body'].read().decode('utf-8')
    
    if ('video-categories/' in key):
        print('Processing categories')
        # Chuyển đổi JSON thành dictionary
        data = json.loads(file_content)

        # Lưu dữ liệu thành file Parquet
        df = pd.json_normalize(data['items'])
        output_path = f"s3://{output_bucket}/{key.replace('.json', '.parquet')}"
        wr.s3.to_parquet(df=df, path=output_path, dataset=False)

        return {
            'response code': 200,
            'msg': 'Processed successfully',
            'key': key,
        }
    
    if ('trending-videos/' in key):
        print('Processing trending videos')
        data = json.loads(file_content)
        df = pd.json_normalize(data)
        
        df_2 = df[['id', 'snippet.publishedAt', 'snippet.channelId', 'snippet.title', 'snippet.description', 'snippet.categoryId', 'snippet.defaultLanguage', 'statistics.viewCount', 'statistics.likeCount', 'statistics.commentCount']]
        
        df_2['id'].fillna('none')
        df_2['snippet.publishedAt'].fillna('none')
        df_2['snippet.channelId'].fillna('none')
        df_2['snippet.title'].fillna('none')
        df_2['snippet.description'].fillna('none')
        df_2['snippet.categoryId'].fillna('none')
        df_2['snippet.defaultLanguage'].fillna('none')
        df_2['statistics.commentCount'].fillna(0)
        df_2['statistics.viewCount'].fillna(0)
        df_2['statistics.likeCount'].fillna(0)

        # Lưu dữ liệu thành file Parquet
        output_path = f"s3://{output_bucket}/{key.replace('.json', '.parquet')}"
        wr.s3.to_parquet(df=df_2, path=output_path, dataset=False)

        return {
            'response code': 200,
            'msg': 'Processed successfully',
            'key': key,
        }

    return {
        'response code': 400,
        'msg': 'No valid key found',
        'key': key,
    }
