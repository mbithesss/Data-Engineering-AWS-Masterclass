import requests
import json
import pandas as pd
import datetime
import s3_file_operations as s3_ops

def extract_data(api_url, table_name):
    page = 1
    next = True
    all_data = []

    while next:
        print(f"Extracting page {page} Data from {table_name}.....")
        response = requests.get(f"{api_url}?page={str(page)}")
        data = response.json().get('results', [])
        all_data.extend(data)

        if response.json().get('info', {}).get("next") is not None:
            page += 1
        else:
            break

    return pd.DataFrame(all_data)

def save_to_s3(df, bucket, table_name):

    s3_ops.write_data_to_s3(df,
                            bucket_name=bucket,
                            key=f"Rick&Morty/Untransformed/{table_name}.csv")

def lambda_handler(event, context):
    print("Starting Extraction....")
    bucket = "de-masterclass" # s3 bucket name

    tables = {
        "Character": "https://rickandmortyapi.com/api/character",
        "Location": "https://rickandmortyapi.com/api/location",
        "Episode": "https://rickandmortyapi.com/api/episode"
    }

    for table_name, api_url in tables.items():
        df = extract_data(api_url, table_name)
        save_to_s3(df, bucket, table_name)
        print(f"Data for {table_name} successfully saved in S3.. you can go check it out...")


    return {
        'statusCode': 200,
        'body': json.dumps('Success!')
    }