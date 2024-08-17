import json
import pandas as pd
import boto3
from io import StringIO
import s3_file_operations as s3_ops

def lambda_handler(event, context):
    bucket = "de-masterclass"  # S3 bucket name

    print("Starting data transformation...")

    # Read data from S3
    print("Reading Character data from S3...")
    characters_df = s3_ops.read_csv_from_s3(bucket, 'Rick&Morty/Untransformed/Character.csv')
    print("Reading Location data from S3...")
    locations_df = s3_ops.read_csv_from_s3(bucket, 'Rick&Morty/Untransformed/Location.csv')
    print("Reading Episode data from S3...")
    episodes_df = s3_ops.read_csv_from_s3(bucket, 'Rick&Morty/Untransformed/Episode.csv')

    # Check if data is loaded successfully
    if characters_df is None or locations_df is None or episodes_df is None:
        print("Error in loading data from S3")
        return {
            'statusCode': 500,
            'body': json.dumps('Error in loading data from S3')
        }

    print("Data loaded successfully from S3")

    # Extract origin and location from characters
    print("Extracting origin and location from characters...")
    characters_df['origin_name'] = characters_df['origin'].apply(lambda x: json.loads(x)['name'])
    characters_df['location_name'] = characters_df['location'].apply(lambda x: json.loads(x)['name'])

    # Create origin and location tables
    print("Creating origin and location tables...")
    origin_df = characters_df[['origin_name']].drop_duplicates().reset_index(drop=True)
    location_df = characters_df[['location_name']].drop_duplicates().reset_index(drop=True)

    # Create character-episode relationship table
    print("Creating character-episode relationship table...")
    characters_df['episode'] = characters_df['episode'].apply(lambda x: json.loads(x))
    character_episode_df = characters_df[['id', 'episode']].explode('episode')
    character_episode_df['episode'] = character_episode_df['episode'].apply(lambda x: int(x.split('/')[-1]))

    # Create location-resident relationship table
    print("Creating location-resident relationship table...")
    locations_df['residents'] = locations_df['residents'].apply(lambda x: json.loads(x))
    location_resident_df = locations_df[['id', 'residents']].explode('residents')
    location_resident_df['residents'] = location_resident_df['residents'].apply(lambda x: int(x.split('/')[-1]))

    # Drop JSON columns from main tables
    print("Dropping JSON columns from main tables...")
    characters_df = characters_df.drop(columns=['origin', 'location', 'episode'])
    locations_df = locations_df.drop(columns=['residents'])
    episodes_df = episodes_df.drop(columns=['characters'])

    # Save transformed data to S3
    print("Saving transformed Character data to S3...")
    s3_ops.write_data_to_s3(characters_df, bucket, 'Rick&Morty/Transformed/Character.csv')
    print("Saving transformed Origin data to S3...")
    s3_ops.write_data_to_s3(origin_df, bucket, 'Rick&Morty/Transformed/Origin.csv')
    print("Saving transformed Location data to S3...")
    s3_ops.write_data_to_s3(location_df, bucket, 'Rick&Morty/Transformed/Location.csv')
    print("Saving transformed CharacterEpisode data to S3...")
    s3_ops.write_data_to_s3(character_episode_df, bucket, 'Rick&Morty/Transformed/CharacterEpisode.csv')
    print("Saving transformed LocationResident data to S3...")
    s3_ops.write_data_to_s3(location_resident_df, bucket, 'Rick&Morty/Transformed/LocationResident.csv')

    print("Transformation and saving to S3 successful!")

    return {
        'statusCode': 200,
        'body': json.dumps('Transformation and saving to S3 successful!')
    }