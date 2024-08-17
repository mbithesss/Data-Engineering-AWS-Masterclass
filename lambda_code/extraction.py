import requests  # Importing the requests library to make HTTP requests
import json  # Importing the json library to handle JSON data
import pandas as pd  # Importing pandas for data manipulation and analysis
import datetime  # Importing datetime to handle date and time
import s3_file_operations as s3_ops  # Importing custom S3 operations module

def extract_data(api_url, table_name):
    page = 1  # Initialize the page number
    next = True  # Initialize the loop control variable
    all_data = []  # Initialize an empty list to store all data

    while next:  # Loop until there are no more pages
        print(f"Extracting page {page} Data from {table_name}.....")
        response = requests.get(f"{api_url}?page={str(page)}")  # Make a GET request to the API
        data = response.json().get('results', [])  # Extract the 'results' from the JSON response
        all_data.extend(data)  # Add the data to the all_data list

        if response.json().get('info', {}).get("next") is not None:  # Check if there is a next page
            page += 1  # Increment the page number
        else:
            break  # Exit the loop if there are no more pages

    return pd.DataFrame(all_data)  # Convert the list of data to a pandas DataFrame

def save_to_s3(df, bucket, table_name, timestamp):
    year = str(timestamp.year)  # Extract the year from the timestamp
    month = str(timestamp.month).zfill(2)  # Extract the month and pad with zero if needed
    date = str(timestamp.day).zfill(2)  # Extract the day and pad with zero if needed
    hour = str(timestamp.hour).zfill(2)  # Extract the hour and pad with zero if needed
    minute = str(timestamp.minute).zfill(2)  # Extract the minute and pad with zero if needed

    # Write the DataFrame to S3 with a specific key format
    s3_ops.write_data_to_s3(df,
                            bucket_name=bucket,
                            key=f"Rick&Morty/{table_name}/{year}/{month}/{date}/{hour}/{minute}/{table_name}.csv")

def update_logs(logs, table_name, df, timestamp):
    # Create a new log entry as a DataFrame
    new_log = pd.DataFrame([{
        "Table_Name": table_name,  # Name of the table
        "latest_idx": df['id'].max() if 'id' in df.columns else None,  # Latest ID in the DataFrame
        "latest_df_size": df.memory_usage(deep=True).sum(),  # Memory usage of the DataFrame
        "rows": len(df),  # Number of rows in the DataFrame
        "pipeline_run_time": str(timestamp),  # Timestamp of the pipeline run
        "stage": "injestion",  # Stage of the pipeline
        "cols_size": len(df.columns)  # Number of columns in the DataFrame
    }])
    logs = pd.concat([logs, new_log], ignore_index=True)  # Append the new log entry to the logs DataFrame
    return logs  # Return the updated logs DataFrame

def lambda_handler(event, context):
    print("Starting Extraction....")
    bucket = "de-masterclass"  # S3 bucket name
    # Initialize an empty DataFrame for logs with specified columns
    logs = pd.DataFrame(columns=["Table_Name", "latest_idx", "latest_df_size", "rows", "pipeline_run_time", "stage", "cols_size"])

    # Dictionary of tables and their corresponding API URLs
    tables = {
        "Character": "https://rickandmortyapi.com/api/character",
        "Location": "https://rickandmortyapi.com/api/location",
        "Episode": "https://rickandmortyapi.com/api/episode"
    }

    for table_name, api_url in tables.items():  # Loop through each table and its API URL
        df = extract_data(api_url, table_name)  # Extract data from the API
        timestamp = datetime.datetime.now()  # Get the current timestamp
        save_to_s3(df, bucket, table_name, timestamp)  # Save the DataFrame to S3
        logs = update_logs(logs, table_name, df, timestamp)  # Update the logs DataFrame
        print(f"Data for {table_name} successfully saved in S3.. you can go check it out...")

    # Save the logs DataFrame to S3 as Logs.csv
    s3_ops.write_data_to_s3(logs, bucket_name=bucket, key="Rick&Morty/Logs/Logs.csv")

    return {
        'statusCode': 200,  # Return status code 200
        'body': json.dumps('Success!')  # Return success message
    }