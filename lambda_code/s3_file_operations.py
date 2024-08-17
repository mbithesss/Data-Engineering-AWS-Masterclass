import pandas as pd
import boto3
import os
from io import StringIO
import io
from botocore.exceptions import ClientError


# s3 bucket details
bucket_name = 'de-masterclass'


def write_data_to_s3(dataframe, bucket_name, key):
    # Convert the dataframe to a string
    csv_buffer = StringIO()
    dataframe.to_csv(csv_buffer, index=False)

    # Creating an S3 client object
    s3 = boto3.client('s3')

    try:
        # Writing the data to S3
        s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=key)
        print(f"Data saved to S3 with bucket_name: {bucket_name} and key: {key}")
        return True
    except ClientError as e:
        print(e)
        return False



# Defining the function to read the CSV file from S3
def read_csv_from_s3(bucket_name, key):
    # Creating an S3 client object
    s3 = boto3.client('s3')

    try:
        # Retrieving the file object from S3
        df_file = s3.get_object(Bucket=bucket_name, Key=key)
        
        # Check if the file is empty
        if df_file['ContentLength'] == 0:
            print(f"The file {key} is empty. Skipping.")
            return pd.DataFrame()  # Return an empty DataFrame
        
        # Reading the CSV data into a pandas dataframe
        df = pd.read_csv(df_file['Body'])
        return df

    except ClientError as e:
        print(e)
        return False
    except pd.errors.EmptyDataError:
        print(f"The file {key} is empty. Skipping.")
        return pd.DataFrame()  # Return an empty DataFrame