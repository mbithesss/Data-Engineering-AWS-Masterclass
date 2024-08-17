import datetime
import pandas as pd
from smart_open import open
import pymysql.cursors
# import rds_config
import s3_file_operations as s3_ops

rds_host = "rick-and-morty-db.ctk2ewoqitft.eu-west-1.rds.amazonaws.com"
rds_username = "admin"
rds_user_pwd = "Rick&Morty123"
rds_db_name = "rick_and_morty"
bucket_name = "de-masterclass"


def character_loader():
    # path = f"s3://{s3_ops.bucket_name}/Rick&Morty/Untransformed/Character.csv"
    # latest_records = pd.read_csv(open(path))
    latest_records = s3_ops.read_csv_from_s3(bucket_name, "Rick&Morty/Untransformed/Character.csv")
    latest_records = latest_records.fillna('NULL')

    create_sql_script = """
        CREATE TABLE Character_Table (
            id INT NOT NULL PRIMARY KEY,
            name VARCHAR(255),
            status VARCHAR(255),
            species VARCHAR(255),
            type VARCHAR(255),
            gender VARCHAR(255),
            origin VARCHAR(255),
            location VARCHAR(255),
            image VARCHAR(255),
            episode VARCHAR(255),
            url VARCHAR(255),
            created VARCHAR(255)) 
        ENGINE=INNODB;
    """

    try:
        conn = pymysql.connect(host=rds_host,
                               user=rds_username,
                               password=rds_user_pwd,
                               port=3306,
                               database=rds_db_name,
                               cursorclass=pymysql.cursors.DictCursor)
        cursor = conn.cursor()

        cursor.execute("DROP TABLE IF EXISTS Character_Table")
        cursor.execute(create_sql_script)
        
        # define the list of columns to insert data into
        column_names = list(latest_records.columns)
        
        for i, row in latest_records.iterrows():
            # create a string with the correct number of placeholders for the data
            placeholders = ','.join(['%s']*len(column_names))
            # create the SQL insert statement with the specified column names and placeholders
            sql_insert = f"INSERT INTO Character_Table ({','.join(column_names)}) VALUES ({placeholders});"
            # extract the data for the specified columns from the row
            data = tuple(row[column] for column in column_names)
            # execute the insert statement with the data
            cursor.execute(sql_insert, data)
            conn.commit()
            
        print(f"finished insertion of {latest_records.shape[0]} Character records to RDS...")
            

    except Exception as e:
        print("Exception: ", e)


character_loader()