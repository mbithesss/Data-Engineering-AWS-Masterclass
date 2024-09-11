# Data Engineering on AWS Masterclass

The main goal of this Masterclass is to utilize data engineering skills to power an analytics workload on the AWS Cloud platform.

We are going to extract Rick and Morty series data from an open-source API, apply some transformations to the data, and model a few tables to load into our data warehouse to help answer some analytics questions. The data will be modeled according to the ERD diagram attached in this project folder. Although we are using MySQL, a transactional database, instead of PostgreSQL, an analytics database, this is just an illustration project for the masterclass. We will use the free tier option for creating the RDS instance to maintain cost.

### Learning Objectives

1. Designing a sample real-world data platform on AWS
2. Designing an architecture diagram
3. Ingesting data from REST using Python code in AWS Lambda
4. Staging data using AWS Simple Storage Service (S3)
5. Designing a simple MySQL engine data warehouse using AWS RDS
6. Job scheduling using AWS EventBridge
7. Running queries from our modeled data warehouse

### Workload Requirements

1. Ingesting data in near real-time - We will need lightweight compute resources for this to run simple Python scripts. AWS Lambda Serverless Service will be an ideal solution for this.
2. We need to stage this data somewhere for the next pipeline task to pick it up. AWS S3 Object storage will be ideal for this.
3. We need to get the extracted data and load it into a simple data warehouse. We will use Lambda for this as well and connect it to a MySQL DB to mimic our data warehouse.
4. We will need to visualize this data.
5. We will need to connect all these stages and automate them using schedulers that we can trigger the start and the rest flows. We will use AWS EventBridge Service to achieve this.
6. Connect to our modeled data warehouse using any SQL client applications and run some SQL queries to answer a few analytics questions.

# DATA INGESTION

### A. Working with AWS Lambda

We will be reading Rick & Morty data from an open-source API, and we will then use AWS Serverless compute service called **AWS Lambda** to help in achieving our ETL needs.

1. Navigate to your AWS Management Console and search for AWS Lambda service, make sure you are in the ***Ireland Region (eu-west-1)***.
2. Create a Lambda Function on AWS, use the following specifications:
    - Use Author From Scratch Approach
    - Runtime: **Python 3.12**
    - Architecture: **x86_64**
    - Execution Role: ***Create a new role with basic Lambda permissions***

3. To use Python Libraries such as Pandas for data wrangling, we need to add a Layer to our Lambda Function for this:
    - Scroll down to the layers section and click on **Add a Layer**
    - Select **AWS Layer**
    - From the drop-down, select **AWSSDKPandas-Python311** then select the latest version

4. Make sure to create a new Python file from the lambda file project folder called ***s3_file_operations.py*** to help us in writing data to an S3 bucket. You can find the script under the **utils** folder in this repository.
5. To increase our Timeout Limit, navigate to the ***Configuration*** section, click on Edit, set **15 minutes** maximum timeout, and click on save.
6. We are also going to need to grant Lambda permissions to access AWS S3 Service, we do this by attaching an ***AmazonS3FullAccess*** Policy to our execution role.

### B. Creating an S3 Bucket

1. Head on to your AWS Management Console and search for AWS S3.
2. Click on create a new Bucket, name your bucket with a unique name e.g. ***de-masterclass-XX*** (XX - can be your name initials).
3. Disable ACLs (Access Control Lists).
4. Turn off ***Block all Public Access check box - Not Recommended though***.
5. Enable Versioning.
6. Encryption: ***Server-side encryption with Amazon S3 managed keys (SSE-S3)***.
7. Bucket Key: ***Enabled***.
8. Finally, click on Create Bucket.

Access your S3 Bucket and inside of it create a folder called Rick&Morty.

### C. Creating an AWS Relational Database Service (RDS) Instance

1. Navigate to your AWS Management Console and search for RDS.
2. Click on "Create database".
3. Choose "Standard Create".
4. Engine options: Select **MySQL**.
5. Version: Select the latest available version.
6. Templates: Select **Free tier**.
7. Settings:
    - DB instance identifier: `rick-and-morty-db`
    - Master username: `admin`
    - Master password: `your_password` (replace with a secure password)
8. DB instance size: Select **db.t2.micro** (Free tier eligible).
9. Storage: 
    - Storage type: **General Purpose (SSD)**
    - Allocated storage: **20 GiB** (Free tier eligible)
10. Connectivity:
    - Virtual Private Cloud (VPC): Default VPC
    - Public access: **Yes**
    - VPC security group: Create a new security group
11. Additional configuration:
    - Initial database name: `rick_and_morty`
    - Backup retention period: **7 days**
    - Enable Enhanced monitoring: **No**
    - Maintenance window: No preference
12. Click on "Create database".

Once the database is created, note down the endpoint address. You will use this endpoint to connect to the database from your Lambda function.

**Note**: Edit the instance security group to allow MySQL connection inbound traffic from your IP address location. To do this:
1. Navigate to the RDS instance in the AWS Management Console.
2. Click on the instance to view its details.
3. Under the "Connectivity & security" tab, find the "VPC security groups" section and click on the security group link.
4. In the security group details, go to the "Inbound rules" tab and click on "Edit inbound rules".
5. Add a new rule with the following details:
    - Type: MySQL/Aurora
    - Protocol: TCP
    - Port range: 3306
    - Source: My IP (or specify your IP address)
6. Click on "Save rules".

### D. Connecting Everything Together

Now that we have set up our S3 bucket and RDS instance, we need to create three Lambda functions for the ETL process: one for extraction, another for transformations, and the other for loading.

1. **Extraction Lambda Function**: This function will extract data from the Rick and Morty API and save it to the S3 bucket.
    - Refer to the code in `Rick_and_Morty_Extraction.py` for the implementation.

2. **Transformation Lambda Function**: This function will read the raw data from S3, apply the necessary transformations, and save the transformed data back to S3.
    - Refer to the code in `Rick_and_Morty_Extraction.py` for the implementation.

3. **Loading Lambda Function**: This function will read the transformed data from S3 and load it into the RDS instance.
    - Refer to the code in `Rick_and_Morty_Loading.py` for the implementation:

### Testing the Pipeline

1. **Invoke the Extraction Lambda Function**: Test the extraction function to ensure it successfully extracts data from the API and saves it to the S3 bucket.
2. **Invoke the Transformation Lambda Function**: Test the transformation function to ensure it reads the raw data from S3, applies the transformations, and saves the transformed data back to S3.
3. **Invoke the Loading Lambda Function**: Test the loading function to ensure it reads the transformed data from S3 and loads it into the RDS instance.

By following these steps, you will have a fully functional data pipeline that extracts data from an API, transforms it, and loads it into a MySQL database on AWS RDS. This setup will help you understand the end-to-end process of building a data engineering solution on AWS.
