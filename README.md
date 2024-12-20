# Movie Data Analysis Pipeline Using AWS Glue

## Overview

This project automates the ETL process for a movie dataset using AWS services. It evaluates data quality, separates valid and invalid data, and stores the results in Amazon Redshift and S3 for further analysis.

---

## Architecture

1. **Amazon S3**: Stores the raw movie dataset and processed output files.
2. **AWS Glue**: Performs data quality evaluation, transformation, and ETL.
3. **Amazon Redshift**: Stores the valid, transformed data for analytics.
4. **AWS CLI**: Used to automate resource creation and management.

---

## Prerequisites

1. AWS Account
2. AWS CLI installed and configured
3. IAM role with the following permissions:
    - S3 access
    - Glue access
    - Redshift access

---

## Installation and Setup

### Step 1: Install AWS CLI

To install the AWS CLI on macOS:
```bash
curl "https://awscli.amazonaws.com/AWSCLIV2.pkg" -o "AWSCLIV2.pkg"
sudo installer -pkg AWSCLIV2.pkg -target /
```

### Step 2: Configure AWS CLI

Set up AWS CLI by running:
```bash
aws configure
```

### Step 3: Create IAM Role

Create an IAM role with the following policies:
- AmazonS3FullAccess
- AWSGlueServiceRole
- AmazonRedshiftFullAccess


### Steps to Execute the Project

### Step 1: Setup AWS Resources

1. Create S3 Buckets
```bash
aws s3 mb s3://movie-data-analysis
aws s3 mb s3://movie-data-analysis/temp
```

2. Create Folders in S3
```bash
aws s3api put-object --bucket movie-data-analysis --key input/
aws s3api put-object --bucket movie-data-analysis --key output/
aws s3api put-object --bucket movie-data-analysis --key invalid-data/
```

3. Upload Dataset to S3
```bash
aws s3 cp /path/to/movie_dataset.csv s3://movie-data-analysis/input/
```


### Step 2: Create Redshift Table
1. Open the Redshift Query Editor.
2. Run the SQL command given in file "ddl_create_table.sql" to create the table.


### Step 3: AWS Glue Setup

1. Create Glue Job
```bash
aws glue create-job \
    --name movie_data_analysis_glue_job \
    --role <YOUR_GLUE_SERVICE_ROLE> \
    --command '{"Name": "glueetl", "ScriptLocation": "s3://movie-data-analysis/scripts/glue_job_script.py", "PythonVersion": "3"}'
```

2. Start Glue Job
```bash
aws glue start-job-run --job-name movie_data_analysis_glue_job
```