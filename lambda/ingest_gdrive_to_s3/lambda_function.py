"""
===============================================================================
    Lambda Name : gdrive_to_s3_csv_sync
    Author      : Bagus Adiyanto
    Created On  : 2025-10-07
    Last Updated: 2025-10-07

    Summary:
    --------
    This AWS Lambda function automates the ingestion of CSV files from a 
    specified Google Drive folder into an Amazon S3 bucket. The workflow 
    includes the following key steps:

    1. Authenticate with Google Drive using a service account.
    2. List all CSV files in the configured Google Drive folder.
    3. For each file:
         - Check if the file already exists in the target S3 bucket/prefix.
         - If not present, download the file from Google Drive.
         - Upload the file into Amazon S3.
    4. Return a summary of uploaded files.

    Notes:
    ------
    - Skips files that already exist in S3 (idempotent uploads).
    - Requires a valid `credentials.json` service account key packaged 
      with the Lambda deployment.
    - IAM role must allow `s3:HeadObject` and `s3:PutObject`.
    - Only processes files with MIME type `text/csv`.
===============================================================================
"""

import boto3
import json
from googleapiclient.discovery import build
from google.oauth2 import service_account
from botocore.exceptions import ClientError

# Config
FOLDER_ID = '1vjUpextEZWjX2DZEueRshk2GRAAmIQ1G'  # your Google Drive folder ID
S3_BUCKET = 'health-data-project-bucket'
S3_PREFIX = 'data/'  # target folder in S3

def get_gdrive_credentials():
    """
    Load Google Drive credentials from AWS Secrets Manager.
    The secret should contain the full JSON content of credentials.json.
    """
    secret_name = 'gdrive_credential_json'  # your secret name
    region_name = boto3.session.Session().region_name

    client = boto3.client('secretsmanager', region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    secret_dict = json.loads(response['SecretString'])

    # Create credentials from the JSON secret
    creds = service_account.Credentials.from_service_account_info(
        secret_dict,
        scopes=['https://www.googleapis.com/auth/drive']
    )
    return creds

def lambda_handler(event, context):
    # ✅ Authenticate with Google using credentials from Secrets Manager
    creds = get_gdrive_credentials()
    service = build('drive', 'v3', credentials=creds)

    # List CSV files in the Google Drive folder
    results = service.files().list(
        q=f"'{FOLDER_ID}' in parents and mimeType='text/csv'",
        fields="files(id, name)"
    ).execute()
    files = results.get('files', [])

    s3 = boto3.client('s3')
    uploaded = []

    for file in files:
        s3_key = f"{S3_PREFIX}{file['name']}"

        # Check if file already exists in S3
        try:
            s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
            print(f"Skipping {file['name']} (already exists in S3)")
            continue  # Skip upload if file exists
        except ClientError as e:
            if e.response['Error']['Code'] != "404":
                raise  # Only ignore "Not Found" errors

        # Download file from Google Drive
        file_data = service.files().get_media(fileId=file['id']).execute()

        # Upload to S3
        s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=file_data)
        print(f"Uploaded {file['name']} to s3://{S3_BUCKET}/{s3_key}")
        uploaded.append(file['name'])

    return {
        'status': 'success',
        'files_uploaded': uploaded,
        'count': len(uploaded)
    }
