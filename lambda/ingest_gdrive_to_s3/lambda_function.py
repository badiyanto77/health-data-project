import boto3
from googleapiclient.discovery import build
from google.oauth2 import service_account
from botocore.exceptions import ClientError

# Config
FOLDER_ID = '1vjUpextEZWjX2DZEueRshk2GRAAmIQ1G'  # your new Google Drive folder ID
S3_BUCKET = 'health-data-project-bucket'
S3_PREFIX = 'data/'  # target folder in S3

def lambda_handler(event, context):
    # Authenticate with Google
    creds = service_account.Credentials.from_service_account_file(
        'credentials.json',
        scopes=['https://www.googleapis.com/auth/drive']
    )
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