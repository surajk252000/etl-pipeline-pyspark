import boto3
import os

s3 = boto3.client('s3')
BUCKET = 'etl-end-to-end-project'

def upload_to_s3():
    files = {
        'data/employees.csv': 'raw/employees.csv',
        'data/departments.json': 'raw/departments.json'
    }
    
    for local_path, s3_path in files.items():
        if os.path.exists(local_path):
            s3.upload_file(local_path, BUCKET, s3_path)
            print(f" Uploaded {local_path} → s3://{BUCKET}/{s3_path}")
        else:
            print(f" File not found: {local_path}")

if __name__ == "__main__":
    upload_to_s3()