
from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'suraj',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

@dag(
    dag_id='etl_pipeline_dag',
    default_args=default_args,
    description='End-to-End ETL Pipeline - CSV/JSON to MySQL via S3',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'python', 'mysql', 's3'],
)
def etl_pipeline():

    @task()
    def upload_raw_files():
        import boto3
        import os
        s3 = boto3.client('s3')
        bucket = 'etl-end-to-end-project'
        files = {
            '/opt/airflow/data/employees.csv': 'raw/employees.csv',
            '/opt/airflow/data/departments.json': 'raw/departments.json'
        }
        for local_path, s3_path in files.items():
            s3.upload_file(local_path, bucket, s3_path)
            print(f"✅ Uploaded {local_path} → s3://{bucket}/{s3_path}")
        return "Upload complete"

    @task()
    def transform_data(upload_status: str):
        import boto3
        import csv
        import json
        from datetime import datetime as dt

        print(f"Previous step: {upload_status}")
        s3 = boto3.client('s3')

        # ── Read CSV from S3 ──────────────────────────────────────────
        print("📂 Reading CSV from S3...")
        csv_obj = s3.get_object(
            Bucket='etl-end-to-end-project',
            Key='raw/employees.csv'
        )
        csv_content = csv_obj['Body'].read().decode('utf-8').splitlines()
        reader = csv.DictReader(csv_content)
        employees = list(reader)
        print(f"✅ Read {len(employees)} employee records")

        # ── Read JSON from S3 ─────────────────────────────────────────
        print("📂 Reading JSON from S3...")
        json_obj = s3.get_object(
            Bucket='etl-end-to-end-project',
            Key='raw/departments.json'
        )
        departments = json.loads(json_obj['Body'].read().decode('utf-8'))
        print(f"✅ Read {len(departments)} department records")

        # ── Build department lookup map ───────────────────────────────
        dept_map = {d['dept_name'].upper(): d for d in departments}

        # ── Transform ─────────────────────────────────────────────────
        print("⚙️  Applying transformations...")
        transformed = []
        seen_ids = set()

        for emp in employees:
            emp_id = emp.get('emp_id')

            # Deduplication
            if emp_id in seen_ids:
                continue
            seen_ids.add(emp_id)

            # Null handling
            name = emp.get('name') or 'Unknown'
            salary = int(emp.get('salary') or 0)
            department = (emp.get('department') or '').upper()

            # Join with department
            dept_info = dept_map.get(department, {})

            transformed.append({
                'emp_id': emp_id,
                'name': name,
                'department': department,
                'salary': salary,
                'joining_date': emp.get('joining_date'),
                'ingestion_timestamp': str(dt.now()),
                'dept_name': dept_info.get('dept_name', ''),
                'location': dept_info.get('location', ''),
                'budget': dept_info.get('budget', 0)
            })

        print(f"✅ Transformed {len(transformed)} records")
        for r in transformed:
            print(r)

        return "Transform complete"

    @task()
    def load_to_s3_processed(transform_status: str):
        import boto3
        import csv
        import json
        import io
        from datetime import datetime as dt

        print(f"Previous step: {transform_status}")
        s3 = boto3.client('s3')

        # Read from S3 raw again and transform
        csv_obj = s3.get_object(
            Bucket='etl-end-to-end-project',
            Key='raw/employees.csv'
        )
        csv_content = csv_obj['Body'].read().decode('utf-8').splitlines()
        reader = csv.DictReader(csv_content)
        employees = list(reader)

        json_obj = s3.get_object(
            Bucket='etl-end-to-end-project',
            Key='raw/departments.json'
        )
        departments = json.loads(json_obj['Body'].read().decode('utf-8'))
        dept_map = {d['dept_name'].upper(): d for d in departments}

        transformed = []
        seen_ids = set()
        for emp in employees:
            emp_id = emp.get('emp_id')
            if emp_id in seen_ids:
                continue
            seen_ids.add(emp_id)
            name = emp.get('name') or 'Unknown'
            salary = int(emp.get('salary') or 0)
            department = (emp.get('department') or '').upper()
            dept_info = dept_map.get(department, {})
            transformed.append({
                'emp_id': emp_id,
                'name': name,
                'department': department,
                'salary': salary,
                'joining_date': emp.get('joining_date'),
                'ingestion_timestamp': str(dt.now()),
                'dept_name': dept_info.get('dept_name', ''),
                'location': dept_info.get('location', ''),
                'budget': dept_info.get('budget', 0)
            })

        # Save as JSON to S3 processed/
        output = json.dumps(transformed, indent=2)
        s3.put_object(
            Bucket='etl-end-to-end-project',
            Key='processed/employees_processed.json',
            Body=output.encode('utf-8')
        )
        print(f"✅ Saved {len(transformed)} records to s3://etl-end-to-end-project/processed/employees_processed.json")
        return "S3 processed load complete"

    @task()
    def load_to_mysql(s3_status: str):
        import boto3
        import json
        import mysql.connector

        print(f"Previous step: {s3_status}")
        s3 = boto3.client('s3')

        # Read processed data from S3
        obj = s3.get_object(
            Bucket='etl-end-to-end-project',
            Key='processed/employees_processed.json'
        )
        records = json.loads(obj['Body'].read().decode('utf-8'))
        print(f"✅ Read {len(records)} records from S3 processed/")

        # Connect to MySQL
        conn = mysql.connector.connect(
            host='host.docker.internal',
            user='root',
            password='root',
            database='etl_db'
        )
        cursor = conn.cursor()

        # Truncate and reload
        cursor.execute("TRUNCATE TABLE employees")
        for r in records:
            cursor.execute("""
                INSERT INTO employees
                (emp_id, name, department, salary, joining_date,
                 ingestion_timestamp, dept_name, location, budget)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                r['emp_id'], r['name'], r['department'],
                r['salary'], r['joining_date'], r['ingestion_timestamp'],
                r['dept_name'], r['location'], r['budget']
            ))

        conn.commit()
        print(f"✅ Loaded {len(records)} records into MySQL")
        cursor.close()
        conn.close()
        return "MySQL load complete"

    # Task dependencies
    upload_status = upload_raw_files()
    transform_status = transform_data(upload_status)
    s3_status = load_to_s3_processed(transform_status)
    load_to_mysql(s3_status)

etl_pipeline()