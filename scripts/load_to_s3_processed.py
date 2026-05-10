import os
import boto3

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"

from pyspark.sql import SparkSession

# ── 1. Create Spark Session ───────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Load-Processed-S3") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("✅ Spark Session Created Successfully")

# ── 2. Read Parquet from local (output of transform.py) ───────────────
print("\n📂 Reading local Parquet file...")
df = spark.read.parquet("data/output/employees_parquet")
print(f"✅ Total records to upload: {df.count()}")
df.show()

# ── 3. Save directly to S3 processed/ folder as Parquet ──────────────
print("\n☁️  Saving processed Parquet to S3...")
df.write \
    .mode("overwrite") \
    .parquet("s3a://etl-end-to-end-project/processed/employees_parquet")
print("✅ Saved to s3://etl-end-to-end-project/processed/employees_parquet/")

# ── 4. Verify — list files in S3 processed/ folder ───────────────────
print("\n🔍 Verifying files in S3 processed/ folder...")
s3 = boto3.client('s3')
response = s3.list_objects_v2(
    Bucket='etl-end-to-end-project',
    Prefix='processed/'
)
if 'Contents' in response:
    for obj in response['Contents']:
        size_kb = round(obj['Size'] / 1024, 2)
        print(f"  ✅ {obj['Key']} ({size_kb} KB)")
else:
    print("  ❌ No files found in processed/ folder")

print("\n✅ Load to S3 Processed Complete!")
spark.stop()