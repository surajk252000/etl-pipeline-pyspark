import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, upper, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

# ── 1. Create Spark Session with S3 config ───────────────────────────
spark = SparkSession.builder \
    .appName("ETL-Pipeline") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("✅ Spark Session Created Successfully")

# ── 2. Define Schemas ─────────────────────────────────────────────────
emp_schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", LongType(), True),
    StructField("joining_date", StringType(), True)
])

dept_schema = StructType([
    StructField("dept_id", IntegerType(), True),
    StructField("dept_name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("budget", LongType(), True)
])

# ── 3. Read CSV from S3 with Schema ──────────────────────────────────
print("\n📂 Reading CSV from S3...")
df_emp = spark.read.csv(
    "s3a://etl-end-to-end-project/raw/employees.csv",
    header=True,
    schema=emp_schema
)
print("Raw Employee Data:")
df_emp.show()

# ── 4. Read JSON from S3 with Schema ─────────────────────────────────
print("\n📂 Reading JSON from S3...")
df_dept = spark.read.schema(dept_schema).option("multiline", "true").json(
    "s3a://etl-end-to-end-project/raw/departments.json"
)
print("Raw Department Data:")
df_dept.show()

# ── 5. Transformations on Employee Data ───────────────────────────────
print("\n⚙️  Applying Transformations...")
df_emp_clean = df_emp \
    .withColumn("name", when(col("name").isNull(), "Unknown").otherwise(col("name"))) \
    .withColumn("salary", when(col("salary").isNull(), 0).otherwise(col("salary"))) \
    .withColumn("department", upper(col("department"))) \
    .withColumn("ingestion_timestamp", current_timestamp())

print("Cleaned Employee Data:")
df_emp_clean.show()

# ── 6. Deduplication ──────────────────────────────────────────────────
df_emp_dedup = df_emp_clean.dropDuplicates(["emp_id"])
print(f"✅ Records after deduplication: {df_emp_dedup.count()}")

# ── 7. Join Employee + Department ─────────────────────────────────────
print("\n🔗 Joining Employee and Department data...")
df_dept_upper = df_dept.withColumn("dept_name", upper(col("dept_name")))
df_joined = df_emp_dedup.join(
    df_dept_upper,
    df_emp_dedup.department == df_dept_upper.dept_name,
    how="left"
)
print("Joined Data:")
df_joined.show()

# ── 8. Save as Parquet (Intermediate Storage) ─────────────────────────
print("\n💾 Saving as Parquet...")
df_joined.write.mode("overwrite").parquet("data/output/employees_parquet")
print("✅ Parquet saved at data/output/employees_parquet")

# ── 9. Read back Parquet ──────────────────────────────────────────────
print("\n📂 Reading Parquet file back...")
df_parquet = spark.read.parquet("data/output/employees_parquet")
print("Parquet Data:")
df_parquet.show()

print("\n✅ Transformation Complete!")
spark.stop()