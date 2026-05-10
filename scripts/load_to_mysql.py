import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"

from pyspark.sql import SparkSession
import mysql.connector

# ── 1. Create Spark Session ──────────────────────────────────────────
spark = SparkSession.builder \
    .appName("ETL-Load-MySQL") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("✅ Spark Session Created")

# ── 2. Read Parquet ───────────────────────────────────────────────────
print("\n📂 Reading Parquet file...")
df = spark.read.parquet("data/output/employees_parquet")
print("Parquet Data:")
df.show()

# ── 3. Create MySQL Database and Table ───────────────────────────────
print("\n🛢️  Setting up MySQL...")
conn = mysql.connector.connect(
    host="localhost",
    user="root",          # change if different
    password="root"   # change this
)
cursor = conn.cursor()

cursor.execute("CREATE DATABASE IF NOT EXISTS etl_db")
cursor.execute("USE etl_db")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS employees (
        emp_id INT,
        name VARCHAR(100),
        department VARCHAR(100),
        salary BIGINT,
        joining_date VARCHAR(50),
        ingestion_timestamp VARCHAR(100),
        dept_id INT,
        dept_name VARCHAR(100),
        location VARCHAR(100),
        budget BIGINT
    )
""")
cursor.execute("TRUNCATE TABLE employees")
conn.commit()
print("✅ Database and Table ready")

# ── 4. Load into MySQL ────────────────────────────────────────────────
print("\n⬆️  Loading data into MySQL...")
rows = df.collect()
for row in rows:
    cursor.execute("""
        INSERT INTO employees 
        (emp_id, name, department, salary, joining_date, ingestion_timestamp, dept_id, dept_name, location, budget)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        row["emp_id"], row["name"], row["department"],
        row["salary"], row["joining_date"], str(row["ingestion_timestamp"]),
        row["dept_id"], row["dept_name"], row["location"], row["budget"]
    ))

conn.commit()
print(f"✅ {len(rows)} records loaded into MySQL successfully!")

# ── 5. Verify ─────────────────────────────────────────────────────────
cursor.execute("SELECT * FROM etl_db.employees")
results = cursor.fetchall()
print("\n📊 Data in MySQL:")
for r in results:
    print(r)

cursor.close()
conn.close()
spark.stop()
print("\n✅ Load Complete!")