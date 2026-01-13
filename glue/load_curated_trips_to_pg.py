import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "curated_trips_path",
    "pg_jdbc_url",
    "pg_user",
    "pg_password",
    "pg_target_table"
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

curated_trips_path = args["curated_trips_path"]
pg_jdbc_url = args["pg_jdbc_url"]
pg_user = args["pg_user"]
pg_password = args["pg_password"]
pg_target_table = args["pg_target_table"]

df = spark.read.parquet(curated_trips_path)

# --- Normalize column names ---
# Map common TLC / Glue variations -> our canonical lowercase names
rename_map = {
    # TLC standard fields
    "VendorID": "vendorid",
    "RatecodeID": "ratecodeid",
    "PULocationID": "pulocationid",
    "DOLocationID": "dolocationid",
    "tpep_pickup_datetime": "tpep_pickup_datetime",
    "tpep_dropoff_datetime": "tpep_dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "Airport_fee": "airport_fee",     # some versions
    "airport_fee": "airport_fee",
    "cbd_congestion_fee": "cbd_congestion_fee",

    # Your enrichment columns from Glue (your job used PU_/DO_ prefixes)
    "PU_Borough": "pu_borough",
    "PU_Zone": "pu_zone",
    "PU_ServiceZone": "pu_servicezone",
    "DO_Borough": "do_borough",
    "DO_Zone": "do_zone",
    "DO_ServiceZone": "do_servicezone",

    # In case crawler already produced lowercase enrichment names
    "pu_borough": "pu_borough",
    "pu_zone": "pu_zone",
    "pu_servicezone": "pu_servicezone",
    "do_borough": "do_borough",
    "do_zone": "do_zone",
    "do_servicezone": "do_servicezone",
}

# Apply renames where source column exists
for src, tgt in rename_map.items():
    if src in df.columns and src != tgt:
        df = df.withColumnRenamed(src, tgt)

# Canonical list we want to load to Postgres
expected_cols = [
    "vendorid","tpep_pickup_datetime","tpep_dropoff_datetime","passenger_count","trip_distance",
    "ratecodeid","store_and_fwd_flag","pulocationid","dolocationid","payment_type",
    "fare_amount","extra","mta_tax","tip_amount","tolls_amount","improvement_surcharge",
    "total_amount","congestion_surcharge","airport_fee","cbd_congestion_fee",
    "pu_borough","pu_zone","pu_servicezone","do_borough","do_zone","do_servicezone"
]

# Check what is still missing after renaming
missing = [c for c in expected_cols if c not in df.columns]
if missing:
    raise Exception(f"Still missing expected columns after rename: {missing}. Available columns: {df.columns}")

df = df.select(*expected_cols)

# --- Truncate + append load pattern (no psycopg2 needed) ---
empty_df = df.limit(0)

(empty_df.write
  .format("jdbc")
  .option("url", pg_jdbc_url)
  .option("dbtable", pg_target_table)
  .option("user", pg_user)
  .option("password", pg_password)
  .option("driver", "org.postgresql.Driver")
  .option("truncate", "true")
  .mode("overwrite")
  .save()
)

(df.write
  .format("jdbc")
  .option("url", pg_jdbc_url)
  .option("dbtable", pg_target_table)
  .option("user", pg_user)
  .option("password", pg_password)
  .option("driver", "org.postgresql.Driver")
  .mode("append")
  .save()
)

print(f"Loaded {df.count()} rows into {pg_target_table} from {curated_trips_path}")

job.commit()
