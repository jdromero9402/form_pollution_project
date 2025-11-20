from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from datetime import datetime
import sys, json, time, traceback, boto3, psycopg2

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from botocore.exceptions import ClientError

# ======================================================
# Job parameters
# ======================================================
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'RDS_SECRET_NAME',
    'INFLUXDB_SECRET_NAME',
    'BATCH_SIZE',
    'AWS_REGION',
    'SCHEMA',
    'START_DATE',
    'END_DATE'
])

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ======================================================
# Config
# ======================================================
BATCH_SIZE = int(args.get('BATCH_SIZE', '5000'))
AWS_REGION = args.get('AWS_REGION', 'us-east-1')
SCHEMA = args.get('SCHEMA', 'public')
INFLUX_LINE_BATCH_SIZE = 1000     # sub-batch for Influx writes
INFLUX_TIMESTAMP_PRECISION = 's' # can be ns, us, ms, s

# Parse dates
try:
    START_DATE = datetime.strptime(args['START_DATE'], "%Y-%m-%d")
    END_DATE = datetime.strptime(args['END_DATE'], "%Y-%m-%d")
    print(f"Filtering data between {START_DATE} and {END_DATE}")
except Exception as e:
    print(f"❌ Error parsing dates: {e}")
    sys.exit(1)

# ======================================================
# Secrets
# ======================================================
def get_secret(secret_name, region_name):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        return json.loads(client.get_secret_value(SecretId=secret_name)['SecretString'])
    except ClientError as e:
        print(f"Error retrieving secret {secret_name}: {e}")
        raise e

print("Retrieving secrets from AWS Secrets Manager...")
rds_secret = get_secret(args['RDS_SECRET_NAME'], AWS_REGION)
influxdb_secret = get_secret(args['INFLUXDB_SECRET_NAME'], AWS_REGION)

# JDBC config
jdbc_url = f"jdbc:postgresql://{rds_secret['host']}:{rds_secret.get('port', 5432)}/{rds_secret['database']}"
connection_properties = {
    "user": rds_secret['username'],
    "password": rds_secret['password'],
    "driver": "org.postgresql.Driver"
}

# InfluxDB config
INFLUXDB_URL = influxdb_secret['url']
INFLUXDB_TOKEN = influxdb_secret['token']
INFLUXDB_ORG = influxdb_secret['org']
INFLUXDB_BUCKET = influxdb_secret['bucket']

print(f"Connected to RDS: {rds_secret['host']}")
print(f"Database Schema: {SCHEMA}")
print(f"InfluxDB URL: {INFLUXDB_URL}")

# ======================================================
# Helpers
# ======================================================
def update_processed_at(measurement_sks_batch):
    if not measurement_sks_batch:
        return
    try:
        conn = psycopg2.connect(
            host=rds_secret['host'],
            port=rds_secret.get('port', 5432),
            database=rds_secret['database'],
            user=rds_secret['username'],
            password=rds_secret['password']
        )
        cursor = conn.cursor()
        sks_str = ','.join(map(str, measurement_sks_batch))
        update_query = f"""
            UPDATE {SCHEMA}.measurements_raw 
            SET processed_at = CURRENT_TIMESTAMP 
            WHERE measurement_sk IN ({sks_str})
        """
        cursor.execute(update_query)
        conn.commit()
        print(f"Updated processed_at for {cursor.rowcount} records")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error updating processed_at: {e}")
        traceback.print_exc()

# ======================================================
# Optimized write to InfluxDB
# ======================================================
def write_to_influxdb_batch(rows_list):
    print(f"write_to_influxdb_batch called with {len(rows_list)} rows", file=sys.stderr)
    sys.stderr.flush()
    if not rows_list:
        return []

    successful_sks = []
    start_time = time.time()

    # Map precision string
    precision_map = {
        "ns": WritePrecision.NS,
        "us": WritePrecision.US,
        "ms": WritePrecision.MS,
        "s": WritePrecision.S
    }
    wp = precision_map.get(INFLUX_TIMESTAMP_PRECISION.lower(), WritePrecision.S)

    try:
        with InfluxDBClient(
            url=INFLUXDB_URL,
            token=INFLUXDB_TOKEN,
            org=INFLUXDB_ORG,
            timeout=120_000
        ) as client:

            write_api = client.write_api(write_options=SYNCHRONOUS)

            for i in range(0, len(rows_list), INFLUX_LINE_BATCH_SIZE):
                sub_rows = rows_list[i:i + INFLUX_LINE_BATCH_SIZE]
                points = []
                sub_sks = []

                for row in sub_rows:
                    try:
                        point = (
                            Point("air_quality")
                            .tag("station_id", str(row.station_id))
                            .tag("pollutant_id", str(row.pollutant_id))
                            .field("value", float(row.value))
                        )
                        if row.ts_utc:
                            point.time(row.ts_utc, wp)
                        else:
                            point.time(datetime.utcnow(), wp)
                        if row.unit:
                            point.tag("unit", str(row.unit))
                        if row.source:
                            point.tag("source", str(row.source))
                        if row.year:
                            point.tag("year", str(row.year))
                        points.append(point)
                        sub_sks.append(row.measurement_sk)
                    except Exception as e:
                        print(f"⚠️ Error building point for {row.measurement_sk}: {e}")
                        continue

                if not points:
                    continue

                max_retries = 3
                backoff = 5
                for attempt in range(max_retries):
                    try:
                        print(f"→ Writing sub-batch {i//INFLUX_LINE_BATCH_SIZE + 1} "
                              f"({len(points)} pts, precision={INFLUX_TIMESTAMP_PRECISION})",
                              file=sys.stderr)
                        sys.stderr.flush()

                        write_api.write(
                            bucket=INFLUXDB_BUCKET,
                            org=INFLUXDB_ORG,
                            record=points
                        )

                        successful_sks.extend(sub_sks)
                        print(f"✅ Sub-batch {i//INFLUX_LINE_BATCH_SIZE + 1} OK ({len(points)} pts)",
                              file=sys.stderr)
                        break
                    except Exception as e:
                        print(f"⚠️ Error writing sub-batch (attempt {attempt+1}): {e}",
                              file=sys.stderr)
                        if attempt < max_retries - 1:
                            time.sleep(backoff * (2 ** attempt))
                        else:
                            print("🚨 Sub-batch failed permanently", file=sys.stderr)
                            traceback.print_exc(file=sys.stderr)

        print(f"✅ Finished writing {len(successful_sks)} pts in {time.time()-start_time:.1f}s",
              file=sys.stderr)
    except Exception as e:
        print(f"❌ Exception in write_to_influxdb_batch: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)

    sys.stderr.flush()
    return successful_sks

# ======================================================
# Main process
# ======================================================
try:
    # Preload all measurement_sks
    id_query = f"""
        (SELECT measurement_sk
         FROM {SCHEMA}.measurements_raw
         WHERE processed_at IS NULL
         AND is_valid = true
         AND ts_utc BETWEEN '{START_DATE}' AND '{END_DATE}'
         ORDER BY measurement_sk) as ids
    """
    id_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", id_query) \
        .option("user", connection_properties["user"]) \
        .option("password", connection_properties["password"]) \
        .option("driver", connection_properties["driver"]) \
        .load()

    all_ids = [r["measurement_sk"] for r in id_df.collect()]
    total_records = len(all_ids)
    print(f"Total unprocessed records: {total_records}")

    if total_records == 0:
        print("No records to process. Exiting.")
        job.commit()
        sys.exit(0)

    batches = [all_ids[i:i+BATCH_SIZE] for i in range(0, total_records, BATCH_SIZE)]
    print(f"Processing {len(batches)} batches of {BATCH_SIZE} records each")

    total_processed = 0
    total_successful = 0

    for batch_num, id_batch in enumerate(batches, start=1):
        print(f"\n{'='*60}")
        print(f"Processing batch {batch_num}/{len(batches)}")
        print(f"{'='*60}")

        id_list_str = ",".join(map(str, id_batch))
        query = f"""
            (SELECT measurement_sk, station_id, pollutant_id, ts_utc, value, unit, source, year
             FROM {SCHEMA}.measurements_raw
             WHERE measurement_sk IN ({id_list_str})) as measurements
        """

        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", query) \
            .option("user", connection_properties["user"]) \
            .option("password", connection_properties["password"]) \
            .option("driver", connection_properties["driver"]) \
            .option("fetchsize", 10000) \
            .load()

        batch_count = df.count()
        if batch_count == 0:
            print(f"Batch {batch_num}: No records to process")
            continue

        print(f"Batch {batch_num}: Processing {batch_count} records")
        rows = df.collect()
        successful_sks = write_to_influxdb_batch(rows)
        if successful_sks:
            update_processed_at(successful_sks)

        total_processed += batch_count
        total_successful += len(successful_sks)

        print(f"Batch {batch_num} complete: {len(successful_sks)}/{batch_count} successful")
        print(f"Overall progress: {total_successful}/{total_processed}")

    print(f"\n{'='*60}")
    print("Job Complete!")
    print(f"Total processed: {total_processed}")
    print(f"Successfully written: {total_successful}")
    print(f"Success rate: {(total_successful/total_processed*100):.2f}%")
    print(f"{'='*60}")

except Exception as e:
    print(f"Error in Glue job: {e}")
    traceback.print_exc()
    raise
finally:
    job.commit()