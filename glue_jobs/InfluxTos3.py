from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, LongType
from datetime import datetime
from influxdb_client import InfluxDBClient
import sys, json, boto3, pandas as pd
from awsglue.dynamicframe import DynamicFrame

import warnings
from influxdb_client.client.warnings import MissingPivotFunction

warnings.simplefilter("ignore", MissingPivotFunction)

# =====================================================
# 1️⃣ Glue parameters
# =====================================================
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'AWS_REGION',
    'S3_BUCKET',
    'S3_PREFIX',
    'START_DATE',
    'END_DATE',
    'INFLUXDB_SECRET_NAME',
    'REDSHIFT_SECRET_NAME',
    'REDSHIFT_SCHEMA',
    'REDSHIFT_CONNECTION',
    'REDSHIFT_DB'
])

AWS_REGION = args['AWS_REGION']
S3_BUCKET = args['S3_BUCKET']
S3_PREFIX = args['S3_PREFIX']
START_DATE = args['START_DATE']
END_DATE = args['END_DATE']
INFLUXDB_SECRET_NAME = args['INFLUXDB_SECRET_NAME']
REDSHIFT_SECRET_NAME = args['REDSHIFT_SECRET_NAME']
REDSHIFT_SCHEMA = args['REDSHIFT_SCHEMA']
REDSHIFT_CONNECTION = args['REDSHIFT_CONNECTION']
REDSHIFT_DB = args['REDSHIFT_DB']

# =====================================================
# 2️⃣ Glue & Spark contexts
# =====================================================
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# =====================================================
# 3️⃣ Secrets Manager helper
# =====================================================
def get_secret(secret_name):
    client = boto3.client('secretsmanager', region_name=AWS_REGION)
    secret_value = client.get_secret_value(SecretId=secret_name)
    return json.loads(secret_value['SecretString'])

# Retrieve secrets
influx_secret = get_secret(INFLUXDB_SECRET_NAME)

INFLUXDB_URL = influx_secret['url']
INFLUXDB_TOKEN = influx_secret['token']
INFLUXDB_ORG = influx_secret['org']
INFLUXDB_BUCKET = influx_secret['bucket']

# =====================================================
# 4️⃣ Helper functions
# =====================================================
# def write_df_to_s3_parquet(spark_df, key):
#     """
#     Guarda el DataFrame en S3 como Parquet
#     """
#     path = f"s3://{S3_BUCKET}/{key}"
#     spark_df.write.mode("overwrite").parquet(path)
#     print(f"✅ Saved Parquet to {path}")
#     return path

def load_to_redshift_direct(spark_df, table_name, schema):
    """
    Carga un DataFrame de Spark directamente a Redshift usando el conector de Glue.
    Solo usa S3 como directorio temporal interno (requerido por Redshift).
    """
    full_table = f"{schema}.{table_name}"
    
    # Convertir Spark DataFrame a Glue DynamicFrame
    dynamic_frame = DynamicFrame.fromDF(spark_df, glueContext, f"{table_name}_df")
    
    # Escribir a Redshift usando el conector de Glue
    print(f"🚀 Loading {full_table} directly to Redshift...")
    
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynamic_frame,
        catalog_connection=REDSHIFT_CONNECTION,
        connection_options={
            "dbtable": full_table,
            "database": REDSHIFT_DB,
            "preactions": f"TRUNCATE TABLE {full_table};"
        },
        redshift_tmp_dir=f"s3://{S3_BUCKET}/{S3_PREFIX}redshift_temp/"
    )
    
    print(f"✅ Loaded {full_table} into Redshift")

# =====================================================
# 5️⃣ Connect to InfluxDB
# =====================================================
client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
query_api = client.query_api()

# =====================================================
# 6️⃣ ANÁLISIS 1: Min, Max, Avg por año, pollutant y station
# =====================================================
flux_stats = f"""
from(bucket: "{INFLUXDB_BUCKET}")
  |> range(start: {START_DATE}, stop: {END_DATE})
  |> filter(fn: (r) => r["_measurement"] == "air_quality")
  |> filter(fn: (r) => r["_field"] == "value")
  |> map(fn: (r) => ({{ r with year: string(v: uint(v: time(v: r._time)) / uint(v: 1000000000) / uint(v: 31536000) + uint(v: 1970)) }}))
  |> group(columns: ["year", "pollutant_id", "station_id"])
  |> reduce(
      fn: (r, accumulator) => ({{
        min: if r._value < accumulator.min then r._value else accumulator.min,
        max: if r._value > accumulator.max then r._value else accumulator.max,
        sum: accumulator.sum + r._value,
        count: accumulator.count + 1.0
      }}),
      identity: {{min: 999999.0, max: -999999.0, sum: 0.0, count: 0.0}}
  )
  |> map(fn: (r) => ({{
      r with
      avg: r.sum / r.count
  }}))
  |> map(fn: (r) => ({{
      _time: now(),
      year: r.year,
      pollutant_id: r.pollutant_id,
      station_id: r.station_id,
      min_value: r.min,
      max_value: r.max,
      avg_value: r.avg,
      count_records: int(v: r.count)
  }}))
  |> keep(columns: ["year", "pollutant_id", "station_id", "min_value", "max_value", "avg_value", "count_records"])
"""
print("→ Running Min/Max/Avg stats query...")
stats_df = query_api.query_data_frame(flux_stats)

if not stats_df.empty:
    # Eliminar columnas de metadatos de InfluxDB
    influx_metadata_cols = ['result', 'table', '_start', '_stop', '_measurement']
    stats_df = stats_df.drop(columns=[col for col in influx_metadata_cols if col in stats_df.columns], errors='ignore')
    
    # Asegurar que cumple con el esquema de Redshift
    stats_df['year'] = stats_df['year'].astype(int)
    stats_df['pollutant_id'] = stats_df['pollutant_id'].astype(str)
    stats_df['station_id'] = stats_df['station_id'].astype(str)
    stats_df['min_value'] = stats_df['min_value'].astype(float)
    stats_df['max_value'] = stats_df['max_value'].astype(float)
    stats_df['avg_value'] = stats_df['avg_value'].astype(float)
    stats_df['count_records'] = stats_df['count_records'].astype(int)
    
    # Definir el esquema explícito
    schema_stats = StructType([
        StructField("year", IntegerType(), False),
        StructField("pollutant_id", StringType(), False),
        StructField("station_id", StringType(), False),
        StructField("min_value", DoubleType(), True),
        StructField("max_value", DoubleType(), True),
        StructField("avg_value", DoubleType(), True),
        StructField("count_records", LongType(), True)
    ])
    
    # Seleccionar solo las columnas necesarias en el orden correcto
    stats_df = stats_df[['year', 'pollutant_id', 'station_id', 'min_value', 'max_value', 'avg_value', 'count_records']]
    
    spark_stats_df = spark.createDataFrame(stats_df, schema=schema_stats)
    load_to_redshift_direct(spark_stats_df, "air_quality_stats", REDSHIFT_SCHEMA)
else:
    print("⚠️ No data returned for stats query")

# =====================================================
# 7️⃣ ANÁLISIS 2: Tasa de cambio diaria (derivada)
# =====================================================
flux_daily_derivative = f"""
from(bucket: "{INFLUXDB_BUCKET}")
  |> range(start: {START_DATE}, stop: {END_DATE})
  |> filter(fn: (r) => r["_measurement"] == "air_quality")
  |> filter(fn: (r) => r["_field"] == "value")
  |> group(columns: ["station_id", "pollutant_id"])
  |> sort(columns: ["_time"])
  |> derivative(unit: 1d, nonNegative: false)
  |> map(fn: (r) => ({{
      r with
      daily_rate: r._value
  }}))
  |> keep(columns: ["_time", "station_id", "pollutant_id", "daily_rate"])
"""
print("→ Running daily derivative query...")
daily_deriv_df = query_api.query_data_frame(flux_daily_derivative)

if not daily_deriv_df.empty:
    # Eliminar columnas de metadatos de InfluxDB
    influx_metadata_cols = ['result', 'table', '_start', '_stop', '_measurement', '_field']
    daily_deriv_df = daily_deriv_df.drop(columns=[col for col in influx_metadata_cols if col in daily_deriv_df.columns], errors='ignore')
    
    daily_deriv_df.rename(columns={"_time": "time"}, inplace=True)
    daily_deriv_df["time"] = pd.to_datetime(daily_deriv_df["time"])
    daily_deriv_df["year"] = daily_deriv_df["time"].dt.year
    daily_deriv_df["month"] = daily_deriv_df["time"].dt.month
    daily_deriv_df["day"] = daily_deriv_df["time"].dt.day
    
    # Asegurar tipos de datos
    daily_deriv_df['pollutant_id'] = daily_deriv_df['pollutant_id'].astype(str)
    daily_deriv_df['station_id'] = daily_deriv_df['station_id'].astype(str)
    daily_deriv_df['daily_rate'] = daily_deriv_df['daily_rate'].astype(float)
    daily_deriv_df['year'] = daily_deriv_df['year'].astype(int)
    daily_deriv_df['month'] = daily_deriv_df['month'].astype(int)
    daily_deriv_df['day'] = daily_deriv_df['day'].astype(int)
    
    # Definir esquema
    schema_daily = StructType([
        StructField("time", TimestampType(), False),
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("day", IntegerType(), False),
        StructField("pollutant_id", StringType(), False),
        StructField("station_id", StringType(), False),
        StructField("daily_rate", DoubleType(), True)
    ])
    
    # Seleccionar columnas en orden correcto
    daily_deriv_df = daily_deriv_df[['time', 'year', 'month', 'day', 'pollutant_id', 'station_id', 'daily_rate']]
    
    spark_daily_deriv_df = spark.createDataFrame(daily_deriv_df, schema=schema_daily)
    load_to_redshift_direct(spark_daily_deriv_df, "air_quality_daily_derivative", REDSHIFT_SCHEMA)
else:
    print("⚠️ No data returned for daily derivative query")

# =====================================================
# 8️⃣ ANÁLISIS 3: Tasa de cambio mensual (derivada)
# =====================================================
flux_monthly_derivative = f"""
from(bucket: "{INFLUXDB_BUCKET}")
  |> range(start: {START_DATE}, stop: {END_DATE})
  |> filter(fn: (r) => r["_measurement"] == "air_quality")
  |> filter(fn: (r) => r["_field"] == "value")
  |> group(columns: ["station_id", "pollutant_id"])
  |> aggregateWindow(every: 1mo, fn: mean, createEmpty: false)
  |> sort(columns: ["_time"])
  |> derivative(unit: 1mo, nonNegative: false)
  |> map(fn: (r) => ({{
      r with
      monthly_rate: r._value
  }}))
  |> keep(columns: ["_time", "station_id", "pollutant_id", "monthly_rate"])
"""
print("→ Running monthly derivative query...")
monthly_deriv_df = query_api.query_data_frame(flux_monthly_derivative)

if not monthly_deriv_df.empty:
    # Eliminar columnas de metadatos de InfluxDB
    influx_metadata_cols = ['result', 'table', '_start', '_stop', '_measurement', '_field']
    monthly_deriv_df = monthly_deriv_df.drop(columns=[col for col in influx_metadata_cols if col in monthly_deriv_df.columns], errors='ignore')
    
    monthly_deriv_df.rename(columns={"_time": "time"}, inplace=True)
    monthly_deriv_df["time"] = pd.to_datetime(monthly_deriv_df["time"])
    monthly_deriv_df["year"] = monthly_deriv_df["time"].dt.year
    monthly_deriv_df["month"] = monthly_deriv_df["time"].dt.month
    
    # Asegurar tipos de datos
    monthly_deriv_df['pollutant_id'] = monthly_deriv_df['pollutant_id'].astype(str)
    monthly_deriv_df['station_id'] = monthly_deriv_df['station_id'].astype(str)
    monthly_deriv_df['monthly_rate'] = monthly_deriv_df['monthly_rate'].astype(float)
    monthly_deriv_df['year'] = monthly_deriv_df['year'].astype(int)
    monthly_deriv_df['month'] = monthly_deriv_df['month'].astype(int)
    
    # Definir esquema
    schema_monthly = StructType([
        StructField("time", TimestampType(), False),
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("pollutant_id", StringType(), False),
        StructField("station_id", StringType(), False),
        StructField("monthly_rate", DoubleType(), True)
    ])
    
    # Seleccionar columnas en orden correcto
    monthly_deriv_df = monthly_deriv_df[['time', 'year', 'month', 'pollutant_id', 'station_id', 'monthly_rate']]
    
    spark_monthly_deriv_df = spark.createDataFrame(monthly_deriv_df, schema=schema_monthly)
    load_to_redshift_direct(spark_monthly_deriv_df, "air_quality_monthly_derivative", REDSHIFT_SCHEMA)
else:
    print("⚠️ No data returned for monthly derivative query")

# =====================================================
# 9️⃣ ANÁLISIS 4: Picos y Valles por año, pollutant y station
# =====================================================
flux_peaks = f"""
import "date"

peaks = from(bucket: "{INFLUXDB_BUCKET}")
  |> range(start: {START_DATE}, stop: {END_DATE})
  |> filter(fn: (r) => r["_measurement"] == "air_quality")
  |> filter(fn: (r) => r["_field"] == "value")
  |> map(fn: (r) => ({{ r with year: string(v: date.year(t: r._time)) }}))
  |> group(columns: ["year", "pollutant_id", "station_id"])
  |> max()
  |> map(fn: (r) => ({{
      year: r.year,
      pollutant_id: r.pollutant_id,
      station_id: r.station_id,
      peak_time: r._time,
      peak_value: r._value
  }}))
  |> keep(columns: ["year", "pollutant_id", "station_id", "peak_time", "peak_value"])

valleys = from(bucket: "{INFLUXDB_BUCKET}")
  |> range(start: {START_DATE}, stop: {END_DATE})
  |> filter(fn: (r) => r["_measurement"] == "air_quality")
  |> filter(fn: (r) => r["_field"] == "value")
  |> map(fn: (r) => ({{ r with year: string(v: date.year(t: r._time)) }}))
  |> group(columns: ["year", "pollutant_id", "station_id"])
  |> min()
  |> map(fn: (r) => ({{
      year: r.year,
      pollutant_id: r.pollutant_id,
      station_id: r.station_id,
      valley_time: r._time,
      valley_value: r._value
  }}))
  |> keep(columns: ["year", "pollutant_id", "station_id", "valley_time", "valley_value"])

join(
    tables: {{peaks: peaks, valleys: valleys}},
    on: ["year", "pollutant_id", "station_id"]
)
"""
print("→ Running peaks and valleys query...")
peaks_df = query_api.query_data_frame(flux_peaks)

if not peaks_df.empty:
    # Eliminar columnas de metadatos de InfluxDB
    influx_metadata_cols = ['result', 'table', '_start', '_stop', '_measurement', '_field']
    peaks_df = peaks_df.drop(columns=[col for col in influx_metadata_cols if col in peaks_df.columns], errors='ignore')
    
    # Asegurar tipos de datos
    peaks_df['year'] = peaks_df['year'].astype(int)
    peaks_df['pollutant_id'] = peaks_df['pollutant_id'].astype(str)
    peaks_df['station_id'] = peaks_df['station_id'].astype(str)
    peaks_df['peak_time'] = pd.to_datetime(peaks_df['peak_time'])
    peaks_df['valley_time'] = pd.to_datetime(peaks_df['valley_time'])
    peaks_df['peak_value'] = peaks_df['peak_value'].astype(float)
    peaks_df['valley_value'] = peaks_df['valley_value'].astype(float)
    
    # Definir esquema
    schema_peaks = StructType([
        StructField("year", IntegerType(), False),
        StructField("pollutant_id", StringType(), False),
        StructField("station_id", StringType(), False),
        StructField("peak_time", TimestampType(), True),
        StructField("peak_value", DoubleType(), True),
        StructField("valley_time", TimestampType(), True),
        StructField("valley_value", DoubleType(), True)
    ])
    
    # Seleccionar columnas en orden correcto
    peaks_df = peaks_df[['year', 'pollutant_id', 'station_id', 'peak_time', 'peak_value', 'valley_time', 'valley_value']]
    
    spark_peaks_df = spark.createDataFrame(peaks_df, schema=schema_peaks)
    load_to_redshift_direct(spark_peaks_df, "air_quality_peaks_valleys", REDSHIFT_SCHEMA)
else:
    print("⚠️ No data returned for peaks and valleys query")

# =====================================================
#  🔚 Job complete
# =====================================================
print("✅ Glue Spark job complete — All air quality analytics loaded into Redshift.")
job.commit()