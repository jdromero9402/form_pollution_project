"""
AWS Glue Job - MIGRACIÓN RDS A REDSHIFT (SIN DUPLICADOS)
Implementa UPSERT para evitar duplicados
"""

import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *
import time
from datetime import datetime, timedelta

# ============================================================================
# PARÁMETROS
# ============================================================================
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'RDS_HOST', 'RDS_PORT', 'RDS_DB', 'RDS_USER', 'RDS_REGION', 'RDS_SCHEMA',
    'REDSHIFT_HOST', 'REDSHIFT_PORT', 'REDSHIFT_DB', 'REDSHIFT_USER', 
    'REDSHIFT_PASSWORD', 'REDSHIFT_SCHEMA',
    'SINCE_DAYS'
])

SINCE_DAYS = int(args.get('SINCE_DAYS', '7'))
TARGET_TABLE = 'air_quality_sabana'
STAGING_TABLE = 'air_quality_sabana_staging'
BATCH_SIZE = 50000

# ============================================================================
# INICIALIZAR GLUE
# ============================================================================
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("=" * 80)
print("🚀 CARGA RDS → REDSHIFT (SIN DUPLICADOS)")
print("=" * 80)
print(f"Since Days: {SINCE_DAYS}")
print("=" * 80)

# ============================================================================
# FUNCIONES
# ============================================================================

def get_rds_token():
    """Genera token IAM"""
    try:
        print("🔐 Generando token IAM...")
        rds_client = boto3.client('rds', region_name=args['RDS_REGION'])
        token = rds_client.generate_db_auth_token(
            DBHostname=args['RDS_HOST'],
            Port=int(args['RDS_PORT']),
            DBUsername=args['RDS_USER'],
            Region=args['RDS_REGION']
        )
        print("✅ Token generado")
        return token
    except Exception as e:
        print(f"❌ Error: {e}")
        raise


def get_redshift_connection():
    """Crea conexión a Redshift para operaciones SQL"""
    import psycopg2
    
    try:
        conn = psycopg2.connect(
            host=args['REDSHIFT_HOST'],
            port=int(args['REDSHIFT_PORT']),
            database=args['REDSHIFT_DB'],
            user=args['REDSHIFT_USER'],
            password=args['REDSHIFT_PASSWORD']
        )
        return conn
    except Exception as e:
        print(f"❌ Error conectando a Redshift: {e}")
        raise


def extract_from_rds():
    """Extrae datos de RDS"""
    try:
        print("\n📤 EXTRAYENDO DATOS...")
        
        token = get_rds_token()
        
        if SINCE_DAYS > 0:
            cutoff_date = (datetime.now() - timedelta(days=SINCE_DAYS)).strftime('%Y-%m-%d')
            print(f"   Desde: {cutoff_date}")
        else:
            cutoff_date = '2000-01-01'
        
        query = f"""
        (SELECT 
            year, ts_utc,
            EXTRACT(QUARTER FROM ts_utc)::INTEGER AS quarter,
            EXTRACT(MONTH FROM ts_utc)::INTEGER AS month_number,
            EXTRACT(DOW FROM ts_utc)::INTEGER AS day_of_week,
            measurement_sk, value as measurement_value,
            unit as measurement_unit, source as measurement_source,
            is_valid, pollutant_id, station_id,
            inserted_at, processed_at
        FROM {args['RDS_SCHEMA']}.measurements_raw
        WHERE is_valid = TRUE AND value IS NOT NULL 
            AND ts_utc >= '{cutoff_date}'::DATE
        ORDER BY ts_utc DESC
        LIMIT {BATCH_SIZE}) AS subquery
        """
        
        print(f"   Extrayendo hasta {BATCH_SIZE:,} registros...")
        start_time = time.time()
        
        jdbc_url = f"jdbc:postgresql://{args['RDS_HOST']}:{args['RDS_PORT']}/{args['RDS_DB']}"
        
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", query) \
            .option("user", args['RDS_USER']) \
            .option("password", token) \
            .option("driver", "org.postgresql.Driver") \
            .option("fetchsize", "5000") \
            .load()
        
        df = df.withColumn("etl_loaded_at", F.current_timestamp())
        
        # Convertir tipos
        df = df.withColumn("year", F.col("year").cast(IntegerType())) \
               .withColumn("ts_utc", F.col("ts_utc").cast(TimestampType())) \
               .withColumn("measurement_sk", F.col("measurement_sk").cast(LongType())) \
               .withColumn("measurement_value", F.col("measurement_value").cast(DoubleType())) \
               .withColumn("is_valid", F.col("is_valid").cast(BooleanType())) \
               .withColumn("pollutant_id", F.col("pollutant_id").cast(StringType())) \
               .withColumn("station_id", F.col("station_id").cast(StringType()))
        
        df = df.filter(F.col("pollutant_id").isNotNull() & F.col("station_id").isNotNull())
        df = df.cache()
        count = df.count()
        
        elapsed = time.time() - start_time
        
        if count > 0:
            print(f"   ✅ Extraídos {count:,} registros en {elapsed:.2f}s")
            return df
        else:
            print("   ⚠️  No hay datos")
            return None
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None


def enrich_data(df):
    """Enriquece datos - LIMPIEZA DE NaN EN SQL"""
    try:
        print("\n🔧 ENRIQUECIENDO DATOS...")
        
        token = get_rds_token()
        jdbc_url = f"jdbc:postgresql://{args['RDS_HOST']}:{args['RDS_PORT']}/{args['RDS_DB']}"
        
        # ===== POLLUTANTS =====
        print("   📥 Cargando pollutants...")
        
        pollutants_query = f"""
        (SELECT 
            pollutant_id,
            name as pollutant_name,
            default_unit as pollutant_default_unit,
            CASE 
                WHEN who_daily_limit IS NULL THEN NULL
                WHEN who_daily_limit::TEXT = 'NaN' THEN NULL
                ELSE who_daily_limit::TEXT
            END as who_daily_limit_text,
            CASE 
                WHEN who_annual_limit IS NULL THEN NULL
                WHEN who_annual_limit::TEXT = 'NaN' THEN NULL
                ELSE who_annual_limit::TEXT
            END as who_annual_limit_text
         FROM {args['RDS_SCHEMA']}.pollutants) as p
        """
        
        pollutants = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", pollutants_query) \
            .option("user", args['RDS_USER']) \
            .option("password", token) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        pollutants = pollutants \
            .withColumn("who_daily_limit", 
                F.when(F.col("who_daily_limit_text").isNull(), None)
                 .otherwise(F.col("who_daily_limit_text").cast(DoubleType()))) \
            .withColumn("who_annual_limit", 
                F.when(F.col("who_annual_limit_text").isNull(), None)
                 .otherwise(F.col("who_annual_limit_text").cast(DoubleType()))) \
            .drop("who_daily_limit_text", "who_annual_limit_text")
        
        pollutants = pollutants.cache()
        print(f"      ✓ {pollutants.count()} pollutants")
        
        # ===== STATIONS =====
        print("   📥 Cargando stations...")
        
        stations_query = f"""
        (SELECT 
            station_id, station_code, station_name, station_type, authority,
            CASE 
                WHEN lat IS NULL THEN NULL
                WHEN lat::TEXT = 'NaN' THEN NULL
                ELSE lat::TEXT
            END as lat_text,
            CASE 
                WHEN lon IS NULL THEN NULL
                WHEN lon::TEXT = 'NaN' THEN NULL
                ELSE lon::TEXT
            END as lon_text,
            years_min, years_max, zone_id
         FROM {args['RDS_SCHEMA']}.stations) as s
        """
        
        stations = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", stations_query) \
            .option("user", args['RDS_USER']) \
            .option("password", token) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        stations = stations \
            .withColumn("lat", 
                F.when(F.col("lat_text").isNull(), None)
                 .otherwise(F.col("lat_text").cast(DoubleType()))) \
            .withColumn("lon", 
                F.when(F.col("lon_text").isNull(), None)
                 .otherwise(F.col("lon_text").cast(DoubleType()))) \
            .withColumn("zone_id", F.col("zone_id").cast(IntegerType())) \
            .drop("lat_text", "lon_text")
        
        stations = stations.cache()
        print(f"      ✓ {stations.count()} stations")
        
        # ===== ZONES =====
        print("   📥 Cargando zones...")
        
        zones_query = f"""
        (SELECT 
            zone_id, zone_code, zone_name, city_code, city_name, population,
            CASE 
                WHEN centroid_lat IS NULL THEN NULL
                WHEN centroid_lat::TEXT = 'NaN' THEN NULL
                ELSE centroid_lat::TEXT
            END as centroid_lat_text,
            CASE 
                WHEN centroid_lon IS NULL THEN NULL
                WHEN centroid_lon::TEXT = 'NaN' THEN NULL
                ELSE centroid_lon::TEXT
            END as centroid_lon_text
         FROM {args['RDS_SCHEMA']}.zones) as z
        """
        
        zones = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", zones_query) \
            .option("user", args['RDS_USER']) \
            .option("password", token) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        zones = zones \
            .withColumn("zone_id", F.col("zone_id").cast(IntegerType())) \
            .withColumn("population", F.col("population").cast(IntegerType())) \
            .withColumn("centroid_lat", 
                F.when(F.col("centroid_lat_text").isNull(), None)
                 .otherwise(F.col("centroid_lat_text").cast(DoubleType()))) \
            .withColumn("centroid_lon", 
                F.when(F.col("centroid_lon_text").isNull(), None)
                 .otherwise(F.col("centroid_lon_text").cast(DoubleType()))) \
            .drop("centroid_lat_text", "centroid_lon_text")
        
        zones = zones.cache()
        print(f"      ✓ {zones.count()} zones")
        
        # ===== JOINS =====
        print("   🔗 Realizando JOINs...")
        
        df = df.join(F.broadcast(pollutants), on="pollutant_id", how="left")
        
        df = df.join(
            F.broadcast(stations.select(
                F.col("station_id"), F.col("station_code"), F.col("station_name"),
                F.col("station_type"), F.col("authority"),
                F.col("lat").alias("station_latitude"),
                F.col("lon").alias("station_longitude"),
                F.col("years_min").alias("station_start_year"),
                F.col("years_max").alias("station_end_year"),
                F.col("zone_id")
            )),
            on="station_id", how="left"
        )
        
        df = df.join(
            F.broadcast(zones.select(
                F.col("zone_id"), F.col("zone_code"), F.col("zone_name"),
                F.col("city_code"), F.col("city_name"),
                F.col("population").alias("zone_population"),
                F.col("centroid_lat").alias("zone_centroid_lat"),
                F.col("centroid_lon").alias("zone_centroid_lon")
            )),
            on="zone_id", how="left"
        )
        
        # WHO compliance
        df = df.withColumn(
            "who_compliance_status",
            F.when(
                (F.col("who_daily_limit").isNotNull()) & 
                (F.col("measurement_value") > F.col("who_daily_limit")),
                F.lit("Excede límite diario")
            ).when(
                (F.col("who_annual_limit").isNotNull()) & 
                (F.col("measurement_value") > F.col("who_annual_limit")),
                F.lit("Excede límite anual")
            ).otherwise(F.lit("Dentro de límites"))
        )
        
        pollutants.unpersist()
        stations.unpersist()
        zones.unpersist()
        
        print("   ✅ Datos enriquecidos")
        return df
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        raise


def load_to_redshift_upsert(df):
    """Carga a Redshift con UPSERT (evita duplicados)"""
    try:
        print("\n💾 CARGANDO A REDSHIFT (UPSERT)...")
        
        count = df.count()
        print(f"   Total: {count:,}")
        
        if count == 0:
            return 0
        
        df = df.withColumnRenamed("ts_utc", "measurement_timestamp")
        df = df.withColumn("is_valid", 
            F.when(F.col("is_valid") == True, 1).otherwise(0).cast(IntegerType()))
        
        df_final = df.select(
            F.col("year").cast(IntegerType()),
            F.col("measurement_timestamp").cast(TimestampType()),
            F.col("quarter").cast(IntegerType()),
            F.col("month_number").cast(IntegerType()),
            F.col("day_of_week").cast(IntegerType()),
            F.col("measurement_sk").cast(LongType()),
            F.col("measurement_value").cast(DoubleType()),
            F.col("measurement_unit").cast(StringType()),
            F.col("measurement_source").cast(StringType()),
            F.col("is_valid"),
            F.col("pollutant_id").cast(StringType()),
            F.col("pollutant_name").cast(StringType()),
            F.col("pollutant_default_unit").cast(StringType()),
            F.col("who_daily_limit").cast(DoubleType()),
            F.col("who_annual_limit").cast(DoubleType()),
            F.col("who_compliance_status").cast(StringType()),
            F.col("station_id").cast(StringType()),
            F.col("station_code").cast(StringType()),
            F.col("station_name").cast(StringType()),
            F.col("station_type").cast(StringType()),
            F.col("authority").cast(StringType()),
            F.col("station_latitude").cast(DoubleType()),
            F.col("station_longitude").cast(DoubleType()),
            F.col("station_start_year").cast(IntegerType()),
            F.col("station_end_year").cast(IntegerType()),
            F.col("zone_id").cast(IntegerType()),
            F.col("zone_code").cast(StringType()),
            F.col("zone_name").cast(StringType()),
            F.col("city_code").cast(StringType()),
            F.col("city_name").cast(StringType()),
            F.col("zone_population").cast(IntegerType()),
            F.col("zone_centroid_lat").cast(DoubleType()),
            F.col("zone_centroid_lon").cast(DoubleType()),
            F.col("inserted_at").cast(TimestampType()),
            F.col("processed_at").cast(TimestampType()),
            F.col("etl_loaded_at").cast(TimestampType())
        )
        
        jdbc_url = f"jdbc:redshift://{args['REDSHIFT_HOST']}:{args['REDSHIFT_PORT']}/{args['REDSHIFT_DB']}"
        
        # PASO 1: Cargar a tabla staging
        print("   📤 Escribiendo a tabla staging...")
        start_time = time.time()
        
        df_final.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"{args['REDSHIFT_SCHEMA']}.{STAGING_TABLE}") \
            .option("user", args['REDSHIFT_USER']) \
            .option("password", args['REDSHIFT_PASSWORD']) \
            .option("driver", "com.amazon.redshift.jdbc.Driver") \
            .option("batchsize", "5000") \
            .mode("overwrite") \
            .save()
        
        print("   ✅ Datos cargados en staging")
        
        # PASO 2: UPSERT desde staging a tabla final
        print("   🔄 Ejecutando UPSERT...")
        
        conn = get_redshift_connection()
        cursor = conn.cursor()
        
        # DELETE de registros existentes
        delete_query = f"""
        DELETE FROM {args['REDSHIFT_SCHEMA']}.{TARGET_TABLE}
        USING {args['REDSHIFT_SCHEMA']}.{STAGING_TABLE}
        WHERE {TARGET_TABLE}.measurement_sk = {STAGING_TABLE}.measurement_sk;
        """
        
        cursor.execute(delete_query)
        deleted_count = cursor.rowcount
        print(f"      ✓ Eliminados {deleted_count:,} registros duplicados")
        
        # INSERT de nuevos registros
        insert_query = f"""
        INSERT INTO {args['REDSHIFT_SCHEMA']}.{TARGET_TABLE}
        SELECT * FROM {args['REDSHIFT_SCHEMA']}.{STAGING_TABLE};
        """
        
        cursor.execute(insert_query)
        inserted_count = cursor.rowcount
        print(f"      ✓ Insertados {inserted_count:,} registros")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        elapsed = time.time() - start_time
        print(f"   ✅ UPSERT completado en {elapsed:.2f}s")
        
        return inserted_count
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        raise


# ============================================================================
# MAIN
# ============================================================================

def main():
    start_time = time.time()
    
    try:
        df = extract_from_rds()
        
        if df is None or df.count() == 0:
            print("\n✅ No hay datos")
            return
        
        df_enriched = enrich_data(df)
        total = load_to_redshift_upsert(df_enriched)
        
        df.unpersist()
        
        elapsed = time.time() - start_time
        
        print("\n" + "=" * 80)
        print("✅ COMPLETADO (SIN DUPLICADOS)")
        print("=" * 80)
        print(f"⏱️  Tiempo: {elapsed:.2f}s ({elapsed/60:.2f} min)")
        print(f"📊 Registros: {total:,}")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        job.commit()


if __name__ == "__main__":
    main()