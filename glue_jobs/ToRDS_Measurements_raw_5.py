"""
AWS Glue Job - PARTE 2E: CARGAR MEASUREMENTS_RAW A RDS (VERSIÓN MULTI-AÑO)
=============================================================================
Script flexible que soporta 3 modos de operación:
1. SINGLE_YEAR: Procesa un solo año (--YEAR=2022)
2. YEAR_RANGE: Procesa rango de años (--YEAR_START=2022 --YEAR_END=2025)
3. YEAR_LIST: Procesa lista específica (--YEARS=2022,2024,2025)

Características:
- Checkpoints cada 50K registros
- Limpieza de datos (nulls, negativos, duplicados)
- Optimización PostgreSQL
- Estadísticas detalladas
- ETA y velocidad de inserción
- Reconexión automática
- Validación de esquema y tabla

Job Type: Spark (2 G.1X workers)
Usa psycopg v3
"""
import sys
import boto3
import psycopg
from psycopg import OperationalError, DatabaseError, IntegrityError
from psycopg.rows import tuple_row
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# ========================================
# CONFIGURACIÓN Y ARGUMENTOS
# ========================================
# ========================================
# CONFIGURACIÓN Y ARGUMENTOS
# ========================================

print("\n" + "=" * 80)
print("🔍 PASO 1: ANALIZANDO sys.argv RAW")
print("=" * 80)
print(f"sys.argv completo ({len(sys.argv)} elementos):")
for i, arg in enumerate(sys.argv):
    # Truncar URLs muy largas para mejor legibilidad
    if len(arg) > 100 and arg.startswith('http'):
        print(f"  [{i}] {arg[:100]}... (URL truncada)")
    else:
        print(f"  [{i}] {arg}")
print("=" * 80 + "\n")

# Parsear argumentos de sys.argv
# AWS Glue usa formato: --KEY VALUE (dos elementos separados)
args_dict = {}

i = 0
while i < len(sys.argv):
    arg = sys.argv[i]
    
    if arg.startswith('--'):
        key = arg[2:]  # Remover '--'
        
        # El valor está en el siguiente elemento (si existe y no empieza con --)
        if i + 1 < len(sys.argv) and not sys.argv[i + 1].startswith('--'):
            value = sys.argv[i + 1]
            args_dict[key] = value
            i += 2  # Saltar key y value
        else:
            # Parámetro sin valor (flag booleano)
            args_dict[key] = 'true'
            i += 1
    else:
        # Elemento que no empieza con --, saltarlo
        i += 1

print("=" * 80)
print("📋 PASO 2: ARGUMENTOS PARSEADOS MANUALMENTE")
print("=" * 80)
for key in sorted(args_dict.keys()):
    value = args_dict[key]
    # Marcar parámetros de año
    marker = "🎯" if key in ['YEAR', 'YEAR_START', 'YEAR_END', 'YEARS'] else "  "
    # Truncar valores muy largos
    if len(value) > 80:
        value_display = value[:77] + "..."
    else:
        value_display = value
    print(f"{marker} {key:25} = '{value_display}'")
print("=" * 80 + "\n")

# Validar parámetros requeridos
try:
    required_args = [
        'JOB_NAME',
        'S3_BUCKET',
        'S3_PREFIX',
        'RDS_HOST',
        'RDS_PORT',
        'RDS_DB',
        'RDS_USER',
        'RDS_REGION',
        'RDS_SCHEMA',
        'BATCH_SIZE',
        'CHECKPOINT_FREQ'
    ]
    
    # Validar que todos los requeridos existan
    missing = [arg for arg in required_args if arg not in args_dict]
    if missing:
        raise ValueError(f"Faltan parámetros obligatorios: {missing}")
    
    # Usar args_dict como args principal
    args = args_dict
    
    # Agregar defaults para opcionales si no existen
    optional_defaults = {
        'YEAR': 'NONE',
        'YEAR_START': 'NONE',
        'YEAR_END': 'NONE',
        'YEARS': 'NONE',
        'FAIL_ON_ERROR': 'false',
        'SKIP_EXISTING': 'false'
    }
    
    for key, default in optional_defaults.items():
        if key not in args:
            args[key] = default
    
    print("=" * 80)
    print("✅ PASO 3: ARGUMENTOS FINALES (CON DEFAULTS)")
    print("=" * 80)
    
    # Mostrar solo los parámetros relevantes para el usuario
    relevant_keys = required_args + list(optional_defaults.keys())
    
    for key in sorted(relevant_keys):
        if key in args:
            value = args[key]
            marker = "🎯" if key in ['YEAR', 'YEAR_START', 'YEAR_END', 'YEARS'] else "  "
            print(f"{marker} {key:25} = '{value}'")
    
    print("=" * 80 + "\n")
            
except Exception as e:
    print(f"❌ ERROR: {e}")
    print(f"\n🔍 DEBUG: sys.argv = {sys.argv}")
    import traceback
    traceback.print_exc()
    raise

BATCH_SIZE = int(args.get('BATCH_SIZE', '5000'))
CHECKPOINT_FREQ = int(args.get('CHECKPOINT_FREQ', '50000'))
SCHEMA = args['RDS_SCHEMA']
MAX_RETRIES = 3
RETRY_DELAY = 5  # segundos

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("=" * 80)
print(f"📊 PARTE 2E: CARGANDO MEASUREMENTS_RAW A RDS (MULTI-AÑO)")
print("=" * 80)
print(f"⚙️  Configuración:")
print(f"   - S3 Bucket: {args['S3_BUCKET']}")
print(f"   - S3 Prefix: {args['S3_PREFIX']}")
print(f"   - Batch size: {BATCH_SIZE:,}")
print(f"   - Checkpoint cada: {CHECKPOINT_FREQ:,} registros")
print(f"   - Max reintentos: {MAX_RETRIES}")
print(f"   - Fail on error: {args['FAIL_ON_ERROR']}")
print(f"   - Skip existing: {args['SKIP_EXISTING']}")
print("=" * 80)


# ========================================
# FUNCIONES DE UTILIDAD S3
# ========================================
def s3_file_exists(s3_path):
    """Verifica si un archivo existe en S3"""
    try:
        # Extraer bucket y key del path
        s3_path = s3_path.replace('s3://', '')
        bucket = s3_path.split('/')[0]
        key = '/'.join(s3_path.split('/')[1:])
        
        s3_client = boto3.client('s3')
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except:
        return False


def get_s3_file_size(s3_path):
    """Obtiene el tamaño de un archivo en S3"""
    try:
        s3_path = s3_path.replace('s3://', '')
        bucket = s3_path.split('/')[0]
        key = '/'.join(s3_path.split('/')[1:])
        
        s3_client = boto3.client('s3')
        response = s3_client.head_object(Bucket=bucket, Key=key)
        return response['ContentLength']
    except:
        return 0


# ========================================
# DETERMINAR AÑOS A PROCESAR
# ========================================
def is_valid_param(value):
    """
    Verifica si un parámetro tiene un valor válido
    Considera None, '', 'NONE', 'none', 'null' como vacíos
    """
    if value is None:
        return False
    
    # Convertir a string y limpiar
    value_str = str(value).strip().upper()
    
    # Lista de valores considerados como "vacío"
    invalid_values = ('', 'NONE', 'NULL', 'N/A', 'NA', 'EMPTY')
    
    return value_str not in invalid_values


def safe_int(value):
    """
    Convierte un valor a entero de forma segura
    Retorna None si no es un número válido
    """
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def get_years_to_process():
    """
    Determina qué años procesar según los parámetros
    Retorna: lista de años
    """
    print(f"\n🔍 DETECTANDO MODO DE OPERACIÓN")
    print("-" * 80)
    
    # Debug: mostrar valores de los parámetros
    print(f"📋 Parámetros recibidos:")
    print(f"   YEAR: '{args['YEAR']}'")
    print(f"   YEAR_START: '{args['YEAR_START']}'")
    print(f"   YEAR_END: '{args['YEAR_END']}'")
    print(f"   YEARS: '{args['YEARS']}'")
    print("-" * 80)
    
    # Modo 1: Año único
    if is_valid_param(args['YEAR']):
        year = safe_int(args['YEAR'])
        if year is None:
            raise ValueError(f"YEAR debe ser un número válido, recibido: '{args['YEAR']}'")
        
        print(f"✅ Modo: SINGLE_YEAR")
        print(f"   Año: {year}")
        return [year]
    
    # Modo 2: Rango de años
    elif is_valid_param(args['YEAR_START']) and is_valid_param(args['YEAR_END']):
        year_start = safe_int(args['YEAR_START'])
        year_end = safe_int(args['YEAR_END'])
        
        if year_start is None:
            raise ValueError(f"YEAR_START debe ser un número válido, recibido: '{args['YEAR_START']}'")
        if year_end is None:
            raise ValueError(f"YEAR_END debe ser un número válido, recibido: '{args['YEAR_END']}'")
        
        if year_start > year_end:
            raise ValueError(f"YEAR_START ({year_start}) no puede ser mayor que YEAR_END ({year_end})")
        
        years = list(range(year_start, year_end + 1))
        print(f"✅ Modo: YEAR_RANGE")
        print(f"   Rango: {year_start} - {year_end}")
        print(f"   Total años: {len(years)}")
        return years
    
    # Modo 3: Lista de años
    elif is_valid_param(args['YEARS']):
        years_str = args['YEARS'].strip()
        
        try:
            # Dividir por coma y convertir a enteros
            years = []
            for y in years_str.split(','):
                y_clean = y.strip()
                y_int = safe_int(y_clean)
                if y_int is None:
                    raise ValueError(f"'{y_clean}' no es un año válido")
                years.append(y_int)
            
            if not years:
                raise ValueError("La lista de años está vacía")
            
            # Ordenar años
            years.sort()
            
            print(f"✅ Modo: YEAR_LIST")
            print(f"   Años: {years}")
            print(f"   Total años: {len(years)}")
            return years
            
        except Exception as e:
            raise ValueError(f"Error procesando YEARS: {e}")
    
    else:
        # Ningún parámetro válido fue proporcionado
        error_msg = (
            "❌ Debe especificar UNO de estos parámetros con valores válidos:\n"
            "\n"
            "Opción 1 - Año único:\n"
            "  --YEAR=2022\n"
            "  --YEAR_START=NONE\n"
            "  --YEAR_END=NONE\n"
            "  --YEARS=NONE\n"
            "\n"
            "Opción 2 - Rango de años:\n"
            "  --YEAR=NONE\n"
            "  --YEAR_START=2022\n"
            "  --YEAR_END=2025\n"
            "  --YEARS=NONE\n"
            "\n"
            "Opción 3 - Lista específica:\n"
            "  --YEAR=NONE\n"
            "  --YEAR_START=NONE\n"
            "  --YEAR_END=NONE\n"
            "  --YEARS=2022,2024,2025\n"
            "\n"
            "📝 Valores recibidos:\n"
            f"   YEAR: '{args['YEAR']}'\n"
            f"   YEAR_START: '{args['YEAR_START']}'\n"
            f"   YEAR_END: '{args['YEAR_END']}'\n"
            f"   YEARS: '{args['YEARS']}'"
        )
        raise ValueError(error_msg)


# ========================================
# CLASE PARA GESTIÓN DE CONEXIÓN RDS
# ========================================
class RDSConnectionManager:
    """Gestor de conexión con reconexión automática usando psycopg v3"""
    
    def __init__(self, host, port, database, user, region):
        self.host = host
        self.port = int(port)
        self.database = database
        self.user = user
        self.region = region
        self.conn = None
        self.rds_client = boto3.client('rds', region_name=region)
        
    def get_connection(self, retry_count=0):
        """Obtiene o renueva la conexión con reintentos"""
        try:
            if self.conn is None or self.conn.closed:
                print(f"🔐 Generando token IAM (intento {retry_count + 1}/{MAX_RETRIES})...")
                
                if not all([self.host, self.user, self.database]):
                    raise ValueError("RDS_HOST, RDS_USER y RDS_DB son obligatorios")
                
                token = self.rds_client.generate_db_auth_token(
                    DBHostname=self.host,
                    Port=self.port,
                    DBUsername=self.user,
                    Region=self.region
                )
                
                if not token:
                    raise ValueError("No se pudo generar el token IAM")
                
                print("📡 Conectando a RDS...")
                
                self.conn = psycopg.connect(
                    host=self.host,
                    port=self.port,
                    dbname=self.database,
                    user=self.user,
                    password=token,
                    sslmode='require',
                    connect_timeout=10
                )
                
                # Verificar conexión
                with self.conn.cursor() as cur:
                    cur.execute('SELECT 1')
                
                # Optimizaciones PostgreSQL
                with self.conn.cursor() as cur:
                    cur.execute("SET synchronous_commit = OFF")
                    cur.execute("SET work_mem = '256MB'")
                    cur.execute("SET maintenance_work_mem = '512MB'")
                
                print(f"✅ Conexión establecida")
            
            return self.conn
            
        except OperationalError as e:
            print(f"⚠️  Error de conexión a RDS: {e}")
            if retry_count < MAX_RETRIES - 1:
                print(f"   Reintentando en {RETRY_DELAY} segundos...")
                time.sleep(RETRY_DELAY)
                return self.get_connection(retry_count + 1)
            else:
                print("❌ Máximo de reintentos alcanzado")
                raise
        
        except Exception as e:
            print(f"❌ Error inesperado al conectar a RDS: {e}")
            raise
    
    def close(self):
        """Cierra la conexión"""
        if self.conn and not self.conn.closed:
            self.conn.close()
            print("🔌 Conexión cerrada")


# ========================================
# FUNCIONES DE VALIDACIÓN
# ========================================
def verify_schema_exists(conn, schema_name):
    """Verifica que el schema existe"""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.schemata 
                    WHERE schema_name = %s
                )
            """, (schema_name,))
            exists = cur.fetchone()[0]
            
            if not exists:
                print(f"⚠️  Schema '{schema_name}' no existe. Creando...")
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                conn.commit()
                print(f"   ✅ Schema '{schema_name}' creado")
            else:
                print(f"✅ Schema '{schema_name}' verificado")
            
            return True
            
    except Exception as e:
        print(f"❌ Error al verificar schema: {e}")
        raise


def verify_table_exists(conn, schema_name, table_name):
    """Verifica que la tabla existe"""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = %s AND table_name = %s
                )
            """, (schema_name, table_name))
            exists = cur.fetchone()[0]
            
            if not exists:
                raise Exception(
                    f"La tabla {schema_name}.{table_name} no existe. "
                    f"Ejecuta el DDL primero."
                )
            
            print(f"✅ Tabla {schema_name}.{table_name} verificada")
            return True
            
    except Exception as e:
        print(f"❌ Error al verificar tabla: {e}")
        raise


def check_year_already_processed(conn, schema_name, year):
    """Verifica si un año ya fue procesado"""
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT COUNT(*) 
                FROM {schema_name}.measurements_raw 
                WHERE EXTRACT(YEAR FROM ts_utc) = %s
            """, (year,))
            count = cur.fetchone()[0]
            return count > 0, count
    except:
        return False, 0


# ========================================
# FUNCIONES DE LIMPIEZA DE DATOS
# ========================================
def clean_measurements_data(df):
    """Limpia y valida los datos de mediciones"""
    print(f"\n🧹 LIMPIEZA DE DATOS")
    print("-" * 80)
    
    initial_count = len(df)
    stats = {
        'initial_count': initial_count,
        'nulls_removed': 0,
        'negative_values': 0,
        'duplicates_removed': 0,
        'invalid_timestamps': 0,
        'final_count': 0
    }
    
    # 1. Remover nulls
    print("1️⃣  Verificando valores nulos...")
    null_mask = df[['station_id', 'pollutant_id', 'ts_utc', 'value']].isnull().any(axis=1)
    stats['nulls_removed'] = null_mask.sum()
    df = df[~null_mask].copy()
    print(f"   ❌ Removidos: {stats['nulls_removed']:,} registros con nulls")
    
    # 2. Validar valores numéricos
    print("2️⃣  Validando valores numéricos...")
    negative_mask = df['value'] < 0
    stats['negative_values'] = negative_mask.sum()
    df = df[~negative_mask].copy()
    print(f"   ❌ Removidos: {stats['negative_values']:,} valores negativos")
    df = df[np.isfinite(df['value'])].copy()
    
    # 3. Validar timestamps
    print("3️⃣  Validando timestamps...")
    try:
        df['ts_utc'] = pd.to_datetime(df['ts_utc'], utc=True, errors='coerce')
        invalid_ts = df['ts_utc'].isnull()
        stats['invalid_timestamps'] = invalid_ts.sum()
        df = df[~invalid_ts].copy()
        print(f"   ❌ Removidos: {stats['invalid_timestamps']:,} timestamps inválidos")
    except Exception as e:
        print(f"   ⚠️  Error en conversión de timestamps: {e}")
    
    # 4. Remover duplicados
    print("4️⃣  Removiendo duplicados...")
    before_dedup = len(df)
    df = df.drop_duplicates(subset=['station_id', 'pollutant_id', 'ts_utc'], keep='last')
    stats['duplicates_removed'] = before_dedup - len(df)
    print(f"   ❌ Removidos: {stats['duplicates_removed']:,} duplicados")
    
    # 5. Ordenar y validar tipos
    print("5️⃣  Ordenando y validando tipos...")
    df = df.sort_values('ts_utc').reset_index(drop=True)
    
    try:
        df['station_id'] = df['station_id'].astype(int)
        df['value'] = df['value'].astype(float)
        print(f"   ✅ Tipos de datos validados")
    except Exception as e:
        print(f"   ⚠️  Error en conversión de tipos: {e}")
    
    stats['final_count'] = len(df)
    
    print("-" * 80)
    print(f"📊 RESUMEN DE LIMPIEZA:")
    print(f"   Registros iniciales:    {stats['initial_count']:>12,}")
    print(f"   - Nulls:                {stats['nulls_removed']:>12,}")
    print(f"   - Negativos:            {stats['negative_values']:>12,}")
    print(f"   - Timestamps inválidos: {stats['invalid_timestamps']:>12,}")
    print(f"   - Duplicados:           {stats['duplicates_removed']:>12,}")
    print(f"   {'=' * 40}")
    print(f"   Registros finales:      {stats['final_count']:>12,}")
    
    removed = stats['initial_count'] - stats['final_count']
    pct = (removed / stats['initial_count'] * 100) if stats['initial_count'] > 0 else 0
    print(f"   Datos removidos:        {removed:>12,} ({pct:.2f}%)")
    print("-" * 80)
    
    return df, stats


def get_data_statistics(df):
    """Genera estadísticas descriptivas del dataset"""
    print(f"\n📈 ESTADÍSTICAS DEL DATASET")
    print("-" * 80)
    
    # Rango temporal
    min_date = df['ts_utc'].min()
    max_date = df['ts_utc'].max()
    print(f"📅 Rango temporal:")
    print(f"   Desde: {min_date}")
    print(f"   Hasta: {max_date}")
    print(f"   Días:  {(max_date - min_date).days}")
    
    # Estadísticas por contaminante
    print(f"\n🌡️  Por contaminante:")
    pollutant_stats = df.groupby('pollutant_id').agg({
        'value': ['count', 'mean', 'min', 'max', 'std']
    }).round(2)
    print(pollutant_stats.to_string())
    
    # Estaciones únicas
    unique_stations = df['station_id'].nunique()
    print(f"\n📍 Estaciones únicas: {unique_stations:,}")
    
    # Top 5 estaciones
    print(f"\n🏆 Top 5 estaciones con más mediciones:")
    top_stations = df['station_id'].value_counts().head()
    for i, (station, count) in enumerate(top_stations.items(), 1):
        print(f"   {i}. {station}: {count:,}")
    
    print("-" * 80)


# ========================================
# FUNCIÓN DE CARGA CON CHECKPOINTS
# ========================================
def load_with_checkpoints(df, conn_manager, schema, batch_size, checkpoint_freq):
    """Carga datos con checkpoints periódicos y tracking de progreso"""
    print(f"\n💾 INICIANDO CARGA A RDS")
    print("=" * 80)
    
    # Convertir DataFrame a lista de tuplas usando itertuples
    records = list(df.itertuples(index=False, name=None))
    total_records = len(records)
    total_batches = (total_records + batch_size - 1) // batch_size
    
    sql = f"""
    INSERT INTO {schema}.measurements_raw 
        (station_id, pollutant_id, ts_utc, value, unit, source)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (station_id, pollutant_id, ts_utc) DO UPDATE SET
        value = EXCLUDED.value,
        unit = EXCLUDED.unit,
        source = EXCLUDED.source,
        processed_at = NULL
    """
    
    records_inserted = 0
    checkpoints = 0
    start_time = time.time()
    last_checkpoint_time = start_time
    
    print(f"📊 Total registros: {total_records:,}")
    print(f"📦 Batches: {total_batches:,}")
    print(f"✅ Checkpoints cada: {checkpoint_freq:,} registros")
    print("=" * 80)
    
    conn = conn_manager.get_connection()
    
    try:
        with conn.cursor() as cur:
            for i in range(0, total_records, batch_size):
                batch = records[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                
                # Reintentos
                for attempt in range(MAX_RETRIES):
                    try:
                        cur.executemany(sql, batch)
                        records_inserted += len(batch)
                        break
                        
                    except OperationalError as e:
                        print(f"\n⚠️  Error de conexión en lote {batch_num}, intento {attempt + 1}/{MAX_RETRIES}")
                        
                        if attempt < MAX_RETRIES - 1:
                            time.sleep(RETRY_DELAY * (attempt + 1))
                            conn = conn_manager.get_connection()
                            cur = conn.cursor()
                        else:
                            raise
                    
                    except IntegrityError as e:
                        print(f"\n⚠️  Error de integridad en lote {batch_num}: {e}")
                        conn.rollback()
                        break
                
                # Checkpoint periódico
                if records_inserted % checkpoint_freq < batch_size:
                    conn.commit()
                    checkpoints += 1
                    
                    elapsed = time.time() - start_time
                    records_per_sec = records_inserted / elapsed if elapsed > 0 else 0
                    remaining = total_records - records_inserted
                    eta_seconds = remaining / records_per_sec if records_per_sec > 0 else 0
                    eta = timedelta(seconds=int(eta_seconds))
                    
                    checkpoint_elapsed = time.time() - last_checkpoint_time
                    last_checkpoint_time = time.time()
                    
                    print(f"\n{'=' * 80}")
                    print(f"📍 CHECKPOINT #{checkpoints}")
                    print(f"   Registros: {records_inserted:,} / {total_records:,} ({records_inserted/total_records*100:.1f}%)")
                    print(f"   Velocidad: {records_per_sec:,.0f} reg/s")
                    print(f"   Transcurrido: {timedelta(seconds=int(elapsed))}")
                    print(f"   ETA: {eta}")
                    print(f"   Checkpoint: {checkpoint_elapsed:.1f}s")
                    print(f"{'=' * 80}")
                
                elif batch_num % 10 == 0:
                    progress = records_inserted / total_records * 100
                    elapsed = time.time() - start_time
                    records_per_sec = records_inserted / elapsed if elapsed > 0 else 0
                    print(f"   Batch {batch_num:>6,}/{total_batches:<6,} | {progress:>5.1f}% | {records_per_sec:>8,.0f} reg/s", end='\r')
            
            conn.commit()
            
    except Exception as e:
        print(f"\n❌ ERROR durante la carga: {e}")
        conn.rollback()
        raise
    
    total_time = time.time() - start_time
    avg_speed = records_inserted / total_time if total_time > 0 else 0
    
    print(f"\n\n{'=' * 80}")
    print(f"✅ CARGA COMPLETADA")
    print(f"{'=' * 80}")
    print(f"📊 Registros insertados: {records_inserted:,}")
    print(f"⏱️  Tiempo total: {timedelta(seconds=int(total_time))}")
    print(f"⚡ Velocidad promedio: {avg_speed:,.0f} registros/segundo")
    print(f"📍 Checkpoints: {checkpoints}")
    print(f"{'=' * 80}")
    
    return records_inserted


# ========================================
# PROCESAMIENTO DE UN AÑO
# ========================================
def process_year(year, conn_manager):
    """
    Procesa un año específico
    Retorna: (success, records_loaded, error_msg)
    """
    try:
        s3_input = f"s3://{args['S3_BUCKET']}/{args['S3_PREFIX']}/measurements_raw_{year}.parquet"
        
        print(f"\n{'='*80}")
        print(f"📅 PROCESANDO AÑO {year}")
        print(f"{'='*80}")
        
        # 1. Verificar existencia del archivo
        print(f"\n📂 Paso 1: Verificando archivo S3...")
        print(f"   Ruta: {s3_input}")
        
        if not s3_file_exists(s3_input):
            print(f"   ⚠️  Archivo no encontrado")
            return (False, 0, f"Archivo no encontrado: {s3_input}")
        
        file_size = get_s3_file_size(s3_input)
        print(f"   ✅ Archivo encontrado ({file_size / 1024:.2f} KB)")
        
        # 2. Verificar si ya fue procesado
        if args['SKIP_EXISTING'].lower() == 'true':
            print(f"\n🔍 Paso 2: Verificando si ya fue procesado...")
            conn = conn_manager.get_connection()
            already_processed, existing_count = check_year_already_processed(conn, SCHEMA, year)
            
            if already_processed:
                print(f"   ⚠️  Año {year} ya tiene {existing_count:,} registros en BD")
                print(f"   ⏭️  Saltando (SKIP_EXISTING=true)")
                return (True, existing_count, "Already processed (skipped)")
        
        # 3. Leer datos
        print(f"\n📂 Paso 3: Leyendo parquet...")
        df_spark = spark.read.parquet(s3_input)
        row_count = df_spark.count()
        
        if row_count == 0:
            print(f"   ⚠️  Archivo vacío")
            return (False, 0, "Archivo parquet vacío")
        
        print(f"   ✅ {row_count:,} registros leídos")
        df_pandas = df_spark.toPandas()
        
        # Validar columnas
        required_cols = ['station_id', 'pollutant_id', 'ts_utc', 'value', 'unit', 'source']
        missing_cols = [col for col in required_cols if col not in df_pandas.columns]
        
        if missing_cols:
            error = f"Columnas faltantes: {missing_cols}"
            print(f"   ❌ {error}")
            return (False, 0, error)
        
        print(f"   ✅ Columnas requeridas presentes")
        print(f"   Memoria: {df_pandas.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        
        # 4. Limpieza
        print(f"\n🧹 Paso 4: Limpieza y validación...")
        df_clean, clean_stats = clean_measurements_data(df_pandas)
        del df_pandas
        
        if len(df_clean) == 0:
            error = "No hay datos válidos después de limpieza"
            print(f"   ❌ {error}")
            return (False, 0, error)
        
        # 5. Estadísticas
        print(f"\n📈 Paso 5: Estadísticas...")
        get_data_statistics(df_clean)
        
        # 6. Carga
        print(f"\n💾 Paso 6: Cargando a RDS...")
        records_loaded = load_with_checkpoints(
            df_clean,
            conn_manager,
            SCHEMA,
            BATCH_SIZE,
            CHECKPOINT_FREQ
        )
        
        # 7. Verificación
        print(f"\n🔍 Paso 7: Verificando resultado...")
        conn = conn_manager.get_connection()
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {SCHEMA}.measurements_raw WHERE EXTRACT(YEAR FROM ts_utc) = %s",
                (year,)
            )
            total_in_db = cur.fetchone()[0]
            print(f"   Total en BD para {year}: {total_in_db:,} registros")
        
        print(f"\n✅ Año {year} procesado exitosamente")
        print(f"   Originales: {clean_stats['initial_count']:,}")
        print(f"   Limpiados: {clean_stats['final_count']:,}")
        print(f"   Cargados: {records_loaded:,}")
        print(f"   En BD: {total_in_db:,}")
        
        return (True, records_loaded, None)
        
    except Exception as e:
        error = f"Error procesando año {year}: {str(e)}"
        print(f"\n❌ {error}")
        import traceback
        traceback.print_exc()
        return (False, 0, error)


# ========================================
# MAIN EXECUTION
# ========================================
def main():
    """Función principal con procesamiento multi-año"""
    conn_manager = None
    
    try:
        # 1. Determinar años a procesar
        years = get_years_to_process()
        
        # 2. Conectar a RDS y validar
        print(f"\n📡 CONECTANDO A RDS")
        conn_manager = RDSConnectionManager(
            args['RDS_HOST'],
            args['RDS_PORT'],
            args['RDS_DB'],
            args['RDS_USER'],
            args['RDS_REGION']
        )
        
        conn = conn_manager.get_connection()
        
        print(f"\n🔍 VERIFICANDO SCHEMA Y TABLA")
        verify_schema_exists(conn, SCHEMA)
        verify_table_exists(conn, SCHEMA, 'measurements_raw')
        
        # 3. Procesar cada año
        results = {
            'success': [],
            'failed': [],
            'skipped': []
        }
        
        total_start_time = time.time()
        
        for idx, year in enumerate(years, 1):
            print(f"\n{'#'*80}")
            print(f"# AÑO {year} ({idx}/{len(years)})")
            print(f"{'#'*80}")
            
            year_start_time = time.time()
            success, records, error = process_year(year, conn_manager)
            year_time = time.time() - year_start_time
            
            if success:
                if error == "Already processed (skipped)":
                    results['skipped'].append((year, records))
                else:
                    results['success'].append((year, records, year_time))
            else:
                results['failed'].append((year, error))
                
                # Decidir si fallar todo el job
                if args['FAIL_ON_ERROR'].lower() == 'true':
                    raise Exception(f"Job detenido: Error en año {year}")
                else:
                    print(f"\n⚠️  Continuando con siguiente año (FAIL_ON_ERROR=false)...")
        
        # 4. Resumen final
        total_time = time.time() - total_start_time
        
        print(f"\n{'='*80}")
        print(f"🎉 PROCESAMIENTO COMPLETO")
        print(f"{'='*80}")
        
        print(f"\n✅ EXITOSOS ({len(results['success'])})")
        for year, records, year_time in results['success']:
            print(f"   {year}: {records:,} registros en {timedelta(seconds=int(year_time))}")
        
        if results['skipped']:
            print(f"\n⏭️  SALTADOS ({len(results['skipped'])})")
            for year, records in results['skipped']:
                print(f"   {year}: {records:,} registros ya existentes")
        
        if results['failed']:
            print(f"\n❌ FALLIDOS ({len(results['failed'])})")
            for year, error in results['failed']:
                print(f"   {year}: {error}")
        
        print(f"\n⏱️  Tiempo total: {timedelta(seconds=int(total_time))}")
        
        total_records = sum(r[1] for r in results['success'])
        print(f"📊 Total registros procesados: {total_records:,}")
        
        print(f"{'='*80}")
        
        # Decidir si el job fue exitoso
        if results['failed'] and args['FAIL_ON_ERROR'].lower() == 'true':
            raise Exception(f"Job falló: {len(results['failed'])} años no procesados")
        
    except Exception as e:
        print(f"\n{'='*80}")
        print(f"❌ ERROR FATAL EN EL JOB")
        print(f"{'='*80}")
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
        
    finally:
        if conn_manager is not None:
            try:
                conn_manager.close()
            except Exception as e:
                print(f"⚠️  Error al cerrar conexión: {e}")
        
        try:
            job.commit()
        except Exception as e:
            print(f"⚠️  Error al hacer commit del job: {e}")


if __name__ == "__main__":
    main()