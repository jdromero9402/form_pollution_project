"""
AWS Glue Job - PARTE 2-3: CARGAR POLLUTANTS A RDS
Lee pollutants.parquet desde S3 y carga a RDS con IAM Auth
Job Type: Spark (2 G.1X workers)
"""

import sys
import boto3
import psycopg
from psycopg import OperationalError, DatabaseError, IntegrityError
from psycopg.rows import tuple_row
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import time

## @params: [JOB_NAME]
# ============================================================================
# PARÁMETROS
# ============================================================================
try:
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'S3_INPUT',
        'RDS_HOST',
        'RDS_PORT',
        'RDS_DB',
        'RDS_USER',
        'RDS_REGION',
        'RDS_SCHEMA',
        'BATCH_SIZE'
    ])
except Exception as e:
    print(f"❌ ERROR: Falta algún parámetro obligatorio: {e}")
    raise

BATCH_SIZE = int(args.get('BATCH_SIZE', '1000'))
SCHEMA = args['RDS_SCHEMA']
MAX_RETRIES = 3
RETRY_DELAY = 5  # segundos

# DEBUG: imprime argv para verificar qué llegó
print("sys.argv:", sys.argv)

# ============================================================================
# INICIALIZAR
# ============================================================================
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("=" * 70)
print("PARTE 2-2: CARGANDO POLLUTANTS A RDS")
print("=" * 70)

# ============================================================================
# FUNCIONES
# ============================================================================

def get_rds_connection(retry_count=0):
    """
    Genera token IAM y conecta a RDS con reintentos
    
    Args:
        retry_count: Número de reintento actual
    
    Returns:
        psycopg.connection: Conexión activa
    
    Raises:
        Exception: Si falla después de MAX_RETRIES
    """
    try:
        print(f"🔐 Generando token IAM (intento {retry_count + 1}/{MAX_RETRIES})...")
        
        # Validar parámetros
        if not all([args['RDS_HOST'], args['RDS_USER'], args['RDS_DB']]):
            raise ValueError("RDS_HOST, RDS_USER y RDS_DB son obligatorios")
        
        rds_client = boto3.client('rds', region_name=args['RDS_REGION'])
        
        token = rds_client.generate_db_auth_token(
            DBHostname=args['RDS_HOST'],
            Port=int(args['RDS_PORT']),
            DBUsername=args['RDS_USER'],
            Region=args['RDS_REGION']
        )
        
        if not token:
            raise ValueError("No se pudo generar el token IAM")
        
        print("📡 Conectando a RDS...")
        
        conn = psycopg.connect(
            host=args['RDS_HOST'],
            port=int(args['RDS_PORT']),
            dbname=args['RDS_DB'],
            user=args['RDS_USER'],
            password=token,
            sslmode='require',
            connect_timeout=10
        )
        
        # Verificar que la conexión está activa
        with conn.cursor() as cur:
            cur.execute('SELECT 1')
        
        print("✅ Conexión establecida exitosamente")
        return conn
        
    except OperationalError as e:
        print(f"⚠️  Error de conexión a RDS: {e}")
        if retry_count < MAX_RETRIES - 1:
            print(f"   Reintentando en {RETRY_DELAY} segundos...")
            time.sleep(RETRY_DELAY)
            return get_rds_connection(retry_count + 1)
        else:
            print("❌ Máximo de reintentos alcanzado")
            raise
    
    except Exception as e:
        print(f"❌ Error inesperado al conectar a RDS: {e}")
        raise

def verify_schema_exists(conn, schema_name):
    """
    Verifica que el schema existe en la base de datos
    
    Args:
        conn: Conexión a PostgreSQL
        schema_name: Nombre del schema a verificar
    
    Returns:
        bool: True si existe, False si no
    """
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
                print(f"⚠️  ADVERTENCIA: El schema '{schema_name}' no existe")
                print(f"   Creando schema...")
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                conn.commit()
                print(f"   ✅ Schema '{schema_name}' creado")
            
            return True
            
    except Exception as e:
        print(f"⚠️  Error al verificar schema: {e}")
        return False

def verify_table_exists(conn, schema_name, table_name):
    """
    Verifica que la tabla existe en el schema
    
    Args:
        conn: Conexión a PostgreSQL
        schema_name: Nombre del schema
        table_name: Nombre de la tabla
    
    Returns:
        bool: True si existe
    
    Raises:
        Exception: Si la tabla no existe
    """
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

def load_data_with_retry(conn, sql, records, batch_size, retry_count=0):
    """
    Carga datos con manejo de errores y reintentos
    
    Args:
        conn: Conexión a PostgreSQL
        sql: Query SQL de inserción
        records: Datos a insertar
        batch_size: Tamaño del lote
        retry_count: Número de reintento actual
    
    Returns:
        int: Número de registros insertados
    
    Raises:
        Exception: Si falla después de MAX_RETRIES
    """
    try:
        inserted_count = 0
        total_records = len(records)
        
        print(f"💾 Insertando {total_records:,} registros en lotes de {batch_size}...")
        
        with conn.cursor() as cur:
            # Procesar en lotes
            for i in range(0, total_records, batch_size):
                batch = records[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (total_records + batch_size - 1) // batch_size
                
                try:
                    #extras.execute_values(cur, sql, batch, page_size=batch_size)
                    cur.executemany(sql, batch)
                    inserted_count += len(batch)
                    
                    # Mostrar progreso
                    if batch_num % 10 == 0 or batch_num == total_batches:
                        progress = (inserted_count / total_records) * 100
                        print(f"   Progreso: {progress:.1f}% ({inserted_count:,}/{total_records:,})")
                
                except IntegrityError as e:
                    print(f"⚠️  Error de integridad en lote {batch_num}: {e}")
                    print(f"   Continuando con el siguiente lote...")
                    conn.rollback()
                    continue
                
                except DatabaseError as e:
                    print(f"⚠️  Error de base de datos en lote {batch_num}: {e}")
                    conn.rollback()
                    raise
            
            # Commit final
            conn.commit()
            print(f"✅ Commit exitoso: {inserted_count:,} registros")
        
        return inserted_count
        
    except OperationalError as e:
        print(f"⚠️  Error de conexión durante la carga: {e}")
        conn.rollback()
        
        if retry_count < MAX_RETRIES - 1:
            print(f"   Reintentando en {RETRY_DELAY} segundos...")
            time.sleep(RETRY_DELAY)
            
            # Reconectar
            conn.close()
            conn = get_rds_connection()
            
            return load_data_with_retry(conn, sql, records, batch_size, retry_count + 1)
        else:
            print("❌ Máximo de reintentos alcanzado")
            raise
    
    except Exception as e:
        print(f"❌ Error inesperado durante la carga: {e}")
        conn.rollback()
        raise

# ============================================================================
# PROCESO PRINCIPAL
# ============================================================================

def main():
    """Función principal con manejo completo de errores"""
    conn = None
    
    try:
        # 1. LEER DATOS DESDE S3
        print("\n📂 PASO 1: Leyendo pollutants.parquet...")
        print(f"   Ruta: {args['S3_INPUT']}")
        
        try:
            df_spark = spark.read.parquet(args['S3_INPUT'])
            row_count = df_spark.count()
            
            if row_count == 0:
                raise ValueError("El archivo parquet está vacío")
            
            print(f"   ✅ {row_count} registros leídos")
            
            df_pandas = df_spark.toPandas()
            
            # Validar columnas requeridas
            required_cols = ['pollutant_id', 'name', 'default_unit', 
                           'who_daily_limit', 'who_annual_limit']
            missing_cols = [col for col in required_cols if col not in df_pandas.columns]
            
            if missing_cols:
                raise ValueError(f"Columnas faltantes en el parquet: {missing_cols}")
            
            print(f"   ✅ Todas las columnas requeridas presentes")
            
        except Exception as e:
            print(f"❌ Error al leer el archivo parquet: {e}")
            raise
        
        # 2. CONECTAR A RDS
        print("\n📡 PASO 2: Conectando a RDS...")
        conn = get_rds_connection()
        
        # 3. VERIFICAR SCHEMA Y TABLA
        print(f"\n🔍 PASO 3: Verificando schema y tabla...")
        verify_schema_exists(conn, SCHEMA)
        verify_table_exists(conn, SCHEMA, 'pollutants')
        
        # 4. PREPARAR DATOS
        print("\n📋 PASO 4: Preparando datos para inserción...")
        
        try:
            # Validar que pollutant sea string no vacío
            if df_pandas['pollutant_id'].isnull().any():
                raise ValueError("Columna 'pollutant' contiene valores nulos")
            
            records = list(df_pandas.itertuples(index=False, name=None))
            print(f"   ✅ {len(records)} registros preparados")
            
        except Exception as e:
            print(f"❌ Error al preparar datos: {e}")
            raise
        
        # 5. INSERTAR DATOS
        print("\n💾 PASO 5: Insertando datos en RDS...")
        
        sql = f"""
        INSERT INTO {SCHEMA}.pollutants 
            (pollutant_id, name, default_unit, who_daily_limit, who_annual_limit)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (pollutant_id) DO UPDATE SET
            name = EXCLUDED.name,
            default_unit = EXCLUDED.default_unit,
            who_daily_limit = EXCLUDED.who_daily_limit,
            who_annual_limit = EXCLUDED.who_annual_limit
        """
        
        inserted = load_data_with_retry(conn, sql, records, BATCH_SIZE)
        
        # 6. VERIFICAR RESULTADO
        print("\n🔍 PASO 6: Verificando datos insertados...")
        
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.pollutants")
            total_in_db = cur.fetchone()[0]
            print(f"   Total en BD: {total_in_db:,} registros")
        
        # 7. ÉXITO
        print("\n" + "=" * 70)
        print("✅ JOB COMPLETADO EXITOSAMENTE")
        print("=" * 70)
        print(f"Registros procesados: {len(records):,}")
        print(f"Registros insertados: {inserted:,}")
        print(f"Total en base de datos: {total_in_db:,}")
        
    except Exception as e:
        print("\n" + "=" * 70)
        print("❌ JOB FALLÓ")
        print("=" * 70)
        print(f"Error: {e}")
        
        # Log detallado para debugging
        import traceback
        print("\nStack trace completo:")
        traceback.print_exc()
        
        raise
    
    finally:
        # CLEANUP: Siempre cerrar conexión
        if conn is not None:
            try:
                conn.close()
                print("\n🔌 Conexión cerrada")
            except Exception as e:
                print(f"⚠️  Error al cerrar conexión: {e}")
        
        # Commit del job de Glue
        try:
            job.commit()
        except Exception as e:
            print(f"⚠️  Error al hacer commit del job: {e}")

# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    main()