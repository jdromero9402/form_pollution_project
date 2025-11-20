"""
AWS Glue Job - GENERACIÓN DE DATASETS SINTÉTICOS
Versión 3.0 - Corregido para archivos únicos y mejor debugging
"""

import sys
import boto3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import hashlib
import traceback
import os
import io

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# ============================================================================
# INICIALIZACIÓN Y LOGGING
# ============================================================================

def log(message, level="INFO"):
    """Log con timestamp"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] [{level}] {message}")
    sys.stdout.flush()

log("=" * 70)
log("INICIANDO GLUE JOB - DATASETS SINTÉTICOS V3")
log("=" * 70)

# ============================================================================
# OBTENER Y VALIDAR PARÁMETROS
# ============================================================================

def get_arg(name, default=None):
    """Obtiene argumentos de forma robusta"""
    try:
        return getResolvedOptions(sys.argv, [name])[name]
    except Exception:
        flag = f'--{name}'
        for i, arg in enumerate(sys.argv):
            if arg.startswith(flag + '='):
                return arg.split('=', 1)[1]
            if arg == flag and i + 1 < len(sys.argv):
                return sys.argv[i + 1]
        return os.getenv(name, default)

log("Validando parámetros...")
log(f"sys.argv: {sys.argv}")

# Validar parámetros obligatorios
PARAMS = {
    'S3_INPUT': get_arg('S3_INPUT'),
    'S3_OUTPUT': get_arg('S3_OUTPUT'),
    'START_YEAR': get_arg('START_YEAR'),
    'END_YEAR': get_arg('END_YEAR'),
    'END_MONTH': get_arg('END_MONTH', '12'),  # Mes final (1-12), default 12
    'END_DAY': get_arg('END_DAY', '31'),      # Día final (1-31), default 31
    'BASE_DATA_END': get_arg('BASE_DATA_END'),
    'JOB_NAME': get_arg('JOB_NAME', 'generate-synthetic-data')
}

# Verificar parámetros obligatorios
missing = []
for key in ['S3_INPUT', 'S3_OUTPUT', 'START_YEAR', 'END_YEAR', 'BASE_DATA_END']:
    if not PARAMS[key]:
        missing.append(key)
        log(f"❌ Parámetro faltante: {key}", "ERROR")
    else:
        log(f"✅ {key} = {PARAMS[key]}")

# Mostrar parámetros opcionales
log(f"✅ END_MONTH = {PARAMS['END_MONTH']} (opcional, default: 12)")
log(f"✅ END_DAY = {PARAMS['END_DAY']} (opcional, default: 31)")

if missing:
    error_msg = f"""
PARÁMETROS FALTANTES: {', '.join(missing)}

Configura en Glue Job Parameters:
--S3_INPUT          s3://bucket/path/file.csv
--S3_OUTPUT         s3://bucket/path/output
--START_YEAR        2018
--END_YEAR          2025
--END_MONTH         12                    (opcional: mes final 1-12, default: 12)
--END_DAY           31                    (opcional: día final 1-31, default: 31)
--BASE_DATA_END     2023
"""
    log(error_msg, "ERROR")
    raise ValueError(f"Parámetros faltantes: {', '.join(missing)}")

# Convertir a tipos correctos
try:
    START_YEAR = int(PARAMS['START_YEAR'])
    END_YEAR = int(PARAMS['END_YEAR'])
    END_MONTH = int(PARAMS['END_MONTH'])
    END_DAY = int(PARAMS['END_DAY'])
    BASE_DATA_END = int(PARAMS['BASE_DATA_END'])
    S3_INPUT = PARAMS['S3_INPUT'].strip()
    S3_OUTPUT = PARAMS['S3_OUTPUT'].rstrip('/')
    JOB_NAME = PARAMS['JOB_NAME']
except Exception as e:
    log(f"Error convirtiendo parámetros: {e}", "ERROR")
    raise ValueError(f"Error en parámetros: {e}")

# Validaciones
if START_YEAR >= END_YEAR:
    error_msg = f"START_YEAR ({START_YEAR}) debe ser < END_YEAR ({END_YEAR})"
    log(f"❌ {error_msg}", "ERROR")
    raise ValueError(error_msg)

if BASE_DATA_END >= END_YEAR:
    error_msg = f"BASE_DATA_END ({BASE_DATA_END}) debe ser < END_YEAR ({END_YEAR})"
    log(f"❌ {error_msg}", "ERROR")
    raise ValueError(error_msg)

if not S3_INPUT.startswith('s3://') or not S3_OUTPUT.startswith('s3://'):
    error_msg = "Las rutas S3 deben comenzar con s3://"
    log(f"❌ {error_msg}", "ERROR")
    raise ValueError(error_msg)

# Validar fecha final
if not (1 <= END_MONTH <= 12):
    error_msg = f"END_MONTH debe estar entre 1 y 12 (recibido: {END_MONTH})"
    log(f"❌ {error_msg}", "ERROR")
    raise ValueError(error_msg)

if not (1 <= END_DAY <= 31):
    error_msg = f"END_DAY debe estar entre 1 y 31 (recibido: {END_DAY})"
    log(f"❌ {error_msg}", "ERROR")
    raise ValueError(error_msg)

# Validar que la fecha es válida
try:
    END_DATE = datetime(END_YEAR, END_MONTH, END_DAY)
    END_DATE_STR = END_DATE.strftime('%Y-%m-%d')
    log(f"✅ Fecha final de proyección: {END_DATE_STR}")
except ValueError as e:
    error_msg = f"Fecha inválida: {END_YEAR}-{END_MONTH:02d}-{END_DAY:02d} ({e})"
    log(f"❌ {error_msg}", "ERROR")
    raise ValueError(error_msg)

log("✅ Todos los parámetros validados correctamente")

# Configuración
COL_CONFIG = {
    'col_station_id': 'ID Estacion',
    'col_station': 'Estación',
    'col_authority': 'Autoridad Ambiental',
    'col_dept': 'Nombre del Departamento',
    'col_muni': 'Nombre del Municipio',
    'col_pollutant': 'Variable',
    'col_year': 'Año',
    'col_value': 'Promedio',
    'col_lat': 'Latitud',
    'col_lon': 'Longitud',
    'col_station_type': 'Tipo de Estación',
}

POLLUTANTS_INFO = {
    'PM2.5': {'code': 'pm25', 'unit': 'µg/m³', 'daily': 15.0, 'annual': 5.0},
    'PM10': {'code': 'pm10', 'unit': 'µg/m³', 'daily': 45.0, 'annual': 15.0},
    'O3': {'code': 'o3', 'unit': 'µg/m³', 'daily': 100.0, 'annual': None},
    'NO2': {'code': 'no2', 'unit': 'µg/m³', 'daily': 25.0, 'annual': 10.0},
    'SO2': {'code': 'so2', 'unit': 'µg/m³', 'daily': 40.0, 'annual': None},
    'CO': {'code': 'co', 'unit': 'mg/m³', 'daily': 4.0, 'annual': None},
}

# ============================================================================
# INICIALIZAR SPARK Y GLUE
# ============================================================================

log("Inicializando Spark y Glue...")

try:
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(JOB_NAME, {})
    log(f"✅ Spark {spark.version} inicializado")
except Exception as e:
    log(f"❌ Error inicializando Spark: {e}", "ERROR")
    traceback.print_exc()
    sys.exit(1)

# Configuración de Spark
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "50")

log("=" * 70)
log(f"📥 Input: {S3_INPUT}")
log(f"📤 Output: {S3_OUTPUT}/processed/")
log(f"📅 Rango: {START_YEAR}-{END_YEAR}")
log(f"📆 Fecha final proyección: {END_DATE_STR}")
log("=" * 70)

# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================

def normalize_name(name):
    """Normaliza nombres"""
    import unicodedata
    if isinstance(name, pd.Series):
        name = name.iloc[0] if len(name) > 0 else ''
    if pd.isna(name) or name is None:
        return ''
    name = str(name).strip()
    name = unicodedata.normalize('NFD', name)
    name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
    return name.title()

def generate_stable_id(text, prefix='', length=8):
    """Genera ID estable"""
    hash_obj = hashlib.md5(text.encode('utf-8'))
    return f"{prefix}{hash_obj.hexdigest()[:length]}"

def generate_daily_series(annual_mean, start_date, end_date, pollutant_code, seed=None, 
                          historical_trend=0.0, is_extended=False):
    """Genera serie diaria sintética"""
    if seed:
        np.random.seed(seed)
    
    dates = pd.date_range(start_date, end_date, freq='D')
    n_days = len(dates)
    
    if is_extended and historical_trend != 0:
        years_from_base = (dates - pd.Timestamp(f'{BASE_DATA_END}-12-31')).days / 365.25
        trend_factor = (1 + historical_trend) ** years_from_base.values
        adjusted_mean = annual_mean * trend_factor
    else:
        adjusted_mean = annual_mean
    
    if isinstance(adjusted_mean, np.ndarray):
        values = np.array([np.random.normal(m, m * 0.2) for m in adjusted_mean])
    else:
        values = np.random.normal(adjusted_mean, adjusted_mean * 0.2, n_days)
    
    day_of_year = dates.dayofyear
    seasonal = 0.15 * annual_mean * np.sin(2 * np.pi * day_of_year / 365)
    values += seasonal
    
    if pollutant_code in ['pm25', 'pm10', 'no2', 'co']:
        day_of_week = dates.dayofweek
        weekend_factor = np.where(day_of_week >= 5, -0.2, 0.1)
        if isinstance(adjusted_mean, np.ndarray):
            values += weekend_factor * adjusted_mean
        else:
            values += weekend_factor * annual_mean
    
    n_spikes = int(n_days * 0.02)
    if n_spikes > 0:
        spike_indices = np.random.choice(n_days, n_spikes, replace=False)
        values[spike_indices] *= np.random.uniform(1.5, 2.5, n_spikes)
    
    values = np.maximum(values, 0)
    decimals = 1 if pollutant_code == 'co' else 2
    values = np.round(values, decimals)
    
    return dates, values

def save_single_parquet(df_pandas, s3_path, description):
    """
    Guarda DataFrame de Pandas como UN SOLO archivo parquet en S3
    Usa boto3 directamente para evitar particiones de Spark
    """
    try:
        log(f"Guardando {description}...")
        
        if len(df_pandas) == 0:
            log(f"⚠️  {description} vacío, saltando", "WARNING")
            return False
        
        # Convertir a parquet en memoria
        buffer = io.BytesIO()
        df_pandas.to_parquet(buffer, engine='pyarrow', index=False, compression='snappy')
        buffer.seek(0)
        
        # Extraer bucket y key
        s3_path_clean = s3_path.replace('s3://', '')
        parts = s3_path_clean.split('/', 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ''
        
        # Asegurar que termina en .parquet
        if not key.endswith('.parquet'):
            key = key.rstrip('/') + '.parquet'
        
        # Subir a S3
        s3_client = boto3.client('s3')
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.getvalue(),
            ContentType='application/octet-stream'
        )
        
        log(f"✅ {description} guardado: s3://{bucket}/{key}")
        log(f"   Registros: {len(df_pandas):,}")
        return True
        
    except Exception as e:
        log(f"❌ Error guardando {description}: {e}", "ERROR")
        traceback.print_exc()
        return False

# ============================================================================
# CARGA DE DATOS
# ============================================================================

def load_base_dataset():
    """Carga dataset base desde S3"""
    log("=" * 70)
    log("FASE 1: CARGANDO DATASET BASE")
    log("=" * 70)
    
    try:
        log(f"Leyendo desde: {S3_INPUT}")
        
        # Verificar archivo existe
        s3 = boto3.client('s3')
        bucket = S3_INPUT.replace('s3://', '').split('/')[0]
        key = '/'.join(S3_INPUT.replace('s3://', '').split('/')[1:])
        
        log(f"Bucket: {bucket}")
        log(f"Key: {key}")
        
        try:
            response = s3.head_object(Bucket=bucket, Key=key)
            size_mb = response['ContentLength'] / 1024 / 1024
            log(f"✅ Archivo encontrado: {size_mb:.2f} MB")
        except Exception as e:
            log(f"❌ No se puede acceder al archivo: {e}", "ERROR")
            log("Verifica permisos IAM y ruta S3", "ERROR")
            raise
        
        # Leer CSV con Spark
        log("Leyendo CSV con Spark...")
        df_spark = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("encoding", "UTF-8") \
            .csv(S3_INPUT)
        
        count = df_spark.count()
        if count == 0:
            log("❌ CSV vacío", "ERROR")
            raise ValueError("CSV sin datos")
        
        log(f"✅ {count:,} registros leídos")
        
        # Convertir a Pandas
        df_pandas = df_spark.toPandas()
        df_pandas.columns = df_pandas.columns.str.strip()
        
        log(f"Columnas: {list(df_pandas.columns)}")
        
        # Validar columnas
        required = list(COL_CONFIG.values())
        missing = [col for col in required if col not in df_pandas.columns]
        if missing:
            log(f"❌ Columnas faltantes: {missing}", "ERROR")
            raise ValueError(f"Columnas faltantes: {missing}")
        
        log("✅ Columnas validadas")
        
        # Convertir tipos
        numeric_cols = [COL_CONFIG['col_year'], COL_CONFIG['col_value'], 
                       COL_CONFIG['col_lat'], COL_CONFIG['col_lon']]
        
        for col in numeric_cols:
            if df_pandas[col].dtype == 'object':
                df_pandas[col] = df_pandas[col].astype(str).str.replace(',', '').astype(float)
        
        # Filtrar contaminantes
        pollutants = list(POLLUTANTS_INFO.keys())
        df_pandas = df_pandas[df_pandas[COL_CONFIG['col_pollutant']].isin(pollutants)]
        
        if len(df_pandas) == 0:
            log("❌ No hay contaminantes de aire en el dataset", "ERROR")
            raise ValueError("Sin contaminantes de aire")
        
        log(f"📊 Dataset procesado:")
        log(f"   Registros: {len(df_pandas):,}")
        log(f"   Años: {df_pandas[COL_CONFIG['col_year']].min():.0f}-{df_pandas[COL_CONFIG['col_year']].max():.0f}")
        log(f"   Estaciones: {df_pandas[COL_CONFIG['col_station']].nunique()}")
        log(f"   Contaminantes: {sorted(df_pandas[COL_CONFIG['col_pollutant']].unique())}")
        
        return df_pandas
        
    except Exception as e:
        log(f"❌ ERROR cargando dataset: {e}", "ERROR")
        traceback.print_exc()
        raise

# ============================================================================
# GENERACIÓN DE CATÁLOGOS
# ============================================================================

def generate_zones(df_base):
    """Genera tabla zones"""
    log("Generando zones...")
    
    zones_df = df_base[[COL_CONFIG['col_dept'], COL_CONFIG['col_muni']]].drop_duplicates()
    
    zones_list = []
    for idx, row in zones_df.iterrows():
        dept = normalize_name(row[COL_CONFIG['col_dept']])
        muni = normalize_name(row[COL_CONFIG['col_muni']])
        city_code = generate_stable_id(f"{dept}-{muni}", length=6).upper()
        
        zones_list.append({
            'zone_id': int(idx + 1),
            'city_code': city_code,
            'city_name': muni,
            'zone_code': f"Z{idx+1:02d}",
            'zone_name': f"Zona {muni}",
            'population': int(np.random.randint(50000, 1500000)),
            'centroid_lat': round(float(np.random.uniform(1.0, 11.0)), 6),
            'centroid_lon': round(float(np.random.uniform(-79.0, -66.0)), 6)
        })
    
    zones_df = pd.DataFrame(zones_list)
    log(f"✅ {len(zones_df)} zonas generadas")
    return zones_df

def generate_stations(df_base, zones_df):
    """Genera tabla stations"""
    log("Generando stations...")
    
    city_to_zone = dict(zip(zones_df['city_name'], zones_df['zone_id']))
    
    station_cols = [COL_CONFIG['col_station_id'], COL_CONFIG['col_station'], 
                    COL_CONFIG['col_dept'], COL_CONFIG['col_muni'],
                    COL_CONFIG['col_lat'], COL_CONFIG['col_lon'],
                    COL_CONFIG['col_authority'], COL_CONFIG['col_station_type'],
                    COL_CONFIG['col_year']]
    
    stations_df = df_base[station_cols].copy()
    
    agg_dict = {
        COL_CONFIG['col_station']: 'first',
        COL_CONFIG['col_dept']: 'first',
        COL_CONFIG['col_muni']: 'first',
        COL_CONFIG['col_year']: ['min', 'max'],
        COL_CONFIG['col_lat']: 'first',
        COL_CONFIG['col_lon']: 'first',
        COL_CONFIG['col_authority']: 'first',
        COL_CONFIG['col_station_type']: 'first'
    }
    
    grouped = stations_df.groupby([COL_CONFIG['col_station_id']]).agg(agg_dict).reset_index()
    
    stations_list = []
    for idx, row in grouped.iterrows():
        station_id_val = row[COL_CONFIG['col_station_id']]
        if isinstance(station_id_val, pd.Series):
            station_id_val = station_id_val.iloc[0]
        
        station_id_orig = str(station_id_val).replace(',', '').strip()
        station_name = normalize_name(row[(COL_CONFIG['col_station'], 'first')])
        muni = normalize_name(row[(COL_CONFIG['col_muni'], 'first')])
        zone_id = city_to_zone.get(muni, 1)
        
        years_max_original = int(row[(COL_CONFIG['col_year'], 'max')])
        years_max_synthetic = END_YEAR if years_max_original >= BASE_DATA_END else years_max_original
        
        try:
            stations_list.append({
                'station_id': int(float(station_id_orig)),
                'station_code': f"ST-{station_id_orig}",
                'station_name': station_name,
                'zone_id': int(zone_id),
                'lat': round(float(row[(COL_CONFIG['col_lat'], 'first')]), 6),
                'lon': round(float(row[(COL_CONFIG['col_lon'], 'first')]), 6),
                'station_type': str(row[(COL_CONFIG['col_station_type'], 'first')]),
                'authority': str(row[(COL_CONFIG['col_authority'], 'first')]),
                'years_min': int(row[(COL_CONFIG['col_year'], 'min')]),
                'years_max': int(years_max_synthetic)
            })
        except Exception as e:
            log(f"⚠️  Saltando estación {station_id_orig}: {e}", "WARNING")
    
    stations_df = pd.DataFrame(stations_list).drop_duplicates(subset=['station_id'], keep='first')
    log(f"✅ {len(stations_df)} estaciones generadas")
    return stations_df

def generate_pollutants():
    """Genera tabla pollutants"""
    log("Generando pollutants...")
    
    pollutants_list = []
    for name, info in POLLUTANTS_INFO.items():
        pollutants_list.append({
            'pollutant_id': info['code'],
            'name': name,
            'default_unit': info['unit'],
            'who_daily_limit': info['daily'],
            'who_annual_limit': info['annual']
        })
    
    pollutants_df = pd.DataFrame(pollutants_list)
    log(f"✅ {len(pollutants_df)} contaminantes generados")
    return pollutants_df

def generate_station_capabilities(df_base, stations_df):
    """Genera tabla station_capabilities"""
    log("Generando station_capabilities...")
    
    station_id_map = {}
    for _, row in stations_df.iterrows():
        orig_id = row['station_code'].replace('ST-', '')
        station_id_map[orig_id] = row['station_id']
    
    pollutant_map = {name: info['code'] for name, info in POLLUTANTS_INFO.items()}
    unit_map = {name: info['unit'] for name, info in POLLUTANTS_INFO.items()}
    
    caps_df = df_base[[COL_CONFIG['col_station_id'], COL_CONFIG['col_pollutant']]].drop_duplicates()
    
    caps_list = []
    for _, row in caps_df.iterrows():
        station_id_orig = str(row[COL_CONFIG['col_station_id']]).replace(',', '')
        pollutant_name = row[COL_CONFIG['col_pollutant']].strip()
        
        station_id = station_id_map.get(station_id_orig)
        pollutant_code = pollutant_map.get(pollutant_name)
        
        if station_id and pollutant_code:
            caps_list.append({
                'station_id': int(station_id),
                'pollutant_id': pollutant_code,
                'unit': unit_map.get(pollutant_name, 'µg/m³')
            })
    
    caps_df = pd.DataFrame(caps_list).drop_duplicates()
    log(f"✅ {len(caps_df)} capacidades generadas")
    return caps_df

# ============================================================================
# GENERACIÓN DE MEASUREMENTS
# ============================================================================

def calculate_historical_trends(df_base):
    """Calcula tendencias históricas"""
    log("Calculando tendencias...")
    
    pollutant_map = {name: info['code'] for name, info in POLLUTANTS_INFO.items()}
    
    df_with_id = df_base.copy()
    df_with_id['station_id_orig'] = df_with_id[COL_CONFIG['col_station_id']].astype(str).str.replace(',', '')
    
    yearly_data = df_with_id.groupby([
        'station_id_orig',
        COL_CONFIG['col_pollutant'], 
        COL_CONFIG['col_year']
    ])[COL_CONFIG['col_value']].mean().reset_index()
    
    trends = {}
    
    for (station_id, pollutant), group in yearly_data.groupby(['station_id_orig', COL_CONFIG['col_pollutant']]):
        if len(group) >= 3:
            years = group[COL_CONFIG['col_year']].values
            values = group[COL_CONFIG['col_value']].values
            
            if len(years) > 1 and values.std() > 0:
                coeffs = np.polyfit(years - years.min(), values, 1)
                slope = coeffs[0]
                mean_value = values.mean()
                trend_pct = slope / mean_value if mean_value > 0 else 0
                trend_pct = np.clip(trend_pct, -0.1, 0.1)
                
                pollutant_code = pollutant_map.get(pollutant.strip())
                if pollutant_code:
                    trends[(station_id, pollutant_code)] = trend_pct
    
    log(f"✅ {len(trends)} tendencias calculadas")
    return trends

def generate_measurements(df_base, stations_df, station_caps, trends):
    """Genera measurements por año"""
    log("=" * 70)
    log("FASE 2: GENERANDO MEASUREMENTS")
    log("=" * 70)
    log(f"Periodo: {START_YEAR}-{END_YEAR}")
    log(f"Proyectando hasta: {END_DATE_STR}")
    
    pollutant_map = {name: info['code'] for name, info in POLLUTANTS_INFO.items()}
    
    df_with_id = df_base.copy()
    df_with_id['station_id_orig'] = df_with_id[COL_CONFIG['col_station_id']].astype(str).str.replace(',', '')
    annual_means = df_with_id.groupby(['station_id_orig', COL_CONFIG['col_pollutant']])[COL_CONFIG['col_value']].mean()
    
    measurements_by_year = {year: [] for year in range(START_YEAR, END_YEAR + 1)}
    total_combinations = len(station_caps)
    
    for idx, cap in station_caps.iterrows():
        if idx % 10 == 0:
            log(f"Procesando: {idx+1}/{total_combinations}")
        
        station_id = cap['station_id']
        pollutant_code = cap['pollutant_id']
        unit = cap['unit']
        
        station_code = stations_df[stations_df['station_id'] == station_id]['station_code'].iloc[0]
        station_id_orig = station_code.replace('ST-', '')
        pollutant_name = [k for k, v in pollutant_map.items() if v == pollutant_code][0]
        
        try:
            annual_mean = annual_means.loc[(station_id_orig, pollutant_name)]
        except KeyError:
            annual_mean = 25.0 if pollutant_code != 'co' else 1.5
        
        trend = trends.get((station_id_orig, pollutant_code), 0.0)
        
        station_info = stations_df[stations_df['station_id'] == station_id].iloc[0]
        start_year = max(START_YEAR, station_info['years_min'])
        
        if station_info['years_max'] >= BASE_DATA_END:
            end_year = END_YEAR
        else:
            end_year = min(END_YEAR, station_info['years_max'])
        
        start_date = f"{start_year}-01-01"
        
        # Usar fecha final parametrizada
        if end_year == END_YEAR:
            end_date = END_DATE_STR
        else:
            end_date = f"{end_year}-12-31"
        
        is_extended = end_year > BASE_DATA_END
        seed = station_id * 1000 + hash(pollutant_code) % 1000
        
        dates, values = generate_daily_series(
            annual_mean, start_date, end_date, 
            pollutant_code, seed, 
            historical_trend=trend,
            is_extended=is_extended
        )
        
        for date, value in zip(dates, values):
            year = date.year
            source = 'synthetic' if year <= BASE_DATA_END else 'synthetic_projected'
            
            measurements_by_year[year].append({
                'station_id': int(station_id),
                'pollutant_id': pollutant_code,
                'ts_utc': date.strftime('%Y-%m-%dT%H:%M:%SZ'),
                'value': float(value),
                'unit': unit,
                'source': source
            })
    
    total_records = sum(len(v) for v in measurements_by_year.values())
    base_records = sum(len(v) for year, v in measurements_by_year.items() if year <= BASE_DATA_END)
    proj_records = total_records - base_records
    
    log(f"✅ Series generadas:")
    log(f"   Total: {total_records:,}")
    log(f"   Base ({START_YEAR}-{BASE_DATA_END}): {base_records:,}")
    log(f"   Proyectadas ({BASE_DATA_END+1}-{END_DATE_STR}): {proj_records:,}")
    
    return measurements_by_year

# ============================================================================
# GUARDAR PARQUETS
# ============================================================================

def save_all_data(zones_df, stations_df, pollutants_df, caps_df, measurements_by_year):
    """Guarda todos los datos"""
    log("=" * 70)
    log("FASE 3: GUARDANDO PARQUETS")
    log("=" * 70)
    
    processed_dir = f"{S3_OUTPUT}"
    success_count = 0
    
    # Guardar catálogos
    log("Guardando catálogos...")
    
    if save_single_parquet(zones_df, f"{processed_dir}/zones.parquet", "zones"):
        success_count += 1
    
    if save_single_parquet(stations_df, f"{processed_dir}/stations.parquet", "stations"):
        success_count += 1
    
    if save_single_parquet(pollutants_df, f"{processed_dir}/pollutants.parquet", "pollutants"):
        success_count += 1
    
    if save_single_parquet(caps_df, f"{processed_dir}/station_capabilities.parquet", "capabilities"):
        success_count += 1
    
    log(f"✅ {success_count}/4 catálogos guardados")
    
    # Guardar measurements
    measurements_dir = f"{processed_dir}/measurements_raw"
    log(f"Guardando measurements en: {measurements_dir}/")
    
    total_measurements = 0
    measurements_success = 0
    
    for year in range(START_YEAR, END_YEAR + 1):
        if measurements_by_year[year]:
            year_df = pd.DataFrame(measurements_by_year[year])
            year_path = f"{measurements_dir}/measurements_raw_{year}.parquet"
            
            if save_single_parquet(year_df, year_path, f"measurements {year}"):
                total_measurements += len(year_df)
                measurements_success += 1
                success_count += 1
    
    log(f"✅ {measurements_success}/{END_YEAR-START_YEAR+1} años guardados")
    log(f"✅ Total mediciones: {total_measurements:,}")
    
    log(f"\n📁 Estructura en S3:")
    log(f"   {S3_OUTPUT}/")
    #log(f"   └── processed/")
    log(f"       ├── zones.parquet")
    log(f"       ├── stations.parquet")
    log(f"       ├── pollutants.parquet")
    log(f"       ├── station_capabilities.parquet")
    log(f"       └── measurements_raw/")
    log(f"           ├── measurements_raw_2018.parquet")
    log(f"           ├── ... (años intermedios)")
    log(f"           └── measurements_raw_{END_YEAR}.parquet")
    
    return {
        'success_count': success_count,
        'total_measurements': total_measurements,
        'catalogs': 4,
        'measurements_files': measurements_success
    }

# ============================================================================
# VALIDACIÓN
# ============================================================================

def validate_output():
    """Valida archivos generados"""
    log("=" * 70)
    log("FASE 4: VALIDACIÓN")
    log("=" * 70)
    
    try:
        s3 = boto3.client('s3')
        bucket = S3_OUTPUT.replace('s3://', '').split('/')[0]
        prefix = '/'.join(S3_OUTPUT.replace('s3://', '').split('/')[1:])
        
        log(f"Verificando bucket: {bucket}")
        log(f"Prefijo: {prefix}")
        
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        
        if 'Contents' not in response:
            log("⚠️  No se encontraron archivos", "WARNING")
            return False
        
        files = [obj['Key'] for obj in response['Contents']]
        parquet_files = [f for f in files if f.endswith('.parquet')]
        
        log(f"Archivos parquet encontrados: {len(parquet_files)}")
        
        # Verificar catálogos
        catalogs = ['zones.parquet', 'stations.parquet', 'pollutants.parquet', 'station_capabilities.parquet']
        found_catalogs = 0
        missing_catalogs = []
        
        for catalog in catalogs:
            found = any(catalog in f for f in parquet_files)
            if found:
                log(f"   ✅ {catalog}")
                found_catalogs += 1
            else:
                log(f"   ❌ {catalog} FALTANTE", "WARNING")
                missing_catalogs.append(catalog)
        
        # Verificar measurements
        measurements_files = [f for f in parquet_files if 'measurements_raw_' in f and f.endswith('.parquet')]
        
        # Calcular años esperados (solo años con datos)
        expected_years = END_YEAR - START_YEAR + 1
        
        log(f"\nMeasurements:")
        log(f"   Esperados: hasta {expected_years} años")
        log(f"   Encontrados: {len(measurements_files)} archivos")
        
        # Verificar que haya al menos archivos de measurements
        if len(measurements_files) == 0:
            log(f"   ❌ No se encontraron archivos de measurements", "ERROR")
            return False
        
        # Si hay archivos pero menos de los esperados, puede ser normal
        # (algunas estaciones pueden no tener datos en todos los años)
        if len(measurements_files) < expected_years:
            log(f"   ℹ️  Se encontraron {len(measurements_files)}/{expected_years} años")
            log(f"   ℹ️  Esto es normal si algunas estaciones no tienen datos en todos los años")
        
        # Éxito si tenemos los 4 catálogos y al menos 1 archivo de measurements
        success = found_catalogs == 4 and len(measurements_files) > 0
        
        if success:
            log("\n✅ Validación exitosa:")
            log(f"   • 4/4 catálogos presentes")
            log(f"   • {len(measurements_files)} archivos de measurements generados")
        else:
            if missing_catalogs:
                log(f"\n❌ Validación fallida: Faltan catálogos: {missing_catalogs}", "ERROR")
            if len(measurements_files) == 0:
                log(f"\n❌ Validación fallida: No hay archivos de measurements", "ERROR")
        
        return success
        
    except Exception as e:
        log(f"❌ Error en validación: {e}", "ERROR")
        traceback.print_exc()
        return False

# ============================================================================
# MAIN
# ============================================================================

def main():
    """Ejecuta el flujo completo"""
    start_time = datetime.now()
    
    try:
        # Cargar datos
        df_base = load_base_dataset()
        
        # Generar catálogos
        log("\n" + "=" * 70)
        log("GENERANDO CATÁLOGOS")
        log("=" * 70)
        
        zones_df = generate_zones(df_base)
        stations_df = generate_stations(df_base, zones_df)
        pollutants_df = generate_pollutants()
        caps_df = generate_station_capabilities(df_base, stations_df)
        
        # Calcular tendencias
        trends = calculate_historical_trends(df_base)
        
        # Generar measurements
        measurements_by_year = generate_measurements(df_base, stations_df, caps_df, trends)
        
        # Guardar todo
        stats = save_all_data(zones_df, stations_df, pollutants_df, caps_df, measurements_by_year)
        
        # Validar
        validation_ok = validate_output()
        
        # Resumen final
        end_time = datetime.now()
        elapsed = (end_time - start_time).total_seconds()
        
        log("\n" + "=" * 70)
        if validation_ok:
            log("✅ JOB COMPLETADO EXITOSAMENTE")
        else:
            log("⚠️  JOB COMPLETADO CON ADVERTENCIAS")
        log("=" * 70)
        log(f"\n📊 Resumen:")
        log(f"   Zonas: {len(zones_df)}")
        log(f"   Estaciones: {len(stations_df)}")
        log(f"   Contaminantes: {len(pollutants_df)}")
        log(f"   Capacidades: {len(caps_df)}")
        log(f"   Mediciones: {stats['total_measurements']:,}")
        log(f"   Archivos guardados: {stats['success_count']}")
        log(f"\n📅 Periodo de datos:")
        log(f"   Datos base: {START_YEAR}-{BASE_DATA_END}")
        log(f"   Proyección: {BASE_DATA_END+1} hasta {END_DATE_STR}")
        log(f"\n⏱️  Tiempo total: {elapsed/60:.1f} minutos")
        log(f"📅 Finalizado: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        log("=" * 70)
        
        if validation_ok:
            log("✅ ÉXITO: Todos los archivos generados correctamente")
            return 0
        else:
            log("⚠️  ADVERTENCIA: Revisa los logs de validación arriba", "WARNING")
            return 0  # Retornar 0 de todas formas porque los datos se generaron
        
    except Exception as e:
        log("\n" + "=" * 70, "ERROR")
        log("JOB FALLÓ", "ERROR")
        log("=" * 70, "ERROR")
        log(f"Error: {type(e).__name__}: {e}", "ERROR")
        
        end_time = datetime.now()
        elapsed = (end_time - start_time).total_seconds()
        log(f"\n⏱️  Tiempo antes del error: {elapsed/60:.1f} min", "ERROR")
        
        log("\nStack trace completo:", "ERROR")
        traceback.print_exc()
        log("=" * 70, "ERROR")
        
        raise  # Re-lanzar la excepción para que Glue la capture
    
    finally:
        try:
            job.commit()
            log("✅ Glue job commit exitoso")
        except Exception as e:
            log(f"⚠️  Error en job.commit(): {e}", "WARNING")

# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == '__main__':
    try:
        exit_code = main()
        log(f"Job finalizado con código: {exit_code}")
        # NO usar sys.exit() - dejar que el script termine naturalmente
    except Exception as e:
        log(f"Error fatal: {e}", "ERROR")
        traceback.print_exc()
        raise  # Re-lanzar para que Glue lo capture como error