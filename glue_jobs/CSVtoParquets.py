import sys
import boto3
import psycopg2
from psycopg2 import extras
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import hashlib  
from io import BytesIO, StringIO
from awsglue.utils import getResolvedOptions
import traceback

# ============================================================================
# PARÁMETROS DEL JOB
# ============================================================================
def get_job_params():
    """Obtiene parámetros del job de Glue"""
    params = {}
    
    # Lista de parámetros esperados
    param_names = [
        'S3_INPUT',           # s3://bucket/path/to/base_data.csv
        'S3_OUTPUT',          # s3://bucket/output_path/
        'S3_BUCKET',          
        'S3_PREFIX',
        'START_YEAR',
        'END_YEAR',
        'BASE_DATA_END'
    ]
    
    # Parsear argumentos de Glue (formato --key=value)
    for arg in sys.argv[1:]:
        if arg.startswith('--'):
            key_value = arg[2:].split('=', 1)
            if len(key_value) == 2:
                key, value = key_value
                if key in param_names:
                    params[key] = value
    
    return params

# Obtener parámetros
args = get_job_params()

def get_arg(name, default=None):
    # intenta getResolvedOptions
    try:
        return getResolvedOptions(sys.argv, [name])[name]
    except Exception:
        # parsea manualmente --NAME=valor o "--NAME valor"
        flag = f'--{name}'
        for i, a in enumerate(sys.argv):
            if a.startswith(flag + '='):
                return a.split('=', 1)[1]
            if a == flag and i + 1 < len(sys.argv):
                return sys.argv[i+1]
        # env var o default
        return os.getenv(name, default)

# DEBUG: imprime argv para verificar qué llegó
print("sys.argv:", sys.argv)

# Configuración
START_YEAR = int(get_arg('START_YEAR'))
END_YEAR = int(get_arg('END_YEAR'))
BASE_DATA_END = int(get_arg('BASE_DATA_END'))

# Configuración de columnas del CSV base
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

# Mapeo de contaminantes
POLLUTANTS_INFO = {
    'PM2.5': {'code': 'pm25', 'unit': 'µg/m³', 'daily': 15, 'annual': 5},
    'PM10': {'code': 'pm10', 'unit': 'µg/m³', 'daily': 45, 'annual': 15},
    'O3': {'code': 'o3', 'unit': 'µg/m³', 'daily': 100, 'annual': None},
    'NO2': {'code': 'no2', 'unit': 'µg/m³', 'daily': 25, 'annual': 10},
    'SO2': {'code': 'so2', 'unit': 'µg/m³', 'daily': 40, 'annual': None},
    'CO': {'code': 'co', 'unit': 'mg/m³', 'daily': 4, 'annual': None},
}

print("=" * 70)
print("🚀 AWS GLUE JOB (PythonShell) - TRANSFORMACIÓN Y GENERACIÓN DE PARQUETS EN S3")
print("=" * 70)
#print(f"📥 Input CSV: {args['S3_INPUT']}")
#print(f"📤 Output Parquet: {args['S3_OUTPUT']}")

# ============================================================================
# CLIENTE S3
# ============================================================================
print("ClienteS3")
s3_client = boto3.client('s3')

def parse_s3_path(s3_path):
    """Divide s3://bucket/key en bucket y key"""
    path = s3_path.replace('s3://', '')
    parts = path.split('/', 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ''
    return bucket, key

def read_csv_from_s3(s3_path):
    """Lee CSV desde S3 usando pandas"""
    bucket, key = parse_s3_path(s3_path)
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(obj['Body'], thousands=',', decimal='.')

def write_parquet_to_s3(df, s3_path):
    """Escribe DataFrame como Parquet en S3"""
    bucket, key = parse_s3_path(s3_path)
    buffer = BytesIO()
    df.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
    buffer.seek(0)
    s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())

def list_s3_files(s3_path, prefix):
    """Lista archivos en S3 con un prefijo"""
    bucket, base_key = parse_s3_path(s3_path)
    full_prefix = f"{base_key}/{prefix}" if base_key else prefix
    
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=full_prefix)
    
    files = []
    if 'Contents' in response:
        for obj in response['Contents']:
            files.append(f"s3://{bucket}/{obj['Key']}")
    
    return files

# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================
print("FuncionesAuxiliares")
def normalize_name(name):
    """Normaliza nombres (quita acentos, capitaliza)"""
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
    """Genera un ID estable a partir de un texto"""
    hash_obj = hashlib.md5(text.encode('utf-8'))
    return f"{prefix}{hash_obj.hexdigest()[:length]}"

def generate_daily_series(annual_mean, start_date, end_date, pollutant_code, seed=None, 
                          historical_trend=0.0, is_extended=False):
    """Genera serie diaria sintética con tendencias y variación"""
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

# ============================================================================
# FASE 1: CARGA Y GENERACIÓN DE DATASETS
# ============================================================================
print("Carga y Generación de Datasets")
def load_base_dataset():
    """Carga dataset base desde S3"""
    print("\n📂 FASE 1: CARGANDO DATASET BASE")
    print("-" * 70)
    
    s3_input = get_arg('S3_INPUT')
    if not s3_input:
        # Fallback: toma el CSV más reciente del prefijo
        bucket = get_arg('S3_BUCKET', 'airquality-datasets-s3')
        prefix = get_arg('S3_PREFIX', 'air/measurements_raw/')
        s3 = boto3.client('s3')
        newest = None
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                if obj['Key'].lower().endswith('.csv'):
                    if newest is None or obj['LastModified'] > newest['LastModified']:
                        newest = obj
        if not newest:
            raise RuntimeError(f"No se encontró CSV en s3://{bucket}/{prefix} y no llegó --S3_INPUT")
        s3_input = f"s3://{bucket}/{newest['Key']}"
    
    print("Usando S3_INPUT:", s3_input)
    
    #df = read_csv_from_s3(args['S3_INPUT'])
    df = read_csv_from_s3(s3_input)
    
    print("Pasó lectura de S3")
    # Normalizar columnas
    df.columns = df.columns.str.strip()
    
    # Convertir columnas numéricas
    numeric_cols = [COL_CONFIG['col_year'], COL_CONFIG['col_value'], 
                   COL_CONFIG['col_lat'], COL_CONFIG['col_lon']]
    
    for col in numeric_cols:
        if col in df.columns and df[col].dtype == 'object':
            df[col] = df[col].astype(str).str.replace(',', '').astype(float)
    
    # Filtrar solo contaminantes de aire
    pollutant_codes = list(POLLUTANTS_INFO.keys())
    df = df[df[COL_CONFIG['col_pollutant']].isin(pollutant_codes)]
    
    print(f"✅ Dataset cargado: {len(df):,} registros")
    print(f"   Años: {df[COL_CONFIG['col_year']].min():.0f} - {df[COL_CONFIG['col_year']].max():.0f}")
    print(f"   Variables: {df[COL_CONFIG['col_pollutant']].unique().tolist()}")
    print(f"   Estaciones: {df[COL_CONFIG['col_station']].nunique()}")
    
    return df

def generate_zones(df_base):
    """Genera tabla zones"""
    print("\n🌍 Generando zones...")
    
    zones_df = df_base[[COL_CONFIG['col_dept'], COL_CONFIG['col_muni']]].drop_duplicates()
    
    zones_list = []
    for idx, row in zones_df.iterrows():
        dept = normalize_name(row[COL_CONFIG['col_dept']])
        muni = normalize_name(row[COL_CONFIG['col_muni']])
        city_code = generate_stable_id(f"{dept}-{muni}", length=6).upper()
        
        zones_list.append({
            'zone_id': idx + 1,
            'city_code': city_code,
            'city_name': muni,
            'zone_code': f"Z{idx+1:02d}",
            'zone_name': f"Zona {muni}",
            'population': int(np.random.randint(50000, 1500000)),
            'centroid_lat': round(float(np.random.uniform(1.0, 11.0)), 6),
            'centroid_lon': round(float(np.random.uniform(-79.0, -66.0)), 6)
        })
    
    zones_df = pd.DataFrame(zones_list)
    print(f"   ✅ {len(zones_df)} zonas generadas")
    return zones_df

def generate_stations(df_base, zones_df):
    """Genera tabla stations"""
    print("📍 Generando stations...")
    
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
            print(f"   ⚠️  Saltando estación {station_id_orig}: {e}")
    
    stations_df = pd.DataFrame(stations_list).drop_duplicates(subset=['station_id'], keep='first')
    print(f"   ✅ {len(stations_df)} estaciones generadas")
    return stations_df

def generate_pollutants():
    """Genera tabla pollutants"""
    print("🌫️  Generando pollutants...")
    
    pollutants_list = []
    for name, info in POLLUTANTS_INFO.items():
        pollutants_list.append({
            'pollutant': info['code'],
            'name': name,
            'default_unit': info['unit'],
            'who_daily_limit': info['daily'],
            'who_annual_limit': info['annual']
        })
    
    pollutants_df = pd.DataFrame(pollutants_list)
    print(f"   ✅ {len(pollutants_df)} contaminantes generados")
    return pollutants_df

def generate_station_capabilities(df_base, stations_df):
    """Genera tabla station_capabilities"""
    print("⚙️  Generando station_capabilities...")
    
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
                'pollutant': pollutant_code,
                'unit': unit_map.get(pollutant_name, 'µg/m³')
            })
    
    caps_df = pd.DataFrame(caps_list).drop_duplicates()
    print(f"   ✅ {len(caps_df)} capacidades generadas")
    return caps_df

def calculate_historical_trends(df_base):
    """Calcula tendencias históricas"""
    print("📈 Calculando tendencias...")
    
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
    
    print(f"   ✅ {len(trends)} tendencias calculadas")
    return trends

def generate_measurements(df_base, stations_df, station_caps, trends):
    """Genera measurements_raw particionado por año"""
    print("📊 Generando measurements_raw...")
    
    pollutant_map = {name: info['code'] for name, info in POLLUTANTS_INFO.items()}
    
    df_with_id = df_base.copy()
    df_with_id['station_id_orig'] = df_with_id[COL_CONFIG['col_station_id']].astype(str).str.replace(',', '')
    annual_means = df_with_id.groupby(['station_id_orig', COL_CONFIG['col_pollutant']])[COL_CONFIG['col_value']].mean()
    
    measurements_by_year = {year: [] for year in range(START_YEAR, END_YEAR + 1)}
    total_combinations = len(station_caps)
    current_date = datetime.now()
    
    for idx, cap in station_caps.iterrows():
        if idx % 10 == 0:
            print(f"   Procesando: {idx+1}/{total_combinations}", end='\r')
        
        station_id = cap['station_id']
        pollutant_code = cap['pollutant']
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
        end_date = current_date.strftime('%Y-%m-%d') if end_year == END_YEAR else f"{end_year}-12-31"
        
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
                'pollutant': pollutant_code,
                'ts_utc': date.strftime('%Y-%m-%dT%H:%M:%SZ'),
                'value': float(value),
                'unit': unit,
                'source': source
            })
    
    print(f"\n   ✅ Series temporales generadas")
    return measurements_by_year

# ============================================================================
# FASE 2: GUARDAR PARQUETS EN S3
# ============================================================================
print("Guardar parquets en S3")
def save_all_parquets(zones_df, stations_df, pollutants_df, caps_df, measurements_by_year):
    """Guarda todos los DataFrames como Parquet en S3"""
    print("\n💾 FASE 2: GUARDANDO PARQUETS EN S3")
    print("-" * 70)
    
    #s3_output = args['S3_OUTPUT'].rstrip('/')
    s3_output = get_arg('S3_OUTPUT').rstrip('/')
    
    try:
        # Guardar catálogos
        print("📦 Guardando catálogos...")
        write_parquet_to_s3(zones_df, f"{s3_output}/zones.parquet")
        write_parquet_to_s3(stations_df, f"{s3_output}/stations.parquet")
        write_parquet_to_s3(pollutants_df, f"{s3_output}/pollutants.parquet")
        write_parquet_to_s3(caps_df, f"{s3_output}/station_capabilities.parquet")
        print("   ✅ Catálogos guardados")
        
        # Guardar mediciones por año
        print("📊 Guardando mediciones por año...")
        total_records = 0
        for year in range(START_YEAR, END_YEAR + 1):
            if measurements_by_year[year]:
                year_df = pd.DataFrame(measurements_by_year[year])
                write_parquet_to_s3(year_df, f"{s3_output}/measurements_raw_{year}.parquet")
                total_records += len(year_df)
                print(f"   ✅ {year}: {len(year_df):,} registros")
        
        print(f"\n   Total: {total_records:,} mediciones")
        return total_records
    except Exception as e:
        traceback.print_exc()
        raise

# ============================================================================
# MAIN - FASE 1
# ============================================================================
print("Main")
def main():
    """Ejecuta el flujo: Generación → S3"""
    
    try:
        # ====================================================================
        # FASE 1: GENERAR DATASETS SINTÉTICOS
        # ====================================================================
        
        # 1.1 Cargar CSV base
        df_base = load_base_dataset()
        
        # 1.2 Generar catálogos
        print("\n📋 Generando catálogos...")
        zones_df = generate_zones(df_base)
        stations_df = generate_stations(df_base, zones_df)
        pollutants_df = generate_pollutants()
        caps_df = generate_station_capabilities(df_base, stations_df)
        
        # 1.3 Calcular tendencias y generar mediciones
        trends = calculate_historical_trends(df_base)
        measurements_by_year = generate_measurements(df_base, stations_df, caps_df, trends)
        
        # ====================================================================
        # FASE 2: GUARDAR PARQUETS EN S3
        # ====================================================================
        
        total_measurements = save_all_parquets(
            zones_df, stations_df, pollutants_df, caps_df, measurements_by_year
        )
        
        # ====================================================================
        # RESUMEN FINAL
        # ====================================================================
        
        print("\n" + "=" * 70)
        print("✅ JOB COMPLETADO EXITOSAMENTE")
        print("=" * 70)
        print(f"\n📊 Resumen:")
        print(f"   • Zonas: {len(zones_df)}")
        print(f"   • Estaciones: {len(stations_df)}")
        print(f"   • Contaminantes: {len(pollutants_df)}")
        print(f"   • Capacidades: {len(caps_df)}")
        print(f"   • Mediciones: {total_measurements:,}")
        #print(f"\n📁 Output S3: {s3_output}")
        
        if END_YEAR > BASE_DATA_END:
            proj_years = END_YEAR - BASE_DATA_END
            print(f"\n🔮 Proyección: {proj_years} años sintéticos ({BASE_DATA_END+1}-{END_YEAR})")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == '__main__':
    main()