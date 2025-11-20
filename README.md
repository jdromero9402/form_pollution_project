# Form Pollution Project

Sistema completo para la gestión y análisis de datos de calidad del aire, implementado con arquitectura de microservicios en AWS. El proyecto incluye backend API, frontend web, y pipelines de ETL para procesamiento de datos en múltiples sistemas de almacenamiento.

## Estructura del Proyecto

### 1. API (Backend)

**Ubicación:** `api/`

El backend está implementado con **FastAPI** y proporciona una API RESTful para gestionar datos de calidad del aire almacenados en AWS RDS PostgreSQL.

**Características:**
- API REST con FastAPI
- Conexión a base de datos PostgreSQL en AWS RDS
- Endpoints para gestión de:
  - **Zonas**: Información geográfica de zonas de monitoreo
  - **Estaciones**: Estaciones de monitoreo de calidad del aire
  - **Contaminantes**: Tipos de contaminantes (PM2.5, PM10, O3, NO2, SO2, CO)
  - **Capacidades**: Capacidades de medición de cada estación
  - **Mediciones**: Datos de mediciones de calidad del aire
- Documentación automática con Swagger/OpenAPI
- CORS configurado para comunicación con frontend
- Health check endpoints

**Tecnologías:**
- Python 3.x
- FastAPI
- SQLAlchemy (ORM)
- PostgreSQL (AWS RDS)
- Pydantic (validación de datos)

---

### 2. Form Pollution App (Frontend)

**Ubicación:** `form-pollution-app/`

Aplicación web frontend desarrollada en **React** que proporciona una interfaz de usuario para gestionar y visualizar datos de contaminación del aire.

**Características:**
- Interfaz de usuario moderna y responsive
- Formularios para gestión de:
  - **Mediciones**: Registro de mediciones de calidad del aire
  - **Zonas**: Administración de zonas geográficas
  - **Estaciones**: Gestión de estaciones de monitoreo
  - **Contaminantes**: Catálogo de contaminantes
  - **Capacidades**: Configuración de capacidades de estaciones
- Navegación entre diferentes vistas
- Integración con API backend mediante HTTP requests
- Componentes reutilizables y modulares

**Tecnologías:**
- React
- JavaScript/JSX
- CSS3
- HTML5

---

### 3. AWS Glue Jobs (ETL Pipelines)

**Ubicación:** `glue_jobs/`

Pipeline de ETL implementado con **AWS Glue** para procesar, transformar y cargar datos de calidad del aire entre diferentes sistemas de almacenamiento en AWS.

#### 3.1. Job para Cargar CSV Raw a S3 como Parquet

**Archivo:** `CSVRawToParquets_S3.py`

**Descripción:**
Job que procesa archivos CSV raw de datos de calidad del aire y los convierte a formato Parquet optimizado en S3. Genera datasets sintéticos con proyecciones futuras basadas en tendencias históricas.

**Funcionalidades:**
- Lee archivos CSV desde S3
- Genera datasets sintéticos con datos históricos y proyecciones
- Crea catálogos estructurados:
  - `zones.parquet`: Información de zonas geográficas
  - `stations.parquet`: Datos de estaciones de monitoreo
  - `pollutants.parquet`: Catálogo de contaminantes
  - `station_capabilities.parquet`: Capacidades de medición
- Genera mediciones diarias sintéticas por año en `measurements_raw/`
- Aplica tendencias históricas y estacionalidad
- Validación de datos y estructura de salida

**Parámetros principales:**
- `S3_INPUT`: Ruta del archivo CSV de entrada
- `S3_OUTPUT`: Ruta de salida en S3
- `START_YEAR`, `END_YEAR`: Rango de años a procesar
- `BASE_DATA_END`: Año final de datos históricos
- `END_MONTH`, `END_DAY`: Fecha final de proyección

---

#### 3.2. Jobs para Cargar a RDS PostgreSQL Relacional

Estos jobs cargan los datos procesados desde archivos Parquet en S3 hacia AWS RDS PostgreSQL, utilizando autenticación IAM.

##### 3.2.1. `ToRDS_Zones_1.py`
- **Función:** Carga datos de zonas geográficas
- Lee `zones.parquet` desde S3
- Inserta/actualiza registros en tabla `zones` con UPSERT
- Manejo de errores y reintentos automáticos

##### 3.2.2. `ToRDS_Stations_2.py`
- **Función:** Carga datos de estaciones de monitoreo
- Lee `stations.parquet` desde S3
- Inserta/actualiza registros en tabla `stations`
- Valida relaciones con zonas

##### 3.2.3. `ToRDS_Pollutants_3.py`
- **Función:** Carga catálogo de contaminantes
- Lee `pollutants.parquet` desde S3
- Inserta/actualiza registros en tabla `pollutants`
- Incluye límites WHO (Organización Mundial de la Salud)

##### 3.2.4. `ToRDS_Station_Capabilities_4.py`
- **Función:** Carga capacidades de medición de estaciones
- Lee `station_capabilities.parquet` desde S3
- Inserta/actualiza registros en tabla `station_capabilities`
- Relaciona estaciones con contaminantes que pueden medir

##### 3.2.5. `ToRDS_Measurements_raw_5.py`
- **Función:** Carga mediciones de calidad del aire (multi-año)
- Lee archivos `measurements_raw_{year}.parquet` desde S3
- Soporta procesamiento de año único, rango de años, o lista específica
- Inserta/actualiza registros en tabla `measurements_raw`
- Limpieza de datos (nulls, negativos, duplicados)
- Checkpoints periódicos para grandes volúmenes
- Estadísticas detalladas y tracking de progreso

**Características comunes de los jobs RDS:**
- Autenticación IAM con tokens temporales
- Procesamiento en lotes (batch processing)
- Manejo de errores con reintentos
- Validación de esquema y tablas
- UPSERT para evitar duplicados

---

#### 3.3. Jobs para Cargar a InfluxDB y de InfluxDB a Redshift

##### 3.3.1. `loadInfluxDb.py`
- **Función:** Carga datos desde RDS PostgreSQL hacia InfluxDB
- Lee mediciones no procesadas desde `measurements_raw` en RDS
- Convierte datos relacionales a formato de series temporales
- Escribe puntos de datos a InfluxDB con tags y campos
- Actualiza campo `processed_at` en RDS después de cargar
- Procesamiento en lotes con reintentos
- Filtrado por rango de fechas

**Características:**
- Conversión de modelo relacional a time-series
- Tags: `station_id`, `pollutant_id`, `unit`, `source`, `year`
- Campo: `value` (valor de medición)
- Precisión de timestamps configurable

##### 3.3.2. `InfluxTos3.py`
- **Función:** Extrae análisis de InfluxDB y carga a Redshift
- Ejecuta queries Flux en InfluxDB para análisis de datos
- Genera 4 tipos de análisis:
  1. **Estadísticas agregadas**: Min, Max, Promedio por año, contaminante y estación
  2. **Derivada diaria**: Tasa de cambio diaria de valores
  3. **Derivada mensual**: Tasa de cambio mensual de valores
  4. **Picos y valles**: Valores máximos y mínimos por año, contaminante y estación
- Carga resultados directamente a Redshift usando Glue connector
- Tablas destino en Redshift:
  - `air_quality_stats`
  - `air_quality_daily_derivative`
  - `air_quality_monthly_derivative`
  - `air_quality_peaks_valleys`

---

#### 3.4. Job para Cargar de RDS a Redshift

**Archivo:** `FromRDS_To_Redshift.py`

**Descripción:**
Job que migra datos enriquecidos desde RDS PostgreSQL hacia Amazon Redshift para análisis analítico, implementando UPSERT para evitar duplicados.

**Funcionalidades:**
- Extrae datos recientes de `measurements_raw` desde RDS
- Enriquece datos con JOINs a tablas relacionadas:
  - `pollutants`: Información de contaminantes y límites WHO
  - `stations`: Datos de estaciones de monitoreo
  - `zones`: Información geográfica de zonas
- Calcula estado de cumplimiento con límites WHO
- Carga a tabla staging en Redshift
- Ejecuta UPSERT (DELETE + INSERT) para evitar duplicados
- Tabla destino: `air_quality_sabana` (tabla de hechos enriquecida)

**Características:**
- Filtrado por fecha (`SINCE_DAYS`)
- Limpieza de valores NaN en SQL
- Conversión de tipos de datos
- Optimización para grandes volúmenes

---

## Flujo de Datos

```
CSV Raw → [CSVRawToParquets_S3] → Parquet en S3
                                      ↓
                    ┌─────────────────┴─────────────────┐
                    ↓                                     ↓
        [ToRDS_* Jobs]                            [loadInfluxDb]
                    ↓                                     ↓
            RDS PostgreSQL                          InfluxDB
                    ↓                                     ↓
        [FromRDS_To_Redshift]                    [InfluxTos3]
                    ↓                                     ↓
              Redshift                              Redshift
                    └─────────────────┬─────────────────┘
                                      ↓
                            Análisis y Reportes
```

## Tecnologías Utilizadas

- **Backend:** FastAPI, Python, SQLAlchemy, PostgreSQL
- **Frontend:** React, JavaScript, CSS
- **ETL:** AWS Glue, PySpark, Pandas
- **Almacenamiento:** 
  - AWS S3 (Parquet)
  - AWS RDS PostgreSQL (Datos relacionales)
  - InfluxDB (Series temporales)
  - Amazon Redshift (Data Warehouse)
- **Autenticación:** AWS IAM, Secrets Manager

## Notas

- Todos los jobs de Glue utilizan autenticación IAM para conexiones seguras
- Los jobs implementan manejo robusto de errores y reintentos
- Los datos se validan y limpian en cada etapa del pipeline
- El sistema soporta procesamiento incremental y por lotes
