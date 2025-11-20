# Air Quality API

**Resumen**
- **Descripción**: API REST construida con `FastAPI` para gestionar datos de calidad del aire (zonas, estaciones, contaminantes, capacidades y mediciones).
- **Ubicación**: Código en `api/`.
- **Entradas principales**: `/api/v1/zones`, `/api/v1/stations`, `/api/v1/pollutants`, `/api/v1/capabilities`, `/api/v1/measurements`.

**Stack Tecnológico**
- **Framework**: `FastAPI`
- **Servidor ASGI**: `uvicorn`
- **Base de datos**: PostgreSQL (asíncrono con `asyncpg` y `SQLAlchemy` 2.x)
- **Validación**: `pydantic` / `pydantic-settings`

**Requisitos**
- Python 3.11+ recomendado
- PostgreSQL accesible (puede usarse con la configuración de `docker-compose.yml` o una instancia externa)
- Dependencias listadas en `requirements.txt`

**Instalación local (virtualenv)**
- Crear y activar un entorno virtual:

```
python -m venv .venv
source .venv/bin/activate
```

- Instalar dependencias:

```
pip install -r requirements.txt
```


- Ejecutar localmente con `uvicorn` (desde la carpeta `api`):

```
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Acceder a la documentación interactiva en `http://localhost:8000/docs` (Swagger) o `http://localhost:8000/redoc`.

**Arranque con Docker Compose**
- Desde la raíz del proyecto (donde está `docker-compose.yml`):

```
docker compose up --build
```

- El servicio de la API quedará expuesto en el puerto `8005` del host (mappeado a `8000` del contenedor) según `docker-compose.yml`.

**Endpoints principales (resumen)**
- **GET** `/` : Información básica de la API y rutas de documentación.
- **GET** `/health` : Health check.
- **/api/v1/measurements**
  - `POST /api/v1/measurements/` : Crear una medición (acepta formulario multipart con los campos definidos en `schemas/measurement.py`).
  - `POST /api/v1/measurements/bulk` : Crear mediciones en bulk (envía JSON array).
  - `GET /api/v1/measurements/` : Obtener mediciones.
- **/api/v1/pollutants**
  - `POST /api/v1/pollutants/` : Crear contaminante.
  - `GET /api/v1/pollutants/` : Listar contaminantes (soporta `skip` y `limit`).
- **/api/v1/stations**, **/api/v1/zones**, **/api/v1/capabilities**
  - Routers con operaciones CRUD básicas (revisar `app/routers/*` para detalles).

**Ejemplos rápidos (curl)**
- Health check:

```
curl http://localhost:8000/health
```

- Crear una medición (ejemplo mínimo, adaptar campos según `schemas/measurement.py`):

```
curl -X POST "http://localhost:8000/api/v1/measurements/" \
  -F "station_id=ST123" \
  -F "pollutant_id=NO2" \
  -F "value=12.5" \
  -F "measured_at=2025-11-19T12:00:00Z"
```

- Crear mediciones en bulk (JSON):

```
curl -X POST "http://localhost:8000/api/v1/measurements/bulk" \
  -H "Content-Type: application/json" \
  -d '[{"station_id":"ST1","pollutant_id":"PM2.5","value":10,"measured_at":"2025-11-19T12:00:00Z"}]'
```

**Estructura relevante del proyecto**
- `app/main.py` : Inicialización de la aplicación y registro de routers.
- `app/config.py` : Configuración via `pydantic-settings` y construcción de `database_url`.
- `app/database.py` : Inicialización de la conexión asíncrona y dependencias (verificar funciones `init_db`, `get_db`, `close_db`).
- `app/routers/` : Routers por recurso (`zones`, `stations`, `pollutants`, `capabilities`, `measurements`).
- `app/models/` y `app/schemas/` : Modelos SQLAlchemy y esquemas Pydantic.

**Notas de desarrollo**
- Los routers usan dependencias asíncronas y `SQLAlchemy` 2.x con `AsyncSession`.
- Para producción, restringir `CORS` y no exponer credenciales en `docker-compose.yml` ni en código.
- Si se usan migraciones, añadir y configurar `alembic` (actualmente no incluido explícitamente).

**Contacto / Soporte**
- Repositorio: trabajar desde este proyecto localmente.
- Si necesitas que documente endpoints concretos con ejemplos de request/responses, indícame qué router quieres que detalle.

---