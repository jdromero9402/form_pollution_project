# Form Pollution App (Frontend)

Small React frontend for the Form Pollution project. Built with Create React App. Provides UI to manage zones, stations, pollutants and measurements and communicates with the Air Quality API.

---

## Prerequisites / Requisitos
- Node.js 16+ (recommended 18)
- npm (or yarn / pnpm)
- Docker & docker-compose (optional, for containerized dev or deployment)

---

## Quick start (development)
Install dependencies and run the dev server (hot reload):

# form-pollution-app

**Resumen**
- **Descripción**: Interfaz web construida con React (Create React App) para gestionar zonas, estaciones, contaminantes, capacidades y mediciones. Se comunica con la API `Air Quality API` ubicada en `/api/v1`.
- **Ruta del código**: `form-pollution-app/`.

**Stack**
- **Framework**: React (Create React App)
- **Node**: Node.js (recomendado 18)
- **Bundler / dev**: `react-scripts`
- **Contenedor**: Docker (opcional)

**Requisitos**
- `Node.js` 16+ (recomendado 18)
- `npm` o equivalente (yarn, pnpm)
- `Docker` y `docker-compose` (opcional)

**Instalación y arranque (desarrollo)**
- Clonar el repositorio y entrar al directorio de la app:

```bash
cd form-pollution-app
```

- Instalar dependencias e iniciar el servidor de desarrollo (hot reload):

```bash
npm install
npm start
# Abrir http://localhost:3000
```

Nota: Create React App carga variables de entorno que empiecen con `REACT_APP_` en tiempo de build. Para desarrollo con `npm start` puedes definir `REACT_APP_API_URL` localmente o usar `docker-compose` para inyectarla en el contenedor.

**Scripts útiles (desde `form-pollution-app`)**
- `npm start`: inicia el servidor de desarrollo
- `npm run build`: genera la carpeta `build` con la versión lista para producción
- `npm test`: ejecuta tests (configurados por CRA)

**Build y despliegue (producción)**

```bash
npm run build
# El build final queda en ./build
npx serve -s build -l 3000
```

Si se va a hacer un build dentro de Docker o CI, recuerda pasar las variables `REACT_APP_*` en tiempo de build o emplear un mecanismo de inyección en tiempo de ejecución.

**Docker (desarrollo)**
- El `Dockerfile` incluido está pensado para desarrollo y expone el puerto `3000`.
- En el `docker-compose.yml` en la raíz del repo existe un servicio `ui` que construye esta carpeta y define `REACT_APP_API_URL`.

Ejemplo básico desde la raíz del repo:

```bash
docker compose build ui
docker compose up ui
# o para levantar ambos servicios (api + ui):
docker compose up --build
```

**Variables de entorno**
- `REACT_APP_API_URL`: URL base de la API (por ejemplo `http://localhost:8005` o `http://mi-backend:8000`).

Ejemplo de uso en código:

```js
const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:8005';
```

Importante: CRA reemplaza `process.env.REACT_APP_*` durante el build. Cambiar la variable en `docker-compose` después de un build estático no actualizará la app ya compilada.

Si necesitas configuración dinámica en tiempo de ejecución, usa un pequeño script de entrada que escriba un archivo de configuración estático al iniciar el contenedor.

**Estructura del proyecto (relevante)**
- `public/`: HTML y assets estáticos (`index.html`)
- `src/`: código fuente React
  - `src/App.js`: aplicación principal y vistas
  - `src/index.js`: punto de entrada
  - `src/components/`: componentes (Home, forms para zonas, estaciones, contaminantes, mediciones)
  - `src/styles.css`, `src/index.css`: estilos

**Conexión con la API**
- Las llamadas al backend deben apuntar a `${API_URL}/api/v1/...`.
- Ejemplo para crear una medición (fetch):

```js
await fetch(`${API_URL}/api/v1/measurements/`, {
  method: 'POST',
  body: formData, // si usas FormData para multipart/form-data
});
```

Para endpoints JSON, usa `Content-Type: application/json` y `JSON.stringify(payload)`.

**Notas de desarrollo y recomendaciones**
- En producción restringe CORS y no expongas credenciales en `docker-compose.yml`.
- Mantén `REACT_APP_API_URL` coherente entre frontend y backend; al usar Docker Compose, pon el hostname del servicio de la API (p. ej. `http://api:8000`) si ambos servicios corren en la misma red de Docker.
- Si prefieres inyectar configuración en tiempo de ejecución, considera soluciones como `envsubst` o servir un archivo `config.json` generado por el entrypoint.

**Contribuciones**
- Crear una rama nueva, hacer cambios y abrir un Pull Request.
- Mantener la UI traducida al español donde aplique.

Si quieres, puedo añadir ejemplos concretos de request/response basados en los `schemas` del backend o generar un pequeño archivo `src/utils/api.js` con helpers para todas las entidades.

---