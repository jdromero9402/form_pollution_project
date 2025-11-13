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

```bash
cd form-pollution-app
npm install
npm start
# Open http://localhost:3000
```

Notes:
- When started with `npm start` inside the container, `process.env.REACT_APP_API_URL` from docker-compose will be available to the dev server.
- Create React App injects REACT_APP_* variables at build time when running `npm run build`.

---

## Build for production
Create a production build:

```bash
npm run build
# files produced in ./build
```

Serve the build with a static server (example):

```bash
npx serve -s build -l 3000
```

If you build the app in CI or Docker, make sure the required env vars are provided at build time or use a runtime injection strategy.

---

## Environment variables / Variables de entorno
- REACT_APP_API_URL — Base URL of the backend API (e.g. http://localhost:8005). Access it in code as:

```js
const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:8005';
```

Important:
- CRA reads REACT_APP_* at build time. Changing docker-compose env after a static build will not change the bundled value. For dynamic runtime config, use a small runtime config file or entrypoint script that substitutes values.

---

## Docker / docker-compose
A simple Dockerfile exists for development (runs `npm start`). Example docker-compose service is defined in the repository root (service name: `ui`).

Build and run:
```bash
# from repo root
docker-compose build ui
docker-compose up ui
# or run both services:
docker-compose up --build
```

---

## Usage from code
Example helper to call API:

```js
// src/utils/api.js
const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:8005';

export async function postZone(payload) {
  const res = await fetch(`${API_URL}/api/v1/zones/`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  return res;
}
```

Check response.status === 201 for Created.

---

## Development notes / Notas de desarrollo
- Partial HTML fragments in `form-ui` use htmx for a separate minimal UI; the React app is independent.
- Keep global header in `index.html` (or remove header from partials injected via htmx) to avoid duplicate headings.
- If you switch to Vite, use `VITE_` prefix and `import.meta.env`.

---

## Contributing
- Create a branch, add changes, open a pull request.
- Keep UI translations in Spanish where appropriate.

---