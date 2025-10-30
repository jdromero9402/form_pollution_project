from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from config import get_settings
from database import init_db, close_db
from routers import zones, stations, pollutants, capabilities, measurements

settings = get_settings()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Maneja el ciclo de vida de la aplicación
    """
    # Startup
    print("🚀 Starting Air Quality API...")
    await init_db()
    print("✓ Database initialized")
    
    yield
    
    # Shutdown
    print("🛑 Shutting down Air Quality API...")
    await close_db()
    print("✓ Database connections closed")

# Crear aplicación FastAPI
app = FastAPI(
    title=settings.API_TITLE,
    version=settings.API_VERSION,
    description="API para gestionar datos de calidad del aire en AWS RDS PostgreSQL",
    lifespan=lifespan
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # En producción, especifica los orígenes permitidos
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Incluir todos los routers
app.include_router(zones.router, prefix="/api/v1")
app.include_router(stations.router, prefix="/api/v1")
app.include_router(pollutants.router, prefix="/api/v1")
app.include_router(capabilities.router, prefix="/api/v1")
app.include_router(measurements.router, prefix="/api/v1")

@app.get("/")
async def root():
    """
    Endpoint raíz
    """
    return {
        "message": "Air Quality API",
        "version": settings.API_VERSION,
        "docs": "/docs",
        "redoc": "/redoc"
    }

@app.get("/health")
async def health_check():
    """
    Health check endpoint
    """
    return {
        "status": "healthy",
        "api": settings.API_TITLE,
        "version": settings.API_VERSION
    }
