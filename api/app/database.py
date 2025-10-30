from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import text
from typing import AsyncGenerator
from config import get_settings

settings = get_settings()

# Engine asíncrono
engine = create_async_engine(
    settings.database_url,
    echo=settings.API_DEBUG,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    pool_recycle=3600
)

# Session factory
AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False
)

# Base para los modelos
Base = declarative_base()

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency para obtener sesión de base de datos.
    Configura el schema correcto y maneja el ciclo de vida.
    """
    async with AsyncSessionLocal() as session:
        try:
            # Configurar el schema
            await session.execute(text(f"SET search_path TO {settings.DB_SCHEMA}"))
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

async def init_db():
    """Inicializa la base de datos (crear tablas si no existen)"""
    async with engine.begin() as conn:
        # Crear schema si no existe
        await conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {settings.DB_SCHEMA}"))
        # await conn.execute(text(f"SET search_path TO {settings.DB_SCHEMA}"))
        # Crear todas las tablas
        # await conn.run_sync(Base.metadata.create_all)

async def close_db():
    """Cierra las conexiones de la base de datos"""
    await engine.dispose()

