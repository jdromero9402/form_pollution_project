from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import List

from database import get_db
from models.station import Station
from schemas.station import StationCreate, StationResponse

router = APIRouter(
    prefix="/stations",
    tags=["Stations"],
    responses={404: {"description": "Not found"}}
)

@router.post("/", response_model=StationResponse, status_code=status.HTTP_201_CREATED)
async def create_station(
    station_data: StationCreate = Depends(StationCreate.as_form),
    db: AsyncSession = Depends(get_db)
):
    """
    Crea una nueva estación de monitoreo
    """
    # Verificar si ya existe
    result = await db.execute(
        select(Station).where(Station.station_id == station_data.station_id)
    )
    existing_station = result.scalar_one_or_none()
    
    if existing_station:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Station with station_id {station_data.station_id} already exists"
        )
    
    # Crear nueva estación
    new_station = Station(**station_data.model_dump())
    db.add(new_station)
    await db.commit()
    await db.refresh(new_station)
    
    return new_station

@router.get("/", response_model=List[StationResponse])
async def get_stations(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db)
):
    """
    Obtiene todas las estaciones
    """
    result = await db.execute(
        select(Station).offset(skip).limit(limit)
    )
    stations = result.scalars().all()
    return stations
