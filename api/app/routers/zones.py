from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import List

from database import get_db
from models.zone import Zone
from schemas.zone import ZoneCreate, ZoneResponse

router = APIRouter(
    prefix="/zones",
    tags=["Zones"],
    responses={404: {"description": "Not found"}}
)

@router.post("/", response_model=ZoneResponse, status_code=status.HTTP_201_CREATED)
async def create_zone(
    zone_data: ZoneCreate = Depends(ZoneCreate.as_form),
    db: AsyncSession = Depends(get_db)
):
    """
    Crea una nueva zona en la base de datos
    """
    # Verificar si ya existe
    result = await db.execute(
        select(Zone).where(Zone.zone_id == zone_data.zone_id)
    )
    existing_zone = result.scalar_one_or_none()
    
    if existing_zone:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Zone with zone_id {zone_data.zone_id} already exists"
        )
    
    # Crear nueva zona
    new_zone = Zone(**zone_data.model_dump())
    db.add(new_zone)
    await db.commit()
    await db.refresh(new_zone)
    
    return new_zone

@router.get("/", response_model=List[ZoneResponse])
async def get_zones(
    db: AsyncSession = Depends(get_db)
):
    """
    Obtiene todas las zonas
    """
    result = await db.execute(
        select(Zone)
    )
    zones = result.scalars().all()
    return zones

@router.get("/{zone_id}", response_model=ZoneResponse)
async def get_zone(
    zone_id: int,
    db: AsyncSession = Depends(get_db)
):
    """
    Obtiene una zona específica por zone_id
    """
    result = await db.execute(
        select(Zone).where(Zone.zone_id == zone_id)
    )
    zone = result.scalar_one_or_none()
    
    if not zone:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Zone with zone_id {zone_id} not found"
        )
    
    return zone
