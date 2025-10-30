from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import List

from database import get_db
from models.capability import StationCapability
from schemas.capability import CapabilityCreate, CapabilityResponse

router = APIRouter(
    prefix="/capabilities",
    tags=["Station Capabilities"],
    responses={404: {"description": "Not found"}}
)

@router.post("/", response_model=CapabilityResponse, status_code=status.HTTP_201_CREATED)
async def create_capability(
    capability_data: CapabilityCreate = Depends(CapabilityCreate.as_form),
    db: AsyncSession = Depends(get_db)
):
    """
    Crea una nueva capacidad de estación (qué contaminantes puede medir)
    """
    # Crear nueva capacidad
    new_capability = StationCapability(**capability_data.model_dump())
    db.add(new_capability)
    await db.commit()
    await db.refresh(new_capability)
    
    return new_capability

@router.post("/bulk", status_code=status.HTTP_201_CREATED)
async def create_capabilities_bulk(
    capabilities_data: List[CapabilityCreate],
    db: AsyncSession = Depends(get_db)
):
    """
    Crea múltiples capacidades en bulk
    """
    new_capabilities = [
        StationCapability(**capability.model_dump())
        for capability in capabilities_data
    ]
    
    db.add_all(new_capabilities)
    await db.commit()
    
    return {
        "message": f"{len(new_capabilities)} capabilities created successfully",
        "count": len(new_capabilities)
    }

@router.get("/", response_model=List[CapabilityResponse])
async def get_capabilities(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db)
):
    """
    Obtiene todas las capacidades
    """
    result = await db.execute(
        select(StationCapability).offset(skip).limit(limit)
    )
    capabilities = result.scalars().all()
    return capabilities

@router.get("/station/{station_id}", response_model=List[CapabilityResponse])
async def get_capabilities_by_station(
    station_id: int,
    db: AsyncSession = Depends(get_db)
):
    """
    Obtiene las capacidades de una estación específica
    """
    result = await db.execute(
        select(StationCapability).where(StationCapability.station_id == station_id)
    )
    capabilities = result.scalars().all()
    
    if not capabilities:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No capabilities found for station {station_id}"
        )
    
    return capabilities
