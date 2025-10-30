from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import List

from database import get_db
from models.pollutant import Pollutant
from schemas.pollutant import PollutantCreate, PollutantResponse

router = APIRouter(
    prefix="/pollutants",
    tags=["Pollutants"],
    responses={404: {"description": "Not found"}}
)

@router.post("/", response_model=PollutantResponse, status_code=status.HTTP_201_CREATED)
async def create_pollutant(
    pollutant_data: PollutantCreate = Depends(PollutantCreate.as_form),
    db: AsyncSession = Depends(get_db)
):
    """
    Crea un nuevo contaminante
    """
    # Verificar si ya existe
    result = await db.execute(
        select(Pollutant).where(Pollutant.pollutant_id == pollutant_data.pollutant_id)
    )
    existing_pollutant = result.scalar_one_or_none()
    
    if existing_pollutant:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Pollutant with pollutant_id {pollutant_data.pollutant_id} already exists"
        )
    
    # Crear nuevo contaminante
    new_pollutant = Pollutant(**pollutant_data.model_dump())
    db.add(new_pollutant)
    await db.commit()
    await db.refresh(new_pollutant)
    
    return new_pollutant

@router.get("/", response_model=List[PollutantResponse])
async def get_pollutants(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db)
):
    """
    Obtiene todos los contaminantes
    """
    result = await db.execute(
        select(Pollutant).offset(skip).limit(limit)
    )
    pollutants = result.scalars().all()
    return pollutants
