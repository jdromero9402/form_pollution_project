from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import List

from database import get_db
from models.measurement import Measurement
from schemas.measurement import MeasurementCreate, MeasurementResponse

router = APIRouter(
    prefix="/measurements",
    tags=["Measurements"],
    responses={404: {"description": "Not found"}}
)

@router.post("/", response_model=MeasurementResponse, status_code=status.HTTP_201_CREATED)
async def create_measurement(
    measurement_data: MeasurementCreate = Depends(MeasurementCreate.as_form),
    db: AsyncSession = Depends(get_db)
):
    """
    Crea una nueva medición
    """
    new_measurement = Measurement(**measurement_data.model_dump())
    db.add(new_measurement)
    await db.commit()
    await db.refresh(new_measurement)
    
    return new_measurement

@router.post("/bulk", status_code=status.HTTP_201_CREATED)
async def create_measurements_bulk(
    measurements_data: List[MeasurementCreate],
    db: AsyncSession = Depends(get_db)
):
    """
    Crea múltiples mediciones en bulk (más eficiente)
    """
    new_measurements = [
        Measurement(**measurement.model_dump())
        for measurement in measurements_data
    ]
    
    db.add_all(new_measurements)
    await db.commit()
    
    return {
        "message": f"{len(new_measurements)} measurements created successfully",
        "count": len(new_measurements)
    }

@router.get("/", response_model=List[MeasurementResponse])
async def get_measurements(
    db: AsyncSession = Depends(get_db)
):
    """
    Obtiene mediciones
    """
    result = await db.execute(
        select(Measurement)
    )
    measurements = result.scalars().all()
    return measurements
