from pydantic import BaseModel, Field, ConfigDict
from fastapi import Form
from typing import Optional

class CapabilityBase(BaseModel):
    station_id: int = Field(..., description="ID de la estación")
    pollutant_id: str = Field(..., max_length=20, description="ID del contaminante")
    unit: Optional[str] = Field(None, max_length=20, description="Unidad de medición")

class CapabilityCreate(CapabilityBase):
    """Schema para crear una capacidad"""
    @classmethod
    def as_form(
        cls,
        station_id: int = Form(...),
        pollutant_id: str = Form(...),
        unit: Optional[str] = Form(None),
    ) -> "CapabilityCreate":
        return cls(
            station_id=station_id,
            pollutant_id=pollutant_id,
            unit=unit,
        )

class CapabilityResponse(CapabilityBase):
    """Schema para la respuesta"""
    capability_sk: int
    
    model_config = ConfigDict(from_attributes=True)
