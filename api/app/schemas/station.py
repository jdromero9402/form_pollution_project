from pydantic import BaseModel, Field, ConfigDict
from fastapi import Form
from typing import Optional
from datetime import datetime
from decimal import Decimal

class StationBase(BaseModel):
    station_id: int = Field(..., description="ID único de la estación")
    station_code: str = Field(..., max_length=50, description="Código de la estación")
    station_name: str = Field(..., max_length=200, description="Nombre de la estación")
    zone_id: int = Field(..., description="ID de la zona")
    lat: Optional[Decimal] = Field(None, description="Latitud")
    lon: Optional[Decimal] = Field(None, description="Longitud")
    station_type: Optional[str] = Field(None, max_length=100, description="Tipo de estación")
    authority: Optional[str] = Field(None, max_length=100, description="Autoridad responsable")
    years_min: Optional[int] = Field(None, description="Año mínimo de operación")
    years_max: Optional[int] = Field(None, description="Año máximo de operación")

class StationCreate(StationBase):
    """Schema para crear una estación desde form-data"""
    @classmethod
    def as_form(
        cls,
        station_id: int = Form(...),
        station_code: str = Form(...),
        station_name: str = Form(...),
        zone_id: int = Form(...),
        lat: Optional[str] = Form(None),
        lon: Optional[str] = Form(None),
        station_type: Optional[str] = Form(None),
        authority: Optional[str] = Form(None),
        years_min: Optional[int] = Form(None),
        years_max: Optional[int] = Form(None),
    ) -> "StationCreate":
        def parse_decimal(v):
            if v in (None, ''):
                return None
            from decimal import Decimal as _D
            return _D(v)
        return cls(
            station_id=station_id,
            station_code=station_code,
            station_name=station_name,
            zone_id=zone_id,
            lat=parse_decimal(lat),
            lon=parse_decimal(lon),
            station_type=station_type,
            authority=authority,
            years_min=years_min,
            years_max=years_max,
        )

class StationResponse(StationBase):
    """Schema para la respuesta"""
    station_sk: int
    created_at: datetime
    
    model_config = ConfigDict(from_attributes=True)
