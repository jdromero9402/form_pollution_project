from pydantic import BaseModel, Field, ConfigDict
from fastapi import Form
from typing import Optional
from datetime import datetime
from decimal import Decimal

class ZoneBase(BaseModel):
    zone_id: int = Field(..., description="ID único de la zona")
    city_code: str = Field(..., max_length=10, description="Código de la ciudad")
    city_name: str = Field(..., max_length=100, description="Nombre de la ciudad")
    zone_code: Optional[str] = Field(None, max_length=50, description="Código de la zona")
    zone_name: Optional[str] = Field(None, max_length=100, description="Nombre de la zona")
    population: Optional[int] = Field(None, description="Población")
    centroid_lat: Optional[Decimal] = Field(None, description="Latitud del centroide")
    centroid_lon: Optional[Decimal] = Field(None, description="Longitud del centroide")

class ZoneCreate(ZoneBase):
    """Schema para crear una zona"""
    @classmethod
    def as_form(
        cls,
        zone_id: int = Form(...),
        city_code: str = Form(...),
        city_name: str = Form(...),
        zone_code: Optional[str] = Form(None),
        zone_name: Optional[str] = Form(None),
        population: Optional[int] = Form(None),
        centroid_lat: Optional[str] = Form(None),
        centroid_lon: Optional[str] = Form(None),
    ) -> "ZoneCreate":
        # convertir cadenas vacías / None y parsear Decimal
        def parse_decimal(v):
            if v in (None, ''):
                return None
            from decimal import Decimal as _D
            return _D(v)
        return cls(
            zone_id=zone_id,
            city_code=city_code,
            city_name=city_name,
            zone_code=zone_code,
            zone_name=zone_name,
            population=population,
            centroid_lat=parse_decimal(centroid_lat),
            centroid_lon=parse_decimal(centroid_lon),
        )

class ZoneResponse(ZoneBase):
    """Schema para la respuesta"""
    zone_sk: int
    created_at: datetime
    
    model_config = ConfigDict(from_attributes=True)
