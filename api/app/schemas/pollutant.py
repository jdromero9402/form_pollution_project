from pydantic import BaseModel, Field, ConfigDict
from fastapi import Form
from typing import Optional
from datetime import datetime
from decimal import Decimal

class PollutantBase(BaseModel):
    pollutant_id: str = Field(..., max_length=20, description="ID del contaminante")
    name: str = Field(..., max_length=100, description="Nombre del contaminante")
    default_unit: Optional[str] = Field(None, max_length=20, description="Unidad por defecto")
    who_daily_limit: Optional[Decimal] = Field(None, description="Límite diario OMS")
    who_annual_limit: Optional[Decimal] = Field(None, description="Límite anual OMS")

class PollutantCreate(PollutantBase):
    """Schema para crear un contaminante desde form-data"""
    @classmethod
    def as_form(
        cls,
        pollutant_id: str = Form(...),
        name: str = Form(...),
        default_unit: Optional[str] = Form(None),
        who_daily_limit: Optional[Decimal] = Form(None),
        who_annual_limit: Optional[Decimal] = Form(None),
    ) -> "PollutantCreate":
                # convertir cadenas vacías / None y parsear Decimal
        def parse_decimal(v):
            if v in (None, ''):
                return None
            from decimal import Decimal as _D
            return _D(v)
        return cls(
            pollutant_id=pollutant_id,
            name=name,
            default_unit=default_unit,
            who_daily_limit=parse_decimal(who_daily_limit),
            who_annual_limit=parse_decimal(who_annual_limit),
        )

class PollutantResponse(PollutantBase):
    """Schema para la respuesta"""
    created_at: datetime
    
    model_config = ConfigDict(from_attributes=True)
