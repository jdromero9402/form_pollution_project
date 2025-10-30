from pydantic import BaseModel, Field, ConfigDict
from fastapi import Form
from typing import Optional
from datetime import datetime
from decimal import Decimal

class MeasurementBase(BaseModel):
    station_id: int = Field(..., description="ID de la estación")
    pollutant_id: str = Field(..., max_length=20, description="ID del contaminante")
    ts_utc: datetime = Field(..., description="Timestamp UTC")
    value: Optional[Decimal] = Field(None, description="Valor medido")
    unit: Optional[str] = Field(None, max_length=20, description="Unidad")
    source: Optional[str] = Field(None, max_length=50, description="Fuente de datos")
    is_valid: bool = Field(True, description="¿Es válida la medición?")

class MeasurementCreate(MeasurementBase):
    """Schema para crear una medición desde form-data"""
    @classmethod
    def as_form(
        cls,
        station_id: int = Form(...),
        pollutant_id: int = Form(...),
        ts_utc: str = Form(...),
        value: str = Form(...),
        unit: Optional[str] = Form(None),
        source: Optional[str] = Form(None),
        is_valid: bool = Form(True),
    ) -> "MeasurementCreate":
        def parse_decimal(v):
            if v in (None, ''):
                return None
            from decimal import Decimal as _D
            return _D(v)
        def parse_datetime(s):
            if s in (None, ''):
                return None
            try:
                return datetime.fromisoformat(s)
            except Exception:
                # fallback: try to parse common formats if needed
                return None
        return cls(
            station_id=station_id,
            pollutant_id=pollutant_id,
            ts_utc=parse_datetime(ts_utc),
            value=parse_decimal(value),
            unit=unit,
            source=source,
            is_valid=is_valid,
        )

class MeasurementResponse(MeasurementBase):
    """Schema para la respuesta"""
    measurement_sk: int
    created_at: datetime
    
    model_config = ConfigDict(from_attributes=True)
