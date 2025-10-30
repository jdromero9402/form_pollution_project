"""
Schemas Pydantic para validación de datos

Este módulo exporta todos los schemas de entrada/salida para la API.
"""

from schemas.zone import ZoneCreate, ZoneResponse
from schemas.station import StationCreate, StationResponse
from schemas.pollutant import PollutantCreate, PollutantResponse
from schemas.capability import CapabilityCreate, CapabilityResponse
from schemas.measurement import MeasurementCreate, MeasurementResponse

__all__ = [
    # Zone schemas
    "ZoneCreate",
    "ZoneResponse",
    # Station schemas
    "StationCreate",
    "StationResponse",
    # Pollutant schemas
    "PollutantCreate",
    "PollutantResponse",
    # Capability schemas
    "CapabilityCreate",
    "CapabilityResponse",
    # Measurement schemas
    "MeasurementCreate",
    "MeasurementResponse",
]
