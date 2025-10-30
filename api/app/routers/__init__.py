"""
Routers de la API

Este módulo exporta todos los routers para facilitar su registro
en la aplicación principal.
"""

from routers import zones
from routers import stations
from routers import pollutants
from routers import capabilities
from routers import measurements

# Lista de todos los routers
__all__ = [
    "zones",
    "stations",
    "pollutants",
    "capabilities",
    "measurements"
]
