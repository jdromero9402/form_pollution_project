from sqlalchemy import Column, String, DECIMAL, TIMESTAMP, event
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from database import Base
from decimal import Decimal

class Pollutant(Base):
    __tablename__ = 'pollutants'
    __table_args__ = {'schema': 'air'}
    
    pollutant_id = Column(String(20), primary_key=True)
    name = Column(String(100), nullable=False)
    default_unit = Column(String(20))
    who_daily_limit = Column(DECIMAL(10, 3))
    who_annual_limit = Column(DECIMAL(10, 3))
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp())
    
    # Relaciones
    capabilities = relationship("StationCapability", back_populates="pollutant")
    measurements = relationship("Measurement", back_populates="pollutant")
    
    def __repr__(self):
        return f"<Pollutant {self.pollutant_id}: {self.name}>"

@event.listens_for(Pollutant, "load")
def _sanitize_who_annual_limit(target, context):
    v = target.who_annual_limit
    if isinstance(v, Decimal) and (v.is_nan() or v.is_infinite()):
        target.who_annual_limit = None