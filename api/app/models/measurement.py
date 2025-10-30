from sqlalchemy import Column, BigInteger, Integer, String, DECIMAL, TIMESTAMP, Boolean, ForeignKey, Index
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from database import Base

class Measurement(Base):
    __tablename__ = 'measurements_raw'
    __table_args__ = {'schema': 'air'}
    
    measurement_sk = Column(BigInteger, primary_key=True, autoincrement=True)
    station_id = Column(Integer, ForeignKey('air.stations.station_id', ondelete='CASCADE'), nullable=False)
    pollutant_id = Column(String(20), ForeignKey('air.pollutants.pollutant_id', ondelete='CASCADE'), nullable=False)
    ts_utc = Column(TIMESTAMP, nullable=False)
    value = Column(DECIMAL(12, 4))
    unit = Column(String(20))
    source = Column(String(50))
    is_valid = Column(Boolean, default=True)
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp())
    
    # Relaciones
    station = relationship("Station", back_populates="measurements")
    pollutant = relationship("Pollutant", back_populates="measurements")
    
    def __repr__(self):
        return f"<Measurement {self.station_id}-{self.pollutant_id} @ {self.ts_utc}>"
