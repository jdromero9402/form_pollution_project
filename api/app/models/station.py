from sqlalchemy import Column, BigInteger, Integer, String, DECIMAL, TIMESTAMP, ForeignKey, Index
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from database import Base

class Station(Base):
    __tablename__ = 'stations'
    __table_args__ = {'schema': 'air'}
    
    station_sk = Column(BigInteger, primary_key=True, autoincrement=True)
    station_id = Column(Integer, nullable=False, unique=True)
    station_code = Column(String(50), nullable=False, unique=True)
    station_name = Column(String(200), nullable=False)
    zone_id = Column(Integer, ForeignKey('air.zones.zone_id', ondelete='CASCADE'))
    lat = Column(DECIMAL(9, 6))
    lon = Column(DECIMAL(9, 6))
    station_type = Column(String(100))
    authority = Column(String(100))
    years_min = Column(Integer)
    years_max = Column(Integer)
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp())
    
    # Relaciones
    zone = relationship("Zone", back_populates="stations")
    capabilities = relationship("StationCapability", back_populates="station")
    measurements = relationship("Measurement", back_populates="station")
    
    def __repr__(self):
        return f"<Station {self.station_code}: {self.station_name}>"
