from sqlalchemy import Column, BigInteger, Integer, String, ForeignKey, Index
from sqlalchemy.orm import relationship
from database import Base

class StationCapability(Base):
    __tablename__ = 'station_capabilities'
    __table_args__ = {'schema': 'air'}
    
    capability_sk = Column(BigInteger, primary_key=True, autoincrement=True)
    station_id = Column(Integer, ForeignKey('air.stations.station_id', ondelete='CASCADE'), nullable=False)
    pollutant_id = Column(String(20), ForeignKey('air.pollutants.pollutant_id', ondelete='CASCADE'), nullable=False)
    unit = Column(String(20))
    
    # Relaciones
    station = relationship("Station", back_populates="capabilities")
    pollutant = relationship("Pollutant", back_populates="capabilities")
    
    def __repr__(self):
        return f"<Capability Station:{self.station_id} - Pollutant:{self.pollutant_id}>"
