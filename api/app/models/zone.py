from sqlalchemy import Column, BigInteger, Integer, String, DECIMAL, TIMESTAMP, Index
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from database import Base

class Zone(Base):
    __tablename__ = 'zones'
    __table_args__ = {'schema': 'air'}
    
    zone_sk = Column(BigInteger, primary_key=True, autoincrement=True)
    zone_id = Column(Integer, nullable=False, unique=True)
    city_code = Column(String(10), nullable=False)
    city_name = Column(String(100), nullable=False)
    zone_code = Column(String(50))
    zone_name = Column(String(100))
    population = Column(Integer)
    centroid_lat = Column(DECIMAL(9, 6))
    centroid_lon = Column(DECIMAL(9, 6))
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp())
    
    # Relaciones
    stations = relationship("Station", back_populates="zone")
    
    def __repr__(self):
        return f"<Zone {self.zone_code}: {self.zone_name}>"
