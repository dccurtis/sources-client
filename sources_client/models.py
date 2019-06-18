from sqlalchemy import Column, String, BigInteger, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base


DeclarativeBase = declarative_base()

class Pending(DeclarativeBase):
    __tablename__ = 'pending_msg'

    id = Column(BigInteger, primary_key=True)
    offset = Column(Integer, unique=True)
    created_at = Column(DateTime(timezone=True))
    operation = Column(String(128))
    name = Column(String(128))
    tenant = Column(String(128))
    auth_header = Column(String(512))

def __repr__(self):
    return "<Pending('%s')>" % (self.name)

def create_all(engine):
    DeclarativeBase.metadata.create_all(engine)

# handle msg value:  {"id":16,"name":"AWS","uid":"9688fc8c-4922-4164-8eed-907c2fd6e0a5","created_at":"2019-06-18 18:09:14 UTC","updated_at":"2019-06-18 18:09:14 UTC","source_type_id":2,"version":null,"tenant":"1001"}