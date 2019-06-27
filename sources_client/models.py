from sqlalchemy import Column, String, BigInteger, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base


DeclarativeBase = declarative_base()

class Provider(DeclarativeBase):
    __tablename__ = 'provider'

    source_id = Column(Integer, primary_key=True)
    name = Column(String(128))
    source_type = Column(String(128))
    authentication = Column(String(128))
    billing_source = Column(String(128))
    koku_uuid = Column(String(128))
    auth_header = Column(String(512))

    def __repr__(self):
        return "<Provider('%s')>" % (self.name)


def create_all(engine):
    DeclarativeBase.metadata.create_all(engine)
