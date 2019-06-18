from sqlalchemy import Column, String, BigInteger, Integer
from sqlalchemy.ext.declarative import declarative_base


DeclarativeBase = declarative_base()

class User(DeclarativeBase):
    __tablename__ = 'users'

    id = Column(BigInteger, primary_key=True)
    offset = Column(Integer, unique=True)
    username = Column(String(255))
    operation = Column(String(128))

def __repr__(self):
    return "<User('%s')>" % (self.username)

def create_all(engine):
    DeclarativeBase.metadata.create_all(engine)
