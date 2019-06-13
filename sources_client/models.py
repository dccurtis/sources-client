from sqlalchemy import Column, String, BigInteger
from sqlalchemy.ext.declarative import declarative_base


DeclarativeBase = declarative_base()

class User(DeclarativeBase):
    __tablename__ = 'users'

    id = Column(BigInteger, primary_key=True)
    username = Column(String(255), unique=True)

def __repr__(self):
    return "<User('%s')>" % (self.username)

users_table = User.__table__

def create_all(engine):
    DeclarativeBase.metadata.create_all(engine)
