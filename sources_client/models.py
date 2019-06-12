import os

from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, DateTime, Boolean, BigInteger
from sqlalchemy.ext.declarative import declarative_base


DeclarativeBase = declarative_base()

class User(DeclarativeBase):
    __tablename__ = 'users'

    id = Column(BigInteger, primary_key=True)
    username = Column(String(255), unique=True)

def __repr__(self):
    return "<User('%s')>" % (self.username)

users_table = User.__table__
metadata = DeclarativeBase.metadata

def create_all():
    metadata.create_all(engine)
