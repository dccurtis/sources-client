import logging
from sqlalchemy.orm import sessionmaker
from sqlalchemy import inspect
from models import create_all, User
from config import Config

LOG = logging.getLogger(__name__)
Config().setup_logger(LOG)

class StorageLayer:
    def __init__(self, engine):
        self._create_models(engine)
        Session = sessionmaker(bind=engine)
        self._session = Session()

    def _create_models(self, engine):
        create_all(engine)
        inspector = inspect(engine)
        LOG.info(inspector.get_table_names())

    def store_event(self, offset, operation, username):
        new_event = User(offset=offset, operation=operation, username=username)
        self._session.add(new_event)
        self._session.commit()
