from dateutil import parser
import logging
import json
from sqlalchemy.orm import sessionmaker
from sqlalchemy import inspect
from models import create_all, Pending
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

    def _extract_from_header(self, headers, header_type):
        for header in headers:
            if header_type in header:
                for item in header:
                    if item == header_type:
                        continue
                    else:
                        return item.decode('ascii')

    def store_event(self, msg):
        value = json.loads(msg.value.decode('utf-8'))
        offset = msg.offset
        headers = msg.headers
        print('Message Offset: ', offset)
        print('Message Headers: ', headers)
        print('msg keys ', value.keys())
        created_at_string = value.get('created_at')
        created_at = parser.parse(created_at_string)
        name = value.get('name')
        tenant = value.get('tenant')
        operation = self._extract_from_header(headers, 'event_type')
        auth_header = self._extract_from_header(headers, 'x-rh-identity')
        new_event = Pending(offset=offset, created_at=created_at, name=name, tenant=tenant, operation=operation, auth_header=auth_header)
        self._session.add(new_event)
        self._session.commit()
