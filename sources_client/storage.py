from dateutil import parser
import logging
import json
from sqlalchemy.orm import sessionmaker
from sqlalchemy import inspect
from models import create_all, Provider
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
        # print('Message Offset: ', offset)
        # print('Message Headers: ', headers)
        # print('msg keys ', value.keys())
        created_at_string = value.get('created_at')
        created_at = parser.parse(created_at_string)
        source_id = int(value.get('source_id'))
        name = value.get('name')
        operation = self._extract_from_header(headers, 'event_type')
        auth_header = self._extract_from_header(headers, 'x-rh-identity')
        new_event = Pending(offset=offset, created_at=created_at, source_id=source_id, operation=operation, name=name, auth_header=auth_header)
        self._session.add(new_event)
        self._session.commit()

    def load_providers_to_create(self):
        providers_to_create = []
        for provider in self._session.query(Provider).all():
            if provider.source_type == 'AWS':
                if provider.source_id and provider.name and provider.auth_header and provider.billing_source and not provider.koku_uuid:
                    providers_to_create.append(provider)
            else:
                if provider.source_id and provider.name and provider.auth_header and not provider.koku_uuid:
                    providers_to_create.append(provider)

        return providers_to_create

    async def load_events_queue(self, queue):
        for event in self._session.query(Pending).all():
            print('trying to add events')
            await queue.put(event)

    def create_provider_event(self, msg):
        value = json.loads(msg.value.decode('utf-8'))
        offset = msg.offset
        headers = msg.headers
        source_id = int(value.get('source_id'))
        name = value.get('name')
        operation = self._extract_from_header(headers, 'event_type')
        auth_header = self._extract_from_header(headers, 'x-rh-identity')
        query = self._session.query(Provider).get(source_id)
        if query:
            query.name = name
            query.auth_header = auth_header
        else:
            new_event = Provider(source_id=source_id, name=name, auth_header=auth_header)
            self._session.add(new_event)
        self._session.commit()

    def destroy_provider_event(self, msg):
        value = json.loads(msg.value.decode('utf-8'))
        source_id = int(value.get('source_id'))
        query = self._session.query(Provider).get(source_id)
        koku_uuid = query.koku_uuid
        self._session.delete(query)
        self._session.commit()
        return koku_uuid

    def add_provider_sources_network_info(self, source_id, name, source_type, authentication):
        query = self._session.query(Provider).get(source_id)
        query.name = name
        query.source_type = source_type
        query.authentication = authentication
        self._session.commit()

    def add_provider_billing_source(self, source_id, billing_source):
        query = self._session.query(Provider).get(source_id)
        query.billing_source = billing_source
        self._session.commit()

    def create_provider(self, source_id, name, auth_header):
        query = self._session.query(Provider).filter(Provider.source_id == source_id).first()
        print(query)
        if not self._session.query(Provider).filter(Provider.source_id == source_id):
            new_provider = Provider(source_id=source_id, name=name, auth_header=auth_header)
            self._session.add(new_provider)
            self._session.commit()

    def add_provider_koku_uuid(self, source_id, koku_uuid):
        query = self._session.query(Provider).get(source_id)
        query.koku_uuid = koku_uuid
        self._session.commit()
