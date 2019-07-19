import os
import logging
import tornado.web
import asyncio

from tornado_sqlalchemy import make_session_factory
from tornado.gen import coroutine
from tornado.ioloop import IOLoop
from tornado_sqlalchemy import SessionMixin, as_future
from tornado.escape import json_decode
from config import Config
from storage import StorageLayer
from models import Provider
from kafka_listener import initialize_kafka_listener
from provider_builder import initialize_provider_builder

LOG = logging.getLogger(__name__)
Config().setup_logger(LOG)

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world")

class UserRequestHandler(SessionMixin, tornado.web.RequestHandler):
    @coroutine
    def get(self):
        with self.make_session() as session:
            count = yield as_future(session.query(Provider).count)

        self.write('{} users so far!'.format(count))

class BillingSourceHandler(SessionMixin, tornado.web.RequestHandler):
    @coroutine
    def post(self):
        data_json = json_decode(self.request.body)
        source_id = int(data_json.get('source_id'))
        billing_source = data_json.get('billing_source')

        #TODO make this async?
        with self.make_session() as session:
            query = session.query(Provider).get(source_id)
            if query:
                query.billing_source = billing_source
            else:
                new_provider = Provider(source_id=source_id, billing_source=billing_source)
                session.add(new_provider)
            session.commit()
        self.write('Source ID: {} Billing Source: {}'.format(str(source_id), str(billing_source)))

class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/hello", MainHandler),
            (r'/user', UserRequestHandler),
            (r'/billing_source', BillingSourceHandler),
        ]
        database_url = Config.DATABASE_URL
        print('Database URL: ', database_url)

        factory = make_session_factory(database_url)
        self._storage = StorageLayer(factory.engine)

        initialize_kafka_listener(self._storage)
        initialize_provider_builder(self._storage)

        tornado.web.Application.__init__(self, handlers, session_factory=factory)


if __name__ == "__main__":
    app = Application()
    app.listen(8888)
    print('Listening on port 8888')
    loop = IOLoop.current()
    loop.start()
