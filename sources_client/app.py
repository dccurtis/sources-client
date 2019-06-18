import os
import logging
import tornado.web
import asyncio

from tornado_sqlalchemy import make_session_factory
from tornado.gen import coroutine
from tornado.ioloop import IOLoop
from tornado_sqlalchemy import SessionMixin, as_future

from config import Config
from storage import StorageLayer
from models import Pending
from kafka_listener import initialize_kafka_listener

LOG = logging.getLogger(__name__)
Config().setup_logger(LOG)

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world")

class UserRequestHandler(SessionMixin, tornado.web.RequestHandler):
    @coroutine
    def get(self):
        with self.make_session() as session:
            count = yield as_future(session.query(Pending).count)

        self.write('{} users so far!'.format(count))


class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/hello", MainHandler),
            (r'/user', UserRequestHandler),
        ]
        database_url = Config.DATABASE_URL
        print('Database URL: ', database_url)

        factory = make_session_factory(database_url)
        self._storage = StorageLayer(factory.engine)

        # TODO: Make a REST API call to /application_types and save id for "name": "/insights/platform/cost-management" object
        # Then the Sources.create and Sources.destroy kafka message event types should have source_id in the payload that we can filter on application_type_id (Double check this)
        # Need to find out how often this can/should change and move it somewhere else to it's own event loop to be listening for application_source_id changes.

        self._cost_management_source_id = 2

        initialize_kafka_listener(self._storage, self._cost_management_source_id)

        tornado.web.Application.__init__(self, handlers, session_factory=factory)


if __name__ == "__main__":
    app = Application()
    app.listen(8888)
    print('Listening on port 8888')
    loop = IOLoop.current()
    loop.start()
