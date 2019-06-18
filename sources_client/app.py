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
from models import User
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
            count = yield as_future(session.query(User).count)

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
        initialize_kafka_listener(self._storage)

        tornado.web.Application.__init__(self, handlers, session_factory=factory)


if __name__ == "__main__":
    app = Application()
    app.listen(8888)
    print('Listening on port 8888')
    loop = IOLoop.current()
    loop.start()
