import os
import tornado.ioloop
import tornado.web
from tornado_sqlalchemy import make_session_factory
from tornado.gen import coroutine
from tornado_sqlalchemy import SessionMixin, as_future

from models import create_all, User


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

        DB_NAME = os.getenv('DATABASE_NAME', 'sources-db')
        DB_USER = os.getenv('DATABASE_USER', 'postgres')
        DB_PASSWORD = os.getenv('DATABASE_PASSWORD', 'postgres')
        DB_HOST = os.getenv('POSTGRES_SQL_SERVICE_HOST', 'sources-db')
        DB_PORT = os.getenv('POSTGRES_SQL_SERVICE_PORT', '5432')
        database_url = f'postgres://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        print(database_url)
        
        factory = make_session_factory(database_url)
        create_all(factory.engine)

        tornado.web.Application.__init__(self, handlers, session_factory=factory)

if __name__ == "__main__":
    app = Application()
    app.listen(8888)
    print('Listeneing on port 8888')
    tornado.ioloop.IOLoop.current().start()

