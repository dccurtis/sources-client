import os
import tornado.ioloop
import tornado.web
import asyncio
from concurrent.futures import ThreadPoolExecutor

from tornado_sqlalchemy import make_session_factory
from tornado.gen import coroutine
from tornado.ioloop import IOLoop
from tornado_sqlalchemy import SessionMixin, as_future
from kafkahelpers import ReconnectingClient
from aiokafka import AIOKafkaConsumer

from models import create_all, User

SOURCES_KAFKA_HOST = os.getenv('SOURCES_KAFKA_HOST', 'localhost')
SOURCES_KAFKA_PORT = os.getenv('SOURCES_KAFKA_PORT', '29092')
SOURCES_KAFKA_ADDRESS = f'{SOURCES_KAFKA_HOST}:{SOURCES_KAFKA_PORT}'
SOURCES_TOPIC = os.getenv('SOURCES_KAFKA_TOPIC', 'platform.sources.event-stream')
# kafka_consumer = AIOKafkaConsumer(SOURCES_TOPIC, loop=IOLoop.current().asyncio_loop,
#                                  bootstrap_servers=SOURCES_KAFKA_ADDRESS, group_id='koku-group')
# CONSUMER = ReconnectingClient(kafka_consumer, "consumer")
LOOPS = {}

thread_pool_executor = ThreadPoolExecutor(max_workers=10)

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

async def handle_validation(client):
    data = await client.getmany(timeout_ms=1000, max_records=1)
    for tp, msgs in data.items():
        if tp.topic == SOURCES_TOPIC:
            logger.info("Processing %s messages from topic [%s]", len(msgs), tp.topic, extra={
                "topic": tp.topic
            })
            # TODO: Figure out how to properly handle failures
            # await asyncio.gather(*[handle_file(msg) for msg in msgs])
            await asyncio.sleep(5)


if __name__ == "__main__":
    app = Application()
    app.listen(8888)
    print('Listeneing on port 8888')
    loop = IOLoop.current()
    loop.set_default_executor(thread_pool_executor)

    # LOOPS["consumer"] = IOLoop(make_current=False).instance()
    # LOOPS["consumer"].add_callback(CONSUMER.get_callback(handle_validation))
    # for loop in LOOPS.items():
    #     loop.start()
    # loop.start()
