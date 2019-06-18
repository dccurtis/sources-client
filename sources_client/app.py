import os
import json
import logging
import tornado.web
import asyncio

import threading
from tornado_sqlalchemy import make_session_factory
from tornado.gen import coroutine
from tornado.ioloop import IOLoop
from tornado_sqlalchemy import SessionMixin, as_future
from kafkahelpers import ReconnectingClient
from aiokafka import AIOKafkaConsumer
from kafka.errors import ConnectionError as KafkaConnectionError

from models import create_all, User

EVENT_LOOP = asyncio.get_event_loop()
MSG_PENDING_QUEUE = asyncio.Queue()

SOURCES_KAFKA_HOST = os.getenv('SOURCES_KAFKA_HOST', 'kafka')
SOURCES_KAFKA_PORT = os.getenv('SOURCES_KAFKA_PORT', '29092')
SOURCES_KAFKA_ADDRESS = f'{SOURCES_KAFKA_HOST}:{SOURCES_KAFKA_PORT}'
SOURCES_TOPIC = os.getenv('SOURCES_KAFKA_TOPIC', 'platform.sources.event-stream')

LOOPS = {}

LOG = logging.getLogger(__name__)

class KafkaMsgHandlerError(Exception):
    """Kafka mmsg handler error."""

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


def handle_message(msg):
    """
    Handle messages from message pending queue.

    Handle's messages with topics: 'platform.upload.hccm',
    and 'platform.upload.available'.

    The OCP cost usage payload will land on topic hccm.
    These messages will be extracted into the local report
    directory structure.  Once the file has been verified
    (successfully extracted) we will report the status to
    the Insights Upload Service so the file can be made available
    to other apps on the service.

    Messages on the available topic are messages that have
    been verified by an app on the Insights upload service.
    For now we are just logging the URL for demonstration purposes.
    In the future if we want to maintain a URL to our report files
    in the upload service we could look for hashes for files that
    we have previously validated on the hccm topic.


    Args:
        msg - Upload Service message containing usage payload information.

    Returns:
        (String, dict) - String: Upload Service confirmation status
                         dict: keys: value
                               file: String,
                               cluster_id: String,
                               payload_date: DateTime,
                               manifest_path: String,
                               uuid: String,
                               manifest_path: String

    """
    if msg.topic == SOURCES_TOPIC:
        value = json.loads(msg.value.decode('utf-8'))
        try:
            # report_meta = extract_payload(value['url'])
            print(str(value))
            return True
        except KafkaMsgHandlerError as error:
            LOG.error('Unable to extract payload. Error: %s', str(error))
            return False
    else:
        LOG.error('Unexpected Message')
    return None, None

async def process_messages():  # pragma: no cover
    """
    Process asyncio MSG_PENDING_QUEUE and send validation status.

    Args:
        None

    Returns:
        None

    """
    while True:
        msg = await MSG_PENDING_QUEUE.get()
        status =handle_message(msg)
        if status:
            value = json.loads(msg.value.decode('utf-8'))
            print(value)
            # await send_confirmation(value['request_id'], status)

async def listen_for_messages(consumer):  # pragma: no cover
    """
    Listen for messages on the available and hccm topics.

    Once a message from one of these topics arrives, we add
    them to the MSG_PENDING_QUEUE.

    Args:
        None

    Returns:
        None

    """
    try:
        await consumer.start()
    except KafkaConnectionError:
        await consumer.stop()
        raise KafkaMsgHandlerError('Unable to connect to kafka server.')

    LOG.info('Listener started.  Waiting for messages...')
    try:
        # Consume messages
        async for msg in consumer:
            await MSG_PENDING_QUEUE.put(msg)
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()

def asyncio_listener_thread(loop):  # pragma: no cover
    """
    Worker thread function to run the asyncio event loop.

    Args:
        None

    Returns:
        None

    """
    try:
        while True:
            consumer = AIOKafkaConsumer(
                SOURCES_TOPIC,
                loop=EVENT_LOOP, bootstrap_servers=SOURCES_KAFKA_ADDRESS
            )
            # consumer_client = ReconnectingClient(consumer, "consumer")
            loop.create_task(process_messages())

            try:
                loop.run_until_complete(listen_for_messages(consumer))
            except KafkaMsgHandlerError as err:
                LOG.info('Kafka connection failure.  Error: %s', str(err))
            LOG.info('Attempting to reconnect')

    except KeyboardInterrupt:
        exit(0)

def initialize_kafka_listener():
    event_loop_thread = threading.Thread(target=asyncio_listener_thread, args=(EVENT_LOOP,))
    event_loop_thread.daemon = True
    event_loop_thread.start()
    event_loop_thread.join()

if __name__ == "__main__":
    app = Application()
    app.listen(8888)
    print('Listeneing on port 8888')
    loop = IOLoop.current()
    loop.start()

    initialize_kafka_listener()

