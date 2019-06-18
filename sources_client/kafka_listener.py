import os
import logging
import json
import asyncio
import threading
from aiokafka import AIOKafkaConsumer

from kafka.errors import ConnectionError as KafkaConnectionError

from config import Config
from storage import StorageLayer
LOG = logging.getLogger(__name__)
Config().setup_logger(LOG)

class KafkaMsgHandlerError(Exception):
    """Kafka mmsg handler error."""

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
    if msg.topic == Config.SOURCES_TOPIC:
        try:
            value = json.loads(msg.value.decode('utf-8'))
            print('handle msg value: ', str(value))
            return True
        except Exception as error:
            LOG.error('Unable load message. Error: %s', str(error))
            return False
    else:
        LOG.error('Unexpected Message')
    return False

async def process_messages(msg_pending_queue, storage):  # pragma: no cover
    """
    Process asyncio MSG_PENDING_QUEUE and send validation status.

    Args:
        None

    Returns:
        None

    """
    while True:
        msg = await msg_pending_queue.get()
        status = handle_message(msg)
        if status:
            value = json.loads(msg.value.decode('utf-8'))
            offset = msg.offset
            print('Message Offset: ', offset)
            username = value.get('username', None)
            operation = value.get('operation', None)
            if username and operation:
                print('Found Username: ', username)
                print('Found Operation: ', operation)
                storage.store_event(offset,operation, username)



async def listen_for_messages(consumer, msg_pending_queue):  # pragma: no cover
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

    print('Listener started.  Waiting for messages...')
    try:
        # Consume messages
        async for msg in consumer:
            await msg_pending_queue.put(msg)
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()


def asyncio_listener_thread(storage):
    """
    Worker thread function to run the asyncio event loop.

    Args:
        None

    Returns:
        None

    """
    event_loop = asyncio.new_event_loop()
    pending_process_queue = asyncio.Queue(loop=event_loop)
    try:
        while True:
            consumer = AIOKafkaConsumer(
                Config.SOURCES_TOPIC,
                loop=event_loop, bootstrap_servers=Config.SOURCES_KAFKA_ADDRESS
            )

            event_loop.create_task(process_messages(pending_process_queue, storage))

            try:
                event_loop.run_until_complete(listen_for_messages(consumer, pending_process_queue))
            except KafkaMsgHandlerError as err:
                print('Kafka connection failure.  Error: ', str(err))
            print('Attempting to reconnect')

    except KeyboardInterrupt:
        exit(0)

def initialize_kafka_listener(storage):
    event_loop_thread = threading.Thread(target=asyncio_listener_thread, args=(storage,))
    event_loop_thread.daemon = True
    event_loop_thread.start()
    print('Listening for kafka events')
