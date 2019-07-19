import os
import logging
import json
import asyncio
import threading
from aiokafka import AIOKafkaConsumer

from kafka.errors import ConnectionError as KafkaConnectionError

from config import Config
from storage import StorageLayer
from utils import extract_from_header
from sources_http_client import SourcesHTTPClient
from koku_http_client import KokuHTTPClient

LOG = logging.getLogger(__name__)
Config().setup_logger(LOG)

class KafkaMsgHandlerError(Exception):
    """Kafka mmsg handler error."""

def filter_message(msg, application_source_id):
    if msg.topic == Config.SOURCES_TOPIC:
        if extract_from_header(msg.headers, 'event_type') == 'Application.create':
            try:
                value = json.loads(msg.value.decode('utf-8'))
                print('handle msg value: ', str(value))
                if int(value.get('application_type_id')) == application_source_id:
                    return True, 'create'
                else:
                    print('Ignoring message; wrong application source id')
                    return False, None
            except Exception as error:
                LOG.error('Unable load message. Error: %s', str(error))
                return False, None
        elif extract_from_header(msg.headers, 'event_type') == 'Application.destroy':
            try:
                value = json.loads(msg.value.decode('utf-8'))
                print('handle msg value: ', str(value))
                if int(value.get('application_type_id')) == application_source_id:
                    return True, 'destroy'
                else:
                    print('Ignoring message; wrong application source id')
                    return False, None
            except Exception as error:
                LOG.error('Unable load message. Error: %s', str(error))
                return False, None
        else:
            value = json.loads(msg.value.decode('utf-8'))
            print('Unhandled event type ', str(value))
            return False, None
    else:
        LOG.error('Unexpected Message')
    return False

async def sources_network_info(storage, source_id, auth_header):
    sources_network = SourcesHTTPClient(auth_header, source_id)
    source_details = sources_network.get_source_details()
    source_name = source_details.get('name')
    source_type_id = int(source_details.get('source_type_id'))

    if source_type_id == 1:
        source_type = 'OCP'
        authentication = source_details.get('uid')
    elif source_type_id == 2:
        source_type = 'AWS'
        authentication = sources_network.get_aws_role_arn()
    else:
        source_type = 'UNK'

    storage.add_provider_sources_network_info(source_id, source_name, source_type, authentication)

async def process_messages(msg_pending_queue, storage):  # pragma: no cover
    application_source_id = None
    while True:
        msg = await msg_pending_queue.get()
        if not application_source_id:
            fake_header = 'eyJpZGVudGl0eSI6IHsiYWNjb3VudF9udW1iZXIiOiAiMTIzNDUiLCAiaW50ZXJuYWwiOiB7Im9yZ19pZCI6ICI1NDMyMSJ9fX0='
            application_source_id = SourcesHTTPClient(fake_header).get_cost_management_application_type_id()

        valid_msg, operation = filter_message(msg, application_source_id)
        if valid_msg and operation == 'create':
            storage.create_provider_event(msg)
            auth_header = extract_from_header(msg.headers, 'x-rh-identity')
            value = json.loads(msg.value.decode('utf-8'))
            source_id = int(value.get('source_id'))
            await sources_network_info(storage, source_id, auth_header)
        elif valid_msg and operation == 'destroy':
            #TODO: Move koku remove to provider_builder thread
            koku_uuid = storage.destroy_provider_event(msg)
            value = json.loads(msg.value.decode('utf-8'))
            source_id = int(value.get('source_id'))
            auth_header = extract_from_header(msg.headers, 'x-rh-identity')
            koku_client = KokuHTTPClient(source_id, auth_header)
            koku_client.destroy_provider(koku_uuid)



async def listen_for_messages(consumer, msg_pending_queue):  # pragma: no cover
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
    Listener thread function to run the asyncio event loop.

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
