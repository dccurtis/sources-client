import asyncio
import threading

from koku_http_client import KokuHTTPClient

async def process_in_progress_objects(in_progress_queue, storage):
    print('Waiting for messages')
    while True:
        msg = await in_progress_queue.get()
        print(str(msg.source_id))
        # storage.create_provider(msg.source_id, msg.name, msg.auth_header)
        koku_client = KokuHTTPClient(msg.source_id, msg.auth_header)
        koku_details = koku_client.create_provider(msg.name, msg.source_type, msg.authentication, msg.billing_source)
        storage.add_provider_koku_uuid(msg.source_id, koku_details.get('uuid'))

async def load_pending_items(storage, in_progress_queue, pending_events):
    while True:
        await asyncio.sleep(1)
        pending_events = storage.load_providers_to_create()
        for event in pending_events:
            await in_progress_queue.put(event)

def provider_builder_thread(storage):
    event_loop = asyncio.new_event_loop()
    in_progress_queue = asyncio.Queue(loop=event_loop)

    pending_events = storage.load_providers_to_create()
    event_loop.create_task(load_pending_items(storage, in_progress_queue, pending_events))

    event_loop.run_until_complete(process_in_progress_objects(in_progress_queue, storage))
    print('builder event loop complete')

def initialize_provider_builder(storage):
    event_loop_thread = threading.Thread(target=provider_builder_thread, args=(storage,))
    event_loop_thread.daemon = True
    event_loop_thread.start()
    print('Starting Provider Builder Event Loop')
