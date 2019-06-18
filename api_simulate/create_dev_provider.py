import json
from aiokafka import AIOKafkaProducer
import asyncio

loop = asyncio.get_event_loop()

'''
➜  sources_client git:(setup) ✗ echo -n '{"identity":{"account_number":1001,"type":"User","user":{"username":"user_dev","email":"user_dev@foo.com","is_org_admin":true}},"entitlements"{"hybrid_cloud": {"is_entitled": true}}' | base64
eyJpZGVudGl0eSI6eyJhY2NvdW50X251bWJlciI6MTAwMSwidHlwZSI6IlVzZXIiLCJ1c2VyIjp7InVzZXJuYW1lIjoidXNlcl9kZXYiLCJlbWFpbCI6InVzZXJfZGV2QGZvby5jb20iLCJpc19vcmdfYWRtaW4iOnRydWV9fSwiZW50aXRsZW1lbnRzInsiaHlicmlkX2Nsb3VkIjogeyJpc19lbnRpdGxlZCI6IHRydWV9fQ==
➜  sources_client git:(setup) ✗
'''

async def send_one():
    producer = AIOKafkaProducer(
        loop=loop, bootstrap_servers='localhost:29092')
    # Get cluster layout and initial topic/partition leadership information
    await producer.start()
    try:
        # Produce message
        message_contents = "{\"id\":16,\"name\":\"AWS\",\"uid\":\"9688fc8c-4922-4164-8eed-907c2fd6e0a5\",\"created_at\":\"2019-06-18 18:09:14 UTC\",\"updated_at\":\"2019-06-18 18:09:14 UTC\",\"source_type_id\":2,\"version\":null,\"tenant\":\"1001\",\"application_type_id\":2}"
        message_headers = [("event_type", b"Source.create"),
                           ("x-rh-identity", b"eyJpZGVudGl0eSI6eyJhY2NvdW50X251bWJlciI6MTAwMSwidHlwZSI6IlVzZXIiLCJ1c2VyIjp7InVzZXJuYW1lIjoidXNlcl9kZXYiLCJlbWFpbCI6InVzZXJfZGV2QGZvby5jb20iLCJpc19vcmdfYWRtaW4iOnRydWV9fSwiZW50aXRsZW1lbnRzInsiaHlicmlkX2Nsb3VkIjogeyJpc19lbnRpdGxlZCI6IHRydWV9fQ=="),
                           ("encoding", b"json")]
        msg = bytes(message_contents.encode('utf-8'))
        await producer.send("platform.sources.event-stream", msg, headers=message_headers)
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()

if __name__ == "__main__":
    loop.run_until_complete(send_one())

