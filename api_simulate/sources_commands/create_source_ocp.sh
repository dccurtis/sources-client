#/bin/bash
curl -X POST -H "x-rh-identity: eyJpZGVudGl0eSI6IHsiYWNjb3VudF9udW1iZXIiOiAiMTIzNDUiLCAiaW50ZXJuYWwiOiB7Im9yZ19pZCI6ICI1NDMyMSJ9fX0=" http://localhost:3000/api/v1.0/sources -d "{\"source_type_id\": \"1\", \"name\": \"Test OpenShift Source\"}"
