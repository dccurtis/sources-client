#/bin/bash
curl -X PATCH -H "x-rh-identity: eyJpZGVudGl0eSI6IHsiYWNjb3VudF9udW1iZXIiOiAiMTIzNDUiLCAiaW50ZXJuYWwiOiB7Im9yZ19pZCI6ICI1NDMyMSJ9fX0=" http://localhost:3000/api/v1.0/sources/1 -d "{\"name\": \"Test Amazon Source 2\"}"
