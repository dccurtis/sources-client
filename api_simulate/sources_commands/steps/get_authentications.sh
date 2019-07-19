#!/bin/bash
curl -H "x-rh-identity: eyJpZGVudGl0eSI6IHsiYWNjb3VudF9udW1iZXIiOiAiMTIzNDUiLCAiaW50ZXJuYWwiOiB7Im9yZ19pZCI6ICI1NDMyMSJ9fX0=" "http://localhost:3000/internal/v1.0/authentications/2?expose_encrypted_attribute[]=password" 2>/dev/null | python -m json.tool
