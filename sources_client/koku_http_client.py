import json
import requests

class KokuHTTPClient:
    def __init__(self, source_id, auth_header):
        self._source_id = source_id
        self._base_url = 'http://localhost:8000/r/insights/platform/cost-management/v1'
        header = {}
        header['x-rh-identity'] = auth_header
        self._identity_header = header
    
    def create_provider(self, name, provider_type, authentication, billing_source):
        url = '{}/{}/'.format(self._base_url, 'providers')
        json_data = {}
        json_data["name"] = name
        json_data["type"] = provider_type

        provider_resource_name = {}
        provider_resource_name["provider_resource_name"] = authentication
        json_data["authentication"] = provider_resource_name

        bucket = {}
        bucket["bucket"] = billing_source if billing_source else ''
        json_data["billing_source"] = bucket 
        print(str(json_data))
        r = requests.post(url, headers=self._identity_header, json=json_data)
        response = r.json()
        print(str(response))
        return response

    def destroy_provider(self, provider_uuid):
        url = '{}/{}/{}/'.format(self._base_url, 'providers', provider_uuid)
        requests.delete(url, headers=self._identity_header)
