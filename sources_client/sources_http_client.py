import json
import requests

class SourcesHTTPClient:
    def __init__(self, source_id, auth_header):
        self._source_id = source_id
        self._base_url = 'http://localhost:3000/api/v1.0/'
        header = {}
        header['x-rh-identity'] = auth_header
        self._identity_header = header
    
    def get_source_details(self):
        url = '{}/{}/{}'.format(self._base_url, 'sources', str(self._source_id))
        r = requests.get(url, headers=self._identity_header)
        response = r.json()
        return response
