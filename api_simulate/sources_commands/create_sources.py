import argparse
import sys
import json
import requests

def create_parser():
    """Create the parser for incoming data."""
    parser = argparse.ArgumentParser()
    provider_group = parser.add_mutually_exclusive_group(required=True)
    app_create_group = parser.add_mutually_exclusive_group(required=False)
    parser.add_argument('--name',
                        dest='source_name',
                        required=False,
                        help='Source Name')
    parser.add_argument('--role_arn',
                        dest='role_arn',
                        required=False,
                        help='AWS roleARN')
    parser.add_argument('--s3_bucket',
                        dest='s3_bucket',
                        required=False,
                        help='AWS S3 bucket with cost and usage report')
    app_create_group.add_argument('--create_application',
                        dest='create_application',
                        action='store_true',
                        required=False,
                        help='Attach Cost Management application to source.')
    app_create_group.add_argument('--app_create_source_id',
                        dest='app_create_source_id',
                        required=False,
                        help='Source ID for Cost Management application creation')
    provider_group.add_argument('--aws',
                                dest='aws',
                                action='store_true',
                                help='Create an AWS source.')
    provider_group.add_argument('--ocp',
                                dest='ocp',
                                action='store_true',
                                help='Create an OCP source.')

    return parser

class SourcesClientDataGenerator:
    def __init__(self, auth_header):
        self._sources_client_host = 'http://localhost:8888'
        self._base_url = '{}/{}'.format(self._sources_client_host, '')

        header = {}
        header['x-rh-identity'] = auth_header
        self._identity_header = header

    def create_s3_bucket(self, source_id, billing_source):
        json_data = {}
        json_data['source_id'] = str(source_id)
        json_data['billing_source'] = str(billing_source)

        url = '{}{}'.format(self._base_url, 'billing_source')
        response = requests.post(url, headers=self._identity_header, json=json_data)
        return response

class SourcesDataGenerator:
    def __init__(self, auth_header):
        self._sources_host = 'http://localhost:3000'
        self._base_url = '{}/{}'.format(self._sources_host, '/api/v1.0')

        header = {}
        header['x-rh-identity'] = auth_header
        self._identity_header = header

    def create_source(self, source_name, source_type):
        type_map = {'aws': '2', 'ocp': '1'}
        json_data = {}
        json_data['source_type_id'] = type_map.get(source_type)
        json_data['name'] = source_name

        url = '{}/{}'.format(self._base_url, 'sources')
        r = requests.post(url, headers=self._identity_header, json=json_data)
        response = r.json()
        return response.get('id')

    def create_endpoint(self, source_id):
        json_data = {}
        json_data['host'] = 'www.example.com'
        json_data['path'] = '/api/v1'
        json_data['source_id'] = str(source_id)

        url = '{}/{}'.format(self._base_url, 'endpoints')
        r = requests.post(url, headers=self._identity_header, json=json_data)
        response = r.json()
        return response.get('id')

    def create_aws_authentication(self, resource_id, username, password):
        json_data = {}
        json_data['authtype'] = 'aws_default'
        json_data['name'] = 'AWS default'
        json_data['password'] = str(password)
        json_data['status'] = 'valid'
        json_data['status_details'] = 'Details Here'
        json_data['username'] = 'username'
        json_data['resource_type'] = 'Endpoint'
        json_data['resource_id'] = str(resource_id)

        url = '{}/{}'.format(self._base_url, 'authentications')
        r = requests.post(url, headers=self._identity_header, json=json_data)
        response = r.json()
        return response.get('id')

    def create_application(self, source_id, source_type):
        type_map = {'catalog': '1', 'cost_management': '2', 'topo_inv': '3'}
        json_data = {}
        json_data['source_id'] = str(source_id)
        json_data['application_type_id'] = type_map.get(source_type)

        url = '{}/{}'.format(self._base_url, 'applications')
        r = requests.post(url, headers=self._identity_header, json=json_data)
        response = r.json()
        return response.get('id')

def main(args):
    parser = create_parser()
    args = parser.parse_args()
    parameters = vars(args)

    create_application = parameters.get('create_application')
    app_create_source_id = parameters.get('app_create_source_id')
    identity_header = 'eyJpZGVudGl0eSI6IHsiYWNjb3VudF9udW1iZXIiOiAiMTIzNDUiLCAiaW50ZXJuYWwiOiB7Im9yZ19pZCI6ICI1NDMyMSJ9fX0='
    generator = SourcesDataGenerator(identity_header)
    source_name = parameters.get('source_name')

    if parameters.get('aws'):
        role_arn = parameters.get('role_arn')
        s3_bucket = parameters.get('s3_bucket')

        if app_create_source_id:
            application_id = generator.create_application(app_create_source_id, 'cost_management')
            log_msg = 'Attached Cost Management Application ID {} to Source ID {}'.format(application_id, app_create_source_id)
            print(log_msg)
            return

        print('Creating AWS Source')
        source_id = generator.create_source(source_name, 'aws')
        endpoint_id = generator.create_endpoint(source_id)
        authentications_id = generator.create_aws_authentication(endpoint_id, 'user@example.com', role_arn)

        log_msg = 'AWS Provider Setup Successfully\n\tSource ID: {}\n\tEndpoint ID: {}\n\tAuthentication ID: {}'.format(source_id, endpoint_id, authentications_id)
        print(log_msg)

        if s3_bucket:
            sources_client = SourcesClientDataGenerator(identity_header)
            billing_source_response = sources_client.create_s3_bucket(source_id, s3_bucket)
            log_msg = 'Associating S3 bucket: {}'.format(billing_source_response)
            print(log_msg)
    
        if create_application:
            application_id = generator.create_application(source_id, 'cost_management')
            log_msg = 'Attached Cost Management Application ID {} to Source ID {}'.format(application_id, source_id)
            print(log_msg)
    elif parameters.get('ocp'):
        print('Creating OCP Source')
        source_id = generator.create_source(source_name, 'ocp')
        if create_application:
            application_id = generator.create_application(source_id, 'cost_management')
            log_msg = 'Attached Cost Management Application ID {} to Source ID {}'.format(application_id, source_id)
            print(log_msg)

if '__main__' in __name__:
    main(sys.argv[1:])
