#
# (C) Copyright IBM Corp. 2018
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import json
import logging
import requests
from ..exceptions import StorageNoSuchKeyError
from ...utils import sizeof_fmt


logger = logging.getLogger(__name__)


class SwiftStorageBackend:
    """
    A wrap-up around OpenStack Swift APIs.
    """

    def __init__(self, swift_config):
        self.auth_url = swift_config['swift_auth_url']
        self.user_id = swift_config['swift_user_id']
        self.project_id = swift_config['swift_project_id']
        self.password = swift_config['swift_password']
        self.region = swift_config['swift_region']
        self.endpoint = None

        if 'token' in swift_config:
            self.token = swift_config['token']
            self.endpoint = swift_config['endpoint']
        else:
            self.token = self.generate_swift_token()
            swift_config['token'] = self.token
            swift_config['endpoint'] = self.endpoint

        self.session = requests.session()
        self.session.headers.update({'X-Auth-Token': self.token})
        adapter = requests.adapters.HTTPAdapter(pool_maxsize=64, max_retries=3)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

    def generate_swift_token(self):
        """
        Generates new token for accessing to Swift.
        :return: token
        """
        url = self.auth_url+"/v3/auth/tokens"
        headers = {'Content-Type': 'application/json'}
        data = {"auth":{"identity":{"methods":["password"],"password":{"user":{"id":self.user_id,"password":self.password}}},"scope":{"project":{"id":self.project_id}}}}
        json_data = json.dumps(data)

        r = requests.post(url, data=json_data, headers=headers)

        if r.status_code == 201:
            backend_info = json.loads(r.text)

            for service in backend_info['token']['catalog']:
                if service['name'] == 'swift':
                    for endpoint in service['endpoints']:
                        if endpoint['region'] == self.region:
                            if endpoint['interface'] == 'public':
                                self.endpoint = endpoint['url'].replace('https:', 'http:')

            if not self.endpoint:
                raise Exception('Invalid region name')

            return r.headers['X-Subject-Token']
        else:
            message = json.loads(r.text)['error']['message']
            raise Exception("{} - {} - {}".format(r.status_code, r.reason, message))

    def put_object(self, container_name, key, data):
        """
        Put an object in Swift. Override the object if the key already exists.
        :param key: key of the object.
        :param data: data of the object
        :type data: str/bytes
        :return: None
        """
        url = '/'.join([self.endpoint, container_name, key])
        try:
            res = self.session.put(url, data=data)
            status = 'OK' if res.status_code == 201 else 'Error'
            try:
                logger.debug('PUT Object {} - Size: {} - {}'.format(key, sizeof_fmt(len(data)), status))
            except Exception:
                logger.debug('PUT Object {} - {}'.format(key, status))
        except Exception as e:
            print(e)

    def get_object(self, container_name, key, stream=False, extra_get_args={}):
        """
        Get object from Swift with a key. Throws StorageNoSuchKeyError if the given key does not exist.
        :param key: key of the object
        :return: Data of the object
        :rtype: str/bytes
        """
        if not container_name:
            container_name = self.storage_container
        url = '/'.join([self.endpoint, container_name, key])
        headers = {'X-Auth-Token': self.token}
        headers.update(extra_get_args)
        try:
            res = self.session.get(url, headers=headers, stream=stream)
            if res.status_code == 200 or res.status_code == 206:
                if stream:
                    data = res.raw
                else:
                    data = res.content
                return data
            elif res.status_code == 404:
                raise StorageNoSuchKeyError(key)
            else:
                raise Exception('{} - {}'.format(res.status_code, key))
        except StorageNoSuchKeyError:
            raise StorageNoSuchKeyError(key)
        except Exception as e:
            print(e)
            raise StorageNoSuchKeyError(key)

    def head_object(self, container_name, key):
        """
        Head object from Swift with a key. Throws StorageNoSuchKeyError if the given key does not exist.
        :param key: key of the object
        :return: Data of the object
        :rtype: str/bytes
        """
        url = '/'.join([self.endpoint, container_name, key])
        try:
            res = self.session.head(url)
            if res.status_code == 200:
                return res.headers
            elif res.status_code == 404:
                raise StorageNoSuchKeyError(key)
            else:
                raise Exception('{} - {}'.format(res.status_code, key))
        except Exception as e:
            raise StorageNoSuchKeyError(key)

    def delete_object(self, container_name, key):
        """
        Delete an object from Swift.
        :param bucket: bucket name
        :param key: data key
        """
        url = '/'.join([self.endpoint, container_name, key])
        return self.session.delete(url)

    def delete_objects(self, container_name, key_list):
        """
        Delete a list of objects from Swift.
        :param bucket: bucket name
        :param key: data key
        """
        headers={'X-Auth-Token': self.token,
                 'X-Bulk-Delete': 'True'}

        keys_to_delete = []
        for key in key_list:
            keys_to_delete.append('/{}/{}'.format(container_name, key))

        keys_to_delete = '\n'.join(keys_to_delete)
        url = '/'.join([self.endpoint, '?bulk-delete'])
        return self.session.delete(url, data=keys_to_delete, headers=headers)

    def bucket_exists(self, container_name):
        """
        Head container from Swift with a name. Throws StorageNoSuchKeyError if the given container does not exist.
        :param container_name: name of the container
        :return: Data of the bucket
        :rtype: str/bytes
        """
        url = '/'.join([self.endpoint, container_name])
        try:
            res = self.session.head(url)
            if res.status_code == 204:
                return res.headers
            elif res.status_code == 404:
                raise StorageNoSuchKeyError(container_name)
            else:
                raise Exception('{} - {}'.format(res.status_code))
        except Exception as e:
            raise StorageNoSuchKeyError(container_name)

    def list_objects(self, container_name, prefix=''):
        """
        Lists the objects in a bucket. Throws StorageNoSuchKeyError if the given bucket does not exist.
        :param key: key of the object
        :return: Data of the object
        :rtype: str/bytes
        """
        if prefix:
            url = '/'.join([self.endpoint, container_name, '?format=json&prefix='+prefix])
        else:
            url = '/'.join([self.endpoint, container_name, '?format=json'])
        try:
            res = self.session.get(url)
            objects = res.json()

            # TODO: Adapt to Key and Size
            return objects
        except Exception as e:
            raise e

    def list_keys_with_prefix(self, container_name, prefix):
        """
        Return a list of keys for the given prefix.
        :param prefix: Prefix to filter object names.
        :return: List of keys in bucket that match the given prefix.
        :rtype: list of str
        """
        try:
            objects = self.list_objects(container_name, prefix)
            object_keys = [r['name'] for r in objects]
            return object_keys
        except Exception as e:
            raise(e)
