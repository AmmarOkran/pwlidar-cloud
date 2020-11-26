import os
import sys
import base64
import logging
import zipfile
import textwrap
import pywren_ibm_cloud
from . import config as ibmcf_config
from datetime import datetime, timezone
from ibm_botocore.credentials import DefaultTokenManager
from pywren_ibm_cloud.utils import version_str
from pywren_ibm_cloud.version import __version__
from pywren_ibm_cloud.utils import is_pywren_function
from pywren_ibm_cloud.config import CACHE_DIR, load_yaml_config, dump_yaml_config
from pywren_ibm_cloud.libs.openwhisk.client import OpenWhiskClient

logger = logging.getLogger(__name__)


class IBMCloudFunctionsBackend:
    """
    A wrap-up around IBM Cloud Functions backend.
    """

    def __init__(self, ibm_cf_config):
        logger.debug("Creating IBM Cloud Functions client")
        self.log_level = os.getenv('PYWREN_LOGLEVEL')
        self.name = 'ibm_cf'
        self.ibm_cf_config = ibm_cf_config
        self.is_pywren_function = is_pywren_function()

        self.user_agent = ibm_cf_config['user_agent']
        self.region = ibm_cf_config['region']
        self.endpoint = ibm_cf_config['regions'][self.region]['endpoint']
        self.namespace = ibm_cf_config['regions'][self.region]['namespace']
        self.namespace_id = ibm_cf_config['regions'][self.region].get('namespace_id', None)
        self.api_key = ibm_cf_config['regions'][self.region].get('api_key', None)
        self.iam_api_key = ibm_cf_config.get('iam_api_key', None)

        logger.info("Set IBM CF Namespace to {}".format(self.namespace))
        logger.info("Set IBM CF Endpoint to {}".format(self.endpoint))

        self.user_key = self.api_key[:5] if self.api_key else self.iam_api_key[:5]
        self.package = 'pywren_v{}_{}'.format(__version__, self.user_key)

        if self.api_key:
            enc_api_key = str.encode(self.api_key)
            auth_token = base64.encodebytes(enc_api_key).replace(b'\n', b'')
            auth = 'Basic %s' % auth_token.decode('UTF-8')

            self.cf_client = OpenWhiskClient(endpoint=self.endpoint,
                                             namespace=self.namespace,
                                             auth=auth,
                                             user_agent=self.user_agent)
        elif self.iam_api_key:
            token_manager = DefaultTokenManager(api_key_id=self.iam_api_key)
            token_filename = os.path.join(CACHE_DIR, 'ibm_cf', 'iam_token')

            if 'token' in self.ibm_cf_config:
                logger.debug("Using IBM IAM API Key - Reusing Token from config")
                token_manager._token = self.ibm_cf_config['token']
                token_manager._expiry_time = datetime.strptime(self.ibm_cf_config['token_expiry_time'],
                                                               '%Y-%m-%d %H:%M:%S.%f%z')
                token_minutes_diff = int((token_manager._expiry_time - datetime.now(timezone.utc)).total_seconds() / 60.0)
                logger.debug("Token expiry time: {} - Minutes left: {}".format(token_manager._expiry_time, token_minutes_diff))

            elif os.path.exists(token_filename):
                logger.debug("Using IBM IAM API Key - Reusing Token from local cache")
                token_data = load_yaml_config(token_filename)
                token_manager._token = token_data['token']
                token_manager._expiry_time = datetime.strptime(token_data['token_expiry_time'],
                                                               '%Y-%m-%d %H:%M:%S.%f%z')
                token_minutes_diff = int((token_manager._expiry_time - datetime.now(timezone.utc)).total_seconds() / 60.0)
                logger.debug("Token expiry time: {} - Minutes left: {}".format(token_manager._expiry_time, token_minutes_diff))

            if (token_manager._is_expired() or token_minutes_diff < 11) and not is_pywren_function():
                logger.debug("Using IBM IAM API Key - Token expired. Requesting new token")
                token_manager._token = None
                token_manager.get_token()
                token_data = {}
                token_data['token'] = token_manager._token
                token_data['token_expiry_time'] = token_manager._expiry_time.strftime('%Y-%m-%d %H:%M:%S.%f%z')
                dump_yaml_config(token_filename, token_data)

            ibm_cf_config['token'] = token_manager._token
            ibm_cf_config['token_expiry_time'] = token_manager._expiry_time.strftime('%Y-%m-%d %H:%M:%S.%f%z')

            auth_token = token_manager._token
            auth = 'Bearer ' + auth_token

            self.cf_client = OpenWhiskClient(endpoint=self.endpoint,
                                             namespace=self.namespace_id,
                                             auth=auth,
                                             user_agent=self.user_agent)

        log_msg = ('PyWren v{} init for IBM Cloud Functions - Namespace: {} - '
                   'Region: {}'.format(__version__, self.namespace, self.region))
        if not self.log_level:
            print(log_msg)
        logger.info("IBM CF client created successfully")

    def _format_action_name(self, runtime_name, runtime_memory):
        runtime_name = runtime_name.replace('/', '_').replace(':', '_')
        return '{}_{}MB'.format(runtime_name, runtime_memory)

    def _unformat_action_name(self, action_name):
        runtime_name, memory = action_name.rsplit('_', 1)
        image_name = runtime_name.replace('_', '/', 1)
        image_name = image_name.replace('_', ':', -1)
        return image_name, int(memory.replace('MB', ''))

    def _get_default_runtime_image_name(self):
        this_version_str = version_str(sys.version_info)
        if this_version_str == '3.5':
            image_name = ibmcf_config.RUNTIME_DEFAULT_35
        elif this_version_str == '3.6':
            image_name = ibmcf_config.RUNTIME_DEFAULT_36
        elif this_version_str == '3.7':
            image_name = ibmcf_config.RUNTIME_DEFAULT_37
        return image_name

    def _create_function_handler_zip(self):
        logger.debug("Creating function handler zip in {}".format(ibmcf_config.FH_ZIP_LOCATION))

        def add_folder_to_zip(zip_file, full_dir_path, sub_dir=''):
            for file in os.listdir(full_dir_path):
                full_path = os.path.join(full_dir_path, file)
                if os.path.isfile(full_path):
                    zip_file.write(full_path, os.path.join('pywren_ibm_cloud', sub_dir, file))
                elif os.path.isdir(full_path) and '__pycache__' not in full_path:
                    add_folder_to_zip(zip_file, full_path, os.path.join(sub_dir, file))

        try:
            with zipfile.ZipFile(ibmcf_config.FH_ZIP_LOCATION, 'w', zipfile.ZIP_DEFLATED) as ibmcf_pywren_zip:
                current_location = os.path.dirname(os.path.abspath(__file__))
                module_location = os.path.dirname(os.path.abspath(pywren_ibm_cloud.__file__))
                main_file = os.path.join(current_location, 'entry_point.py')
                ibmcf_pywren_zip.write(main_file, '__main__.py')
                add_folder_to_zip(ibmcf_pywren_zip, module_location)
        except Exception as e:
            raise Exception('Unable to create the {} package: {}'.format(ibmcf_config.FH_ZIP_LOCATION, e))

    def build_runtime(self, docker_image_name, dockerfile):
        """
        Builds a new runtime from a Docker file and pushes it to the Docker hub
        """
        logger.info('Building a new docker image from Dockerfile')
        logger.info('Docker image name: {}'.format(docker_image_name))

        if dockerfile:
            cmd = 'docker build -t {} -f {} .'.format(docker_image_name, dockerfile)
        else:
            cmd = 'docker build -t {} .'.format(docker_image_name)

        res = os.system(cmd)
        if res != 0:
            exit()

        cmd = 'docker push {}'.format(docker_image_name)
        res = os.system(cmd)
        if res != 0:
            exit()

    def create_runtime(self, docker_image_name, memory, timeout):
        """
        Creates a new runtime into IBM CF namespace from an already built Docker image
        """
        if docker_image_name == 'default':
            docker_image_name = self._get_default_runtime_image_name()

        runtime_meta = self._generate_runtime_meta(docker_image_name)

        logger.info('Creating new PyWren runtime based on Docker image {}'.format(docker_image_name))

        self.cf_client.create_package(self.package)
        action_name = self._format_action_name(docker_image_name, memory)

        self._create_function_handler_zip()

        with open(ibmcf_config.FH_ZIP_LOCATION, "rb") as action_zip:
            action_bin = action_zip.read()
        self.cf_client.create_action(self.package, action_name, docker_image_name, code=action_bin,
                                     memory=memory, is_binary=True, timeout=timeout*1000)
        return runtime_meta

    def delete_runtime(self, docker_image_name, memory):
        """
        Deletes a runtime
        """
        if docker_image_name == 'default':
            docker_image_name = self._get_default_runtime_image_name()
        action_name = self._format_action_name(docker_image_name, memory)
        self.cf_client.delete_action(self.package, action_name)

    def delete_all_runtimes(self):
        """
        Deletes all runtimes from all packages
        """
        packages = self.cf_client.list_packages()
        for pkg in packages:
            if (pkg['name'].startswith('pywren') and pkg['name'].endswith(self.user_key)) or \
               (pkg['name'].startswith('pywren') and pkg['name'].count('_') == 1):
                actions = self.cf_client.list_actions(pkg['name'])
                while actions:
                    for action in actions:
                        self.cf_client.delete_action(pkg['name'], action['name'])
                    actions = self.cf_client.list_actions(pkg['name'])
                self.cf_client.delete_package(pkg['name'])

    def list_runtimes(self, docker_image_name='all'):
        """
        List all the runtimes deployed in the IBM CF service
        return: list of tuples (docker_image_name, memory)
        """
        if docker_image_name == 'default':
            docker_image_name = self._get_default_runtime_image_name()
        runtimes = []
        actions = self.cf_client.list_actions(self.package)

        for action in actions:
            action_image_name, memory = self._unformat_action_name(action['name'])
            if docker_image_name == action_image_name or docker_image_name == 'all':
                runtimes.append((action_image_name, memory))
        return runtimes

    def invoke(self, docker_image_name, runtime_memory, payload):
        """
        Invoke -- return information about this invocation
        """
        action_name = self._format_action_name(docker_image_name, runtime_memory)

        activation_id = self.cf_client.invoke(package=self.package,
                                              action_name=action_name,
                                              payload=payload,
                                              is_ow_action=self.is_pywren_function)

        return activation_id

    def get_runtime_key(self, docker_image_name, runtime_memory):
        """
        Method that creates and returns the runtime key.
        Runtime keys are used to uniquely identify runtimes within the storage,
        in order to know which runtimes are installed and which not.
        """
        action_name = self._format_action_name(docker_image_name, runtime_memory)
        runtime_key = os.path.join(self.name, self.region, self.namespace, action_name)

        return runtime_key

    def _generate_runtime_meta(self, docker_image_name):
        """
        Extract installed Python modules from docker image
        """
        action_code = """
            import sys
            import pkgutil

            def main(args):
                print("Extracting preinstalled Python modules...")
                runtime_meta = dict()
                mods = list(pkgutil.iter_modules())
                runtime_meta["preinstalls"] = [entry for entry in sorted([[mod, is_pkg] for _, mod, is_pkg in mods])]
                python_version = sys.version_info
                runtime_meta["python_ver"] = str(python_version[0])+"."+str(python_version[1])
                print("Done!")
                return runtime_meta
            """

        runtime_memory = 128
        # old_stdout = sys.stdout
        # sys.stdout = open(os.devnull, 'w')
        action_name = self._format_action_name(docker_image_name, runtime_memory)
        self.cf_client.create_package(self.package)
        self.cf_client.create_action(self.package, action_name, docker_image_name,
                                     is_binary=False, code=textwrap.dedent(action_code),
                                     memory=runtime_memory, timeout=30000)
        # sys.stdout = old_stdout
        logger.debug("Extracting Python modules list from: {}".format(docker_image_name))

        try:
            retry_invoke = True
            while retry_invoke:
                retry_invoke = False
                runtime_meta = self.cf_client.invoke_with_result(self.package, action_name)
                if 'activationId' in runtime_meta:
                    retry_invoke = True
        except Exception:
            raise("Unable to invoke 'modules' action")
        try:
            self.delete_runtime(docker_image_name, runtime_memory)
        except Exception:
            raise Exception("Unable to delete 'modules' action")

        if not runtime_meta or 'preinstalls' not in runtime_meta:
            raise Exception(runtime_meta)

        return runtime_meta
