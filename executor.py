
import os
import copy
import time
import logging
from pwlidar_cloud.invoker import FunctionInvoker
from pwlidar_cloud.storage import InternalStorage
from pwlidar_cloud.utils import is_pywren_function, create_executor_id
from pwlidar_cloud.config import default_config, default_logging_config, EXECUTION_TIMEOUT, extract_storage_config
from pwlidar_cloud.job import create_map_job

logger = logging.getLogger(__name__)


class FunctionExecutor:

    class State:

        New = 'New'
    
    def __init__(self, config = None, runtime=None, runtime_memory=None,  compute_backend=None, 
                compute_backend_region=None, storage_backend=None, storage_backend_region=None, 
                workers=None, remote_invoker=None, log_level=None):

        self.start_time = time.time() 
        self._state = self.State.New
        self.is_pywren_function = is_pywren_function()
        
        # Log level Configuration
        self.log_level = log_level
        if not self.log_level:
            if(logger.getEffectiveLevel() != logging.WARNING):
                self.log_level = logging.getLevelName(logger.getEffectiveLevel())
        if self.log_level:
            os.environ["PYWREN_LOGLEVEL"] = self.log_level
            if not self.is_pywren_function:
                default_logging_config(self.log_level)
        
        # Overwrite pywren config parameters
        pw_config_ow = {}
        if runtime is not None:
            pw_config_ow['runtime'] = runtime
        if runtime_memory is not None:
            pw_config_ow['runtime_memory'] = runtime_memory
        if compute_backend is not None:
            pw_config_ow['compute_backend'] = compute_backend
        if compute_backend_region is not None:
            pw_config_ow['compute_backend_region'] = compute_backend_region
        if storage_backend is not None:
            pw_config_ow['storage_backend'] = storage_backend
        if storage_backend_region is not None:
            pw_config_ow['storage_backend_region'] = storage_backend_region
        if workers is not None:
            pw_config_ow['workers'] = workers
        
        self.config = default_config(copy.deepcopy(config), pw_config_ow)

        self.executor_id = create_executor_id()
        logger.debug('FunctionExecutor created with ID: {}'.format(self.executor_id))

        self.data_cleaner = self.config['pywren'].get('data_cleaner', True)
        
        storage_config = extract_storage_config(self.config)
        self.internal_storage = InternalStorage(storage_config)
        self.invoker = FunctionInvoker(self.config, self.executor_id, self.internal_storage)

        self.futures = []
        self.total_jobs = 0
        self.cleaned_jobs = set()


    def _create_job_id(self, call_type):
            job_id = str(self.total_jobs).zfill(3)
            self.total_jobs += 1
            return '{}{}'.format(call_type, job_id)


    def lidar_map(self, map_function, map_iterdata, extra_params=None, extra_env=None, runtime_memory=None,
                  chunk_size=None, chunk_n=None, timeout=EXECUTION_TIMEOUT, invoke_pool_threads=500,
                  include_modules=[], exclude_modules=[]):
            """
            :param map_function: the function to map over the data
            :param map_iterdata: An iterable of input data
            :param extra_params: Additional parameters to pass to the function activation. Default None.
            :param extra_env: Additional environment variables for action environment. Default None.
            :param runtime_memory: Memory to use to run the function. Default None (loaded from config).
            :param chunk_size: the size of the data chunks to split each object. 'None' for processing
                            the whole file in one function activation.
            :param chunk_n: Number of chunks to split each object. 'None' for processing the whole
                            file in one function activation.
            :param remote_invocation: Enable or disable remote_invocation mechanism. Default 'False'
            :param timeout: Time that the functions have to complete their execution before raising a timeout.
            :param invoke_pool_threads: Number of threads to use to invoke.
            :param include_modules: Explicitly pickle these dependencies.
            :param exclude_modules: Explicitly keep these modules from pickled dependencies.

            :return: A list with size `len(iterdata)` of futures.
            """
            job_id = self._create_job_id('M')

            runtime_meta = self.invoker.select_runtime(job_id, runtime_memory)

            job = create_map_job(self.config, self.internal_storage,
                                self.executor_id, job_id,
                                map_function=map_function,
                                iterdata=map_iterdata,
                                runtime_meta=runtime_meta,
                                runtime_memory=runtime_memory,
                                extra_params=extra_params,
                                extra_env=extra_env,
                                obj_chunk_size=chunk_size,
                                obj_chunk_number=chunk_n,
                                invoke_pool_threads=invoke_pool_threads,
                                include_modules=include_modules,
                                exclude_modules=exclude_modules,
                                execution_timeout=timeout)