
import os
import sys
import copy
import time
import json
import logging
import signal
import traceback
from pwlidar_cloud.invoker import FunctionInvoker
from pwlidar_cloud.storage import InternalStorage
from pwlidar_cloud.future import FunctionException
from pwlidar_cloud.storage.utils import clean_os_bucket
from pwlidar_cloud.wait import wait_storage, ALL_COMPLETED
from pwlidar_cloud.utils import is_pywren_function, create_executor_id, is_unix_system, is_notebook, timeout_handler
from pwlidar_cloud.config import default_config, default_logging_config, EXECUTION_TIMEOUT, extract_storage_config, JOBS_PREFIX
from pwlidar_cloud.job import create_map_job, create_reduce_job


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


    
    def lidar_call_async(self, func, data, extra_env=None, runtime_memory=None,
                         timeout=EXECUTION_TIMEOUT, include_modules=[], exclude_modules=[]):
        """
        For running one function execution asynchronously

        :param func: the function to map over the data
        :param data: input data
        :param extra_data: Additional data to pass to action. Default None.
        :param extra_env: Additional environment variables for action environment. Default None.
        :param runtime_memory: Memory to use to run the function. Default None (loaded from config).
        :param timeout: Time that the functions have to complete their execution before raising a timeout.
        :param include_modules: Explicitly pickle these dependencies.
        :param exclude_modules: Explicitly keep these modules from pickled dependencies.

        :return: future object.
        """
        job_id = self._create_job_id('A')

        runtime_meta = self.invoker.select_runtime(job_id, runtime_memory)

        job = create_map_job(self.config, self.internal_storage,
                             self.executor_id, job_id,
                             map_function=func,
                             iterdata=[data],
                             runtime_meta=runtime_meta,
                             runtime_memory=runtime_memory,
                             extra_env=extra_env,
                             include_modules=include_modules,
                             exclude_modules=exclude_modules,
                             execution_timeout=timeout)

        futures = self.invoker.run(job)
        self.futures.extend(futures)
        self._state = FunctionExecutor.State.Running

        return futures[0]


    def lidar_map(self, map_function, map_iterdata, extra_params=None, extra_env=None, runtime_memory=None,
                  partition_type = None, chunk_size=None, chunk_n=None, timeout=EXECUTION_TIMEOUT, invoke_pool_threads=500,
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
                                 partition_type = partition_type,
                                 extra_params=extra_params,
                                 extra_env=extra_env,
                                 obj_chunk_size=chunk_size,
                                 obj_chunk_number=chunk_n,
                                 invoke_pool_threads=invoke_pool_threads,
                                 include_modules=include_modules,
                                 exclude_modules=exclude_modules,
                                 execution_timeout=timeout)

            futures = self.invoker.run(job)
            self.futures.extend(futures)
            self._state = FunctionExecutor.State.Running
            if len(futures) == 1:
                return futures[0]
            return futures