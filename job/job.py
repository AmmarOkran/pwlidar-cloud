import os
import time
import pickle
import logging
import inspect
from pwlidar_cloud import utils
from pwlidar_cloud.wait import wait_storage
from pwlidar_cloud.job.tiler import create_tiles
from pwlidar_cloud.storage.utils import create_func_key, create_agg_data_key
from pwlidar_cloud.job.serialize import SerializeIndependent, create_module_data
from pwlidar_cloud.config import EXECUTION_TIMEOUT, MAX_AGG_DATA_SIZE, JOBS_PREFIX

logger = logging.getLogger(__name__)

def create_map_job(config, internal_storage, executor_id, job_id, map_function, iterdata, runtime_meta,
                   runtime_memory=None, extra_params=None, extra_env=None, obj_chunk_size=None,
                   obj_chunk_number=None, invoke_pool_threads=128, include_modules=[], exclude_modules=[],
                   execution_timeout=EXECUTION_TIMEOUT):
    """
    Wrapper to create a map job.  It integrates COS logic to process objects.
    """
    map_func = map_function
    map_iterdata = utils.verify_args(map_function, iterdata, extra_params)
    new_invoke_pool_threads = invoke_pool_threads
    new_runtime_memory = runtime_memory

    if config['pywren'].get('rabbitmq_monitor', False):
        rabbit_amqp_url = config['rabbitmq'].get('amqp_url')
        utils.create_rabbitmq_resources(rabbit_amqp_url, executor_id, job_id)
    
    # Object processing functionality
    parts_per_object = None
    if utils.is_object_processing_function(map_function):
        # If it is object processing function, create partitions according chunk_size or chunk_number
        logger.debug('ExecutorID {} | JobID {} - Calling map on partitions from object storage flow'.format(executor_id, job_id))
        map_iterdata, parts_per_object = create_tiles(config, map_iterdata, obj_chunk_size, obj_chunk_number)
    # ########

def create_reduce_job():
    print("create_reduce_job")