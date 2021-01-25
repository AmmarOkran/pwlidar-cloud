
import tempfile
from pwlidar_cloud.executor import FunctionExecutor
from pwlidar_cloud.version import __version__

name = "pwlidar_cloud"


def ibm_cf_executor(config=None, runtime=None, runtime_memory=None, workers=None, 
                    region=None, storage_backend=None, storage_backend_region=None, 
                    rabbitmq_monitor=None, remote_invoker=None, log_level=None):
    """
    Function executor for IBM Cloud Functions
    """
    compute_backend = 'ibm_cf'
    return FunctionExecutor(
        config=config, runtime=runtime, runtime_memory=runtime_memory,
        workers=workers, compute_backend=compute_backend,
        compute_backend_region=region, storage_backend=storage_backend,
        storage_backend_region=storage_backend_region,
        remote_invoker=remote_invoker, log_level=log_level
    )
