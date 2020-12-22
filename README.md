<!-- # Pwlidar-cloud
Library for processing LiDAR data in the ibm cloud -->

<h1><p align="center"> Pwlidar-cloud </p></h1>

### What is Pwlidar-cloud
[Pwlidar-cloud](https://github.com/AmmarAkran/pwlidar-cloud) is a library for processing LiDAR data in the ibm cloud.


## Initial Requirements
* IBM Cloud Functions account, as described [here](https://cloud.ibm.com/openwhisk/). 
* IBM Cloud Object Storage [account](https://www.ibm.com/cloud/object-storage)
* Python 3.5, Python 3.6 or Python 3.7


## How to use Pwlidar_cloud on IBM Cloud
The primary object in PyWren is the executor. First of all we have to import Pwlidar_cloud, and call on of the available methods to get a ready-to-use executor. The available executor up_to_now is: `ibm_cf_executo()`. For example:

```python
import Pwlidar_cloud as pywren
pw = pywren.ibm_cf_executor()
```

As a simple example, you can copy-paste the next code and run the `my_map_function()` function on IBM Cloud Functions:

```python
import pwlidar_cloud as pywren

def my_map_function(obj):
    print('Bucket: {}'.format(obj.bucket))
    print('Key: {}'.format(obj.key))
    print('Partition num: {}'.format(obj.part))



if __name__ == "__main__":
    fname = "file_name.las"
    bukname = "bucket_name"
    iterdata = 'cos://' + bukname + '/' + fname
    pw = pywren.ibm_cf_executor()
    pw.lidar_map(my_map_function, iterdata, chunk_n = 2) 
    print(pw.get_result())
```

