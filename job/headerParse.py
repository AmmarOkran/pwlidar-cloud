import ibm_boto3
import ibm_botocore
from ibm_botocore.client import Config
import os
import struct

cos_config = { 
            'bucket_name' : '',
            'api_key' : '',
            'service_endpoint' : ''
             }

api_key = cos_config.get('api_key')
service_instance_id = cos_config.get('service_instance_id')
auth_endpoint = cos_config.get('auth_endpoint')
service_endpoint = cos_config.get('service_endpoint')
cos_client = ibm_boto3.client('s3',
                              ibm_api_key_id=api_key,
                              ibm_service_instance_id=service_instance_id, 
                              ibm_auth_endpoint=auth_endpoint,
                              config=Config(signature_version='oauth'),
                              endpoint_url=service_endpoint)
    
def parse_header(data):
    # rng = {'Range': "bytes=" + str(0) + "-" + str(227)}
    # r = cos_client.get_object(Bucket = bucket, Key=key, **rng)
    # data = r['Body'].read()

    # rng = {'Range': "bytes=" + str(96) + "-" + str(100)}
    # r = cos_client.get_object(Bucket = bucket, Key=key, **rng)
    # data = r['Body'].read()
    # header_offset= struct.unpack('<L', data[0:4])[0]

    # rng['Range'] = "bytes=" + str(0) + "-" + str(header_offset)
    # r = cos_client.get_object(Bucket = bucket, Key=key, **rng)
    # data = r['Body'].read()


    file_header = dict()
    file_header['FileSignature'] = data[:4].decode('utf-8')
    file_header['FileSourceID'] = struct.unpack('<H', data[4:6])[0]
    file_header['GlobalEncoding'] = struct.unpack('<H', data[6:8])[0]
    file_header['ProjectID_GUIDdata1'] = struct.unpack('<L', data[8:12])[0]
    file_header['ProjectID_GUIDdata2'] = struct.unpack('<H', data[12:14])[0]
    file_header['ProjectID_GUIDdata3'] = struct.unpack('<H', data[14:16])[0]
    file_header['ProjectID_GUIDdata4'] = [struct.unpack('<B', data[i:i+1])[0] for i in range(16, 24)]
    file_header['VersionMajor'] = struct.unpack('<B', data[24:25])[0]
    file_header['VersionMinor'] = struct.unpack('<B', data[25:26])[0]
    file_header['SystemIdentifier'] = struct.unpack('<32s', data[26:58])[0]
    file_header['GeneratingSoftware'] = struct.unpack('<32s', data[58:90])[0]
    file_header['FileCreationDayOfYear'] = struct.unpack('<H', data[90:92])[0]
    file_header['FileCreationYear'] = struct.unpack('<H', data[92:94])[0]
    file_header['HeaderSize'] = struct.unpack('<H', data[94:96])[0]
    file_header['OffsetPointData'] = struct.unpack('<L', data[96:100])[0]
    file_header['NumberOfVLR'] = struct.unpack('<L', data[100:104])[0]
    file_header['PointDataFormatID'] = struct.unpack('<B', data[104:105])[0]
    file_header['PointDataRecordLength'] = struct.unpack('<H', data[105:107])[0]
    file_header['NumberOfpointrecords'] = struct.unpack('<L', data[107:111])[0]
    file_header['NumberOfpointsbyreturn'] = [struct.unpack('<L', data[i: i+4])[0] for i in range(111, 131, 4)]
    file_header['Xscale'] = struct.unpack('<d', data[131:139])[0]
    file_header['Yscale'] = struct.unpack('<d', data[139:147])[0]
    file_header['Zscale'] = struct.unpack('<d', data[147:155])[0]
    file_header['Xoffset'] = struct.unpack('<d', data[155:163])[0]
    file_header['Yoffset'] = struct.unpack('<d', data[163:171])[0]
    file_header['Zoffset'] = struct.unpack('<d', data[171:179])[0]
    file_header['MaxX'] = struct.unpack('<d', data[179:187])[0]
    file_header['MinX'] = struct.unpack('<d', data[187:195])[0]
    file_header['MaxY'] = struct.unpack('<d', data[195:203])[0]
    file_header['MinY'] = struct.unpack('<d', data[203:211])[0]
    file_header['MaxZ'] = struct.unpack('<d', data[211:219])[0]
    file_header['MinZ'] = struct.unpack('<d', data[219:227])[0]
    
    return file_header