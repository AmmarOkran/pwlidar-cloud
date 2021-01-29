# import ibm_boto3
# import ibm_botocore
# from ibm_botocore.client import Config
import os
import laspy
import struct
import logging
import numpy as np
from pwlidar_cloud import utils
from pwlidar_cloud.storage.utils import CloudObject, CloudObjectUrl

logger = logging.getLogger(__name__)




def scaled_x_dimension(las_file):
    x_dimension = las_file.X
    scale = las_file.header.scale[0]
    offset = las_file.header.offset[0]
    return(x_dimension*scale + offset)


def scaled_y_dimension(las_file):
    y_dimension = las_file.Y
    scale = las_file.header.scale[1]
    offset = las_file.header.offset[1]
    return(y_dimension*scale + offset)


def scaled_z_dimension(las_file):
    z_dimension = las_file.Z
    scale = las_file.header.scale[1]
    offset = las_file.header.offset[1]
    return(z_dimension*scale + offset)



def parse_header(data):
    
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
    file_header['MaxX'] = round(struct.unpack('<d', data[179:187])[0], 2)
    file_header['MinX'] = round(struct.unpack('<d', data[187:195])[0], 2)
    file_header['MaxY'] = round(struct.unpack('<d', data[195:203])[0], 2)
    file_header['MinY'] = round(struct.unpack('<d', data[203:211])[0], 2)
    file_header['MaxZ'] = round(struct.unpack('<d', data[211:219])[0], 2)
    file_header['MinZ'] = round(struct.unpack('<d', data[219:227])[0], 2)
    
    return file_header

def prep_partition(entry, part_info, keys_dict, rows, cols, partition_type):

    partitions = []
    # Identify tiles
    mn_X = part_info['min_X']
    mn_Y = part_info['min_Y']
    max_X = part_info['max_X']
    max_Y = part_info['max_Y']
    min_X = part_info['min_X']
    min_Y = part_info['min_Y']
    pointX_offset = round(((part_info['max_X'] - part_info['min_X']) / rows), 2)   # math.ceil()
    pointY_offset = round(((part_info['max_Y'] - part_info['min_Y']) / cols), 2) # math.ceil()

    sb, bucket, prefix, obj_name = utils.split_object_url(entry['obj'])
    key = '/'.join([prefix, obj_name]) if prefix else obj_name
    total_partitions = 0

    for y in range(cols):
        tilY_st = mn_Y + (pointY_offset * y)
        
        for x in range(rows):
            partition = {}
            tilX_st = mn_X + (pointX_offset * x)
            logger.info('tile X starts from: {}'.format(tilX_st))
            logger.info('tile Y starts from: {}'.format(tilY_st))
        
            if (tilX_st == mn_X and tilY_st == mn_Y):
                # The limits of the X-axis values
                limX_vals = (tilX_st, round((tilX_st + pointX_offset), 2))
                addupp_X_inf = (round((tilX_st + pointX_offset), 2), round((tilX_st + pointX_offset + part_info['delta']), 2))
                addlow_X_inf = (0, 0)
                logger.info("limitation of X values for tile {}, {} is {}".format(x, y, limX_vals))

                # The limits of the Y-axis values                
                limY_vals = (tilY_st, round((tilY_st + pointY_offset), 2))
                addupp_Y_inf = (round((tilY_st + pointY_offset), 2), round((tilY_st + pointY_offset + part_info['delta']), 2)) if not(round((tilY_st + pointY_offset), 2) >= max_Y) and y < (cols - 1) else (0, 0)
                addlow_Y_inf = (0, 0)
                min_X += pointX_offset
                logger.info("limitation of Y values for tile {}, {} is {}".format(x, y, limY_vals))

            elif (tilX_st != mn_X and tilY_st == mn_Y):
                # The limits of the X-axis values
                limX_vals = (tilX_st, round((tilX_st + pointX_offset), 2)) if not(round((tilX_st + pointX_offset), 2) > max_X) and x < (rows - 1) else (tilX_st, max_X)
                addupp_X_inf = (round((tilX_st + pointX_offset), 2), round((tilX_st + pointX_offset + part_info['delta']), 2)) if not(round((tilX_st + pointX_offset), 2) >= max_X) and x < (rows - 1) else (0, 0)
                addlow_X_inf = (tilX_st, round((tilX_st - part_info['delta']), 2))
                logger.info("limitation of X values for tile {}, {} is {}".format(x, y, limX_vals))

                # The limits of the Y-axis values
                limY_vals = (tilY_st, round((tilY_st + pointY_offset), 2))
                addupp_Y_inf = (round((tilY_st + pointY_offset), 2), round((tilY_st + pointY_offset + part_info['delta']), 2)) if not(round((tilY_st + pointY_offset), 2) >= max_Y) and y < (cols - 1) else (0, 0)
                addlow_Y_inf = (0, 0)
                logger.info("limitation of Y values for tile {}, {} is {}".format(x, y, limY_vals))
        
            elif (tilX_st == mn_X and tilY_st != mn_Y):
                # The limits of the X-axis values
                limX_vals = (tilX_st, round((tilX_st + pointX_offset), 2)) if not(round((tilX_st + pointX_offset), 2) >= max_X) and x < (rows - 1) else (tilX_st, max_X)
                addupp_X_inf = (round((tilX_st + pointX_offset), 2), round((tilX_st + pointX_offset + part_info['delta']), 2)) if not(round((tilX_st + pointX_offset), 2) >= max_X) and x < (rows - 1) else (0, 0)
                addlow_X_inf = (0, 0)
                logger.info("limitation of X values for tile {}, {} is {}".format(x, y, limX_vals))

                # The limits of the Y-axis values
                limY_vals = (tilY_st, round((tilY_st + pointY_offset), 2)) if not(round((tilY_st + pointY_offset), 2) >= max_Y) and y < (cols - 1) else (tilY_st, max_Y)
                addupp_Y_inf = (round((tilY_st + pointY_offset), 2), round((tilY_st + pointY_offset + part_info['delta']), 2)) if not(round((tilY_st + pointY_offset), 2) >= max_Y) and y < (cols - 1) else (0, 0)
                addlow_Y_inf = (tilY_st, round((tilY_st - part_info['delta']), 2))
                logger.info("limitation of Y values for tile {}, {} is {}".format(x, y, limY_vals))
        
            else: #(tilX_st != mn_X and tilY_st != mn_Y):
                # The limits of the X-axis values
                limX_vals = (tilX_st, round((tilX_st + pointX_offset), 2)) if not(round((tilX_st + pointX_offset), 2) >= max_X) and x < (rows - 1) else (tilX_st, max_X)
                addupp_X_inf = (round((tilX_st + pointX_offset), 2), round((tilX_st + pointX_offset + part_info['delta']), 2)) if not(round((tilX_st + pointX_offset), 2) >= max_X) and x < (rows - 1) else (0, 0)
                addlow_X_inf = (tilX_st, round((tilX_st - part_info['delta']), 2))
                logger.info("limitation of X values for tile {}, {} is {}".format(x, y, limX_vals))

                # The limits of the Y-axis values
                limY_vals = (tilY_st, round((tilY_st + pointY_offset), 2)) if not(round((tilY_st + pointY_offset), 2) >= max_Y) and y < (cols - 1)else (tilY_st, max_Y)
                addupp_Y_inf = (round((tilY_st + pointY_offset), 2), round((tilY_st + pointY_offset + part_info['delta']), 2)) if not(round((tilY_st + pointY_offset), 2) > max_Y) and y < (cols - 1) else (0, 0)
                addlow_Y_inf = (tilY_st, round((tilY_st - part_info['delta']), 2))
                logger.info("limitation of Y values for tile {}, {} is {}".format(x, y, limY_vals))
                
            partition = entry.copy()
            partition['obj'] = CloudObject(sb, bucket, key)
            partition['obj'].limit_X_values = limX_vals
            partition['obj'].addupp_X_val = addupp_X_inf
            partition['obj'].addlow_X_val = addlow_X_inf
            partition['obj'].limit_Y_values = limY_vals
            partition['obj'].addupp_Y_val = addupp_Y_inf
            partition['obj'].addlow_Y_val = addlow_Y_inf
            partition['obj'].data_byte_range = None
            partition['obj'].pointsX_offset = pointX_offset
            partition['obj'].pointsY_offset = pointY_offset
            partition['obj'].part = total_partitions
            partitions.append(partition)
            total_partitions = total_partitions + 1  
    
    return partitions, total_partitions


def file_part(whole_file, obj):

    bord_meta = dict()
    bord_meta['st_X'] = obj.limit_X_values[0]
    bord_meta['end_X'] = obj.limit_X_values[1]
    bord_meta['st_X_addlowinfo'] = obj.addlow_X_val[0]
    bord_meta['end_X_addlowinfo'] = obj.addlow_X_val[1]
    bord_meta['st_X_addupinfo'] = obj.addupp_X_val[0]
    bord_meta['end_X_addupinfo'] = obj.addupp_X_val[1]

    bord_meta['st_Y'] = obj.limit_Y_values[0]
    bord_meta['end_Y'] = obj.limit_Y_values[1]
    bord_meta['st_Y_addlowinfo'] = obj.addlow_Y_val[0]
    bord_meta['end_Y_addlowinfo'] = obj.addlow_Y_val[1]
    bord_meta['st_Y_addupinfo'] = obj.addupp_Y_val[0]
    bord_meta['end_Y_addupinfo'] = obj.addupp_Y_val[1]


    with open(obj.key, 'wb') as f:
        f.write(whole_file.read())
    
    inFile = laspy.file.File(obj.key, mode="r")

    coords = np.vstack((inFile.x, inFile.y, inFile.z)).transpose() 
    # f = {'st_X': 349000.13, 
    #      'end_X': 349500.06, 
    #      'st_X_addlowinfo': 0, 
    #      'end_X_addlowinfo': 0, 
    #      'st_X_addupinfo': 349500.06, 
    #      'end_X_addupinfo': 349500.26, 
    #      'st_Y': 4562000.04, 
    #      'end_Y': 4563731.93, 
    #      'st_Y_addlowinfo': 0, 
    #      'end_Y_addlowinfo': 0, 
    #      'st_Y_addupinfo': 0, 
    #      'end_Y_addupinfo': 0
    #      }

    if (bord_meta['st_X_addlowinfo'] and bord_meta['st_Y_addlowinfo']) and (bord_meta['st_X_addupinfo'] and bord_meta['st_Y_addupinfo']):
        logger.info('condition num: {}'.format(1))
        coords = coords[(coords[:,0] >= bord_meta['st_X_addlowinfo']) & (coords[:,0] <= bord_meta['end_X_addupinfo']) & 
                        (coords[:,1] >= bord_meta['st_Y_addlowinfo']) & (coords[:,1] <= bord_meta['end_Y_addupinfo'])]
    
    elif (bord_meta['st_X_addlowinfo'] and bord_meta['st_Y_addlowinfo']) and (bord_meta['st_X_addupinfo'] and not bord_meta['st_Y_addupinfo']):
        logger.info('condition num: {}'.format(2))
        coords = coords[(coords[:,0] >= bord_meta['st_X_addlowinfo']) & (coords[:,0] <= bord_meta['end_X_addupinfo']) & 
                        (coords[:,1] >= bord_meta['st_Y_addlowinfo']) & (coords[:,1] <= bord_meta['end_Y'])]

    elif (bord_meta['st_X_addlowinfo'] and bord_meta['st_Y_addlowinfo']) and (not bord_meta['st_X_addupinfo'] and bord_meta['st_Y_addupinfo']):
        logger.info('condition num: {}'.format(3))
        coords = coords[(coords[:,0] >= bord_meta['st_X_addlowinfo']) & (coords[:,0] <= bord_meta['end_X']) & 
                        (coords[:,1] >= bord_meta['st_Y_addlowinfo']) & (coords[:,1] <= bord_meta['end_Y_addupinfo'])]
    
    elif (bord_meta['st_X_addlowinfo'] and bord_meta['st_Y_addlowinfo']) and not(bord_meta['st_X_addupinfo'] or bord_meta['st_Y_addupinfo']):
        logger.info('condition num: {}'.format(4))
        coords = coords[(coords[:,0] >= bord_meta['st_X_addlowinfo']) & (coords[:,0] <= bord_meta['end_X']) & 
                        (coords[:,1] >= bord_meta['st_Y_addlowinfo']) & (coords[:,1] <= bord_meta['end_Y'])]
    
    elif (bord_meta['st_X_addlowinfo'] and not bord_meta['st_Y_addlowinfo']) and (bord_meta['st_X_addupinfo'] and bord_meta['st_Y_addupinfo']):
        logger.info('condition num: {}'.format(5))
        coords = coords[(coords[:,0] >= bord_meta['st_X_addlowinfo']) & (coords[:,0] <= bord_meta['end_X_addupinfo']) & 
                        (coords[:,1] >= bord_meta['st_Y']) & (coords[:,1] <= bord_meta['end_Y_addupinfo'])]
    
    elif (bord_meta['st_X_addlowinfo'] and not bord_meta['st_Y_addlowinfo']) and (bord_meta['st_X_addupinfo'] and not bord_meta['st_Y_addupinfo']):
        logger.info('condition num: {}'.format(6))
        coords = coords[(coords[:,0] >= bord_meta['st_X_addlowinfo']) & (coords[:,0] <= bord_meta['end_X_addupinfo']) & 
                        (coords[:,1] >= bord_meta['st_Y']) & (coords[:,1] <= bord_meta['end_Y'])]
    
    
    elif (bord_meta['st_X_addlowinfo'] and not bord_meta['st_Y_addlowinfo']) and (not bord_meta['st_X_addupinfo'] and bord_meta['st_Y_addupinfo']):
        logger.info('condition num: {}'.format(7))
        coords = coords[(coords[:,0] >= bord_meta['st_X_addlowinfo']) & (coords[:,0] <= bord_meta['end_X']) & 
                        (coords[:,1] >= bord_meta['st_Y']) & (coords[:,1] <= bord_meta['end_Y_addupinfo'])]
    
    elif (bord_meta['st_X_addlowinfo'] and not bord_meta['st_Y_addlowinfo']) and (not bord_meta['st_X_addupinfo'] and not bord_meta['st_Y_addupinfo']):
        logger.info('condition num: {}'.format(8))
        coords = coords[(coords[:,0] >= bord_meta['st_X_addlowinfo']) & (coords[:,0] <= bord_meta['end_X']) & 
                        (coords[:,1] >= bord_meta['st_Y']) & (coords[:,1] <= bord_meta['end_Y'])]
    
    elif (not bord_meta['st_X_addlowinfo'] and bord_meta['st_Y_addlowinfo']) and (bord_meta['st_X_addupinfo'] and bord_meta['st_Y_addupinfo']):
        logger.info('condition num: {}'.format(9))
        coords = coords[(coords[:,0] >= bord_meta['st_X']) & (coords[:,0] <= bord_meta['end_X_addupinfo']) & 
                        (coords[:,1] >= bord_meta['st_Y_addlowinfo']) & (coords[:,1] <= bord_meta['end_Y_addupinfo'])]
    
    elif (not bord_meta['st_X_addlowinfo'] and bord_meta['st_Y_addlowinfo']) and (bord_meta['st_X_addupinfo'] and not bord_meta['st_Y_addupinfo']):
        logger.info('condition num: {}'.format(10))
        coords = coords[(coords[:,0] >= bord_meta['st_X']) & (coords[:,0] <= bord_meta['end_X_addupinfo']) & 
                        (coords[:,1] >= bord_meta['st_Y_addlowinfo']) & (coords[:,1] <= bord_meta['end_Y'])]
    
    elif (not bord_meta['st_X_addlowinfo'] and bord_meta['st_Y_addlowinfo']) and (not bord_meta['st_X_addupinfo'] and bord_meta['st_Y_addupinfo']):
        logger.info('condition num: {}'.format(11))
        coords = coords[(coords[:,0] >= bord_meta['st_X']) & (coords[:,0] <= bord_meta['end_X']) & 
                        (coords[:,1] >= bord_meta['st_Y_addlowinfo']) & (coords[:,1] <= bord_meta['end_Y_addupinfo'])]
    
    elif (not bord_meta['st_X_addlowinfo'] and bord_meta['st_Y_addlowinfo']) and (not bord_meta['st_X_addupinfo'] and not bord_meta['st_Y_addupinfo']):
        logger.info('condition num: {}'.format(12))
        coords = coords[(coords[:,0] >= bord_meta['st_X']) & (coords[:,0] <= bord_meta['end_X']) & 
                        (coords[:,1] >= bord_meta['st_Y_addlowinfo']) & (coords[:,1] <= bord_meta['end_Y'])]
    
    elif (not bord_meta['st_X_addlowinfo'] and not bord_meta['st_Y_addlowinfo']) and (bord_meta['st_X_addupinfo'] and bord_meta['st_Y_addupinfo']):
        logger.info('condition num: {}'.format(13))
        coords = coords[(coords[:,0] >= bord_meta['st_X']) & (coords[:,0] <= bord_meta['end_X_addupinfo']) & 
                        (coords[:,1] >= bord_meta['st_Y']) & (coords[:,1] <= bord_meta['end_Y_addupinfo'])]
    
    elif (not bord_meta['st_X_addlowinfo'] and not bord_meta['st_Y_addlowinfo']) and (bord_meta['st_X_addupinfo'] and not bord_meta['st_Y_addupinfo']):
        logger.info('condition num: {}'.format(14))
        coords = coords[(coords[:,0] >= bord_meta['st_X']) & (coords[:,0] <= bord_meta['end_X_addupinfo']) & 
                        (coords[:,1] >= bord_meta['st_Y']) & (coords[:,1] <= bord_meta['end_Y'])]
    
    elif (not bord_meta['st_X_addlowinfo'] and not bord_meta['st_Y_addlowinfo']) and (not bord_meta['st_X_addupinfo'] and bord_meta['st_Y_addupinfo']):
        logger.info('condition num: {}'.format(15))
        coords = coords[(coords[:,0] >= bord_meta['st_X']) & (coords[:,0] <= bord_meta['end_X']) & 
                        (coords[:,1] >= bord_meta['st_Y']) & (coords[:,1] <= bord_meta['end_Y_addupinfo'])]
    
    else:
        logger.info('condition num: {}'.format(16))
        coords = coords[(coords[:,0] >= bord_meta['st_X']) & (coords[:,0] <= bord_meta['end_X']) & 
                        (coords[:,1] >= bord_meta['st_Y']) & (coords[:,1] <= bord_meta['end_Y'])]
    

    try:
        c = np.in1d(inFile.x, coords[:,0]) & np.in1d(inFile.y, coords[:,1]) & np.in1d(inFile.y, coords[:,2])
        points_kept = inFile.points[c]
        # xmax = max(points_kept[:,0])
        # ymax = max(points_kept[:,1])
        # zmax = max(points_kept[:,2])

        # xmin = min(points_kept[:,0])
        # ymin = min(points_kept[:,1])
        # zmin = min(points_kept[:,2])
        logger.info("sahpe {}".format(points_kept.shape))
        os.remove(obj.key)
        # Write points into las File
        f_name = obj.key
        f_name = f_name.split(".")[0] + "-" + str(obj.part) + ".laz"

        outFile1 = laspy.file.File(f_name, mode = "w",header = inFile.header)
        outFile1.points = points_kept
        # outFile1.header.max = [xmax, ymax, zmax]
        # outFile1.header.min = [xmin, ymin, zmin]
        # outFile1.header.records_count = len(points_kept)
        outFile1.close()
        inFile.close()
        res_file = os.getcwd() + "/" + f_name

    except IndexError as error:
        # Output expected IndexErrors.
        logger.error('Output expected IndexErrors: {}', str(error))
        points_kept = None
        return points_kept

    return res_file #bord_meta,
                       