import h5py
import sys, os
import numpy as np
import pandas as pd
from enum import Enum
import logging
from datetime import datetime

#2/3/2022
# upgrade to python 3.7

_LOGGING_CONFIGURED = False

Job_Categories = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPOTH_P', 'EMPRET_P', 'EMPRSC_P', 'EMPSVC_P']
Summary_Categories = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPOTH_P', 'EMPRET_P', 'EMPSVC_P', 'EMPTOT_P', 'STUGRD_P', 'STUHGH_P', 'STUUNI_P', 'HH_P']

job_rename_dict = {'JOBS_EDU':'EMPEDU_P', 'JOBS_FOOD':'EMPFOO_P', 'JOBS_GOV':'EMPGOV_P', 'JOBS_IND':'EMPIND_P',
    'JOBS_MED':'EMPMED_P', 'JOBS_OFF':'EMPOFC_P', 'JOBS_RET':'EMPRET_P', 'JOBS_RSV':'EMPRSC_P', 'JOBS_SERV':'EMPSVC_P', 'JOBS_OTH':'EMPOTH_P',
    'JOBS_TOTAL':'EMPTOT_P'}
sqft_rename_dict = {'SQFT_EDU':'SQFT_EDU', 'SQFT_FOOD':'SQFT_FOO','SQFT_GOV':'SQFT_GOV','SQFT_IND':'SQFT_IND','SQFT_MED':'SQFT_MED', 'SQFT_OFF':'SQFT_OFC',
    'SQFT_RET':'SQFT_RET', 'SQFT_RSV':'SQFT_RSV', 'SQFT_SERV':'SQFT_SVC', 'SQFT_OTH': 'SQFT_OTH', 'SQFT_NONE':'SQFT_NON', 
    'SQFT_TOTAL':'SQFT_TOT'}
du_rename_dict = {'UNITS_SF':'SFUnits', 'UNITS_MF':'MFUnits'}

class Parcel_Data_Format(Enum):
    Processed_Parcel_Data = "Processed_Parcel_Data"
    BKRCastTAZ_Format = "BKRCastTAZ_Format"
    BKR_Trip_Model_TAZ_Forma = "BKR_Trip_Model_TAZ_Forma"
class Data_Scale_Method(Enum):
    Keep_the_Data_from_the_Partner_City = "Keep_the_Data_from_the_Partner_City"
    Scale_by_Job_Category = "Scale_by_Job_Category"
    Scale_by_Total_Jobs_by_TAZ = "Scale_by_Total_Jobs_by_TAZ"

def h5_to_df(h5_file, group_name):
    """
    Converts the arrays in a H5 store to a Pandas DataFrame. 
    """
    col_dict = {}
    h5_set = h5_file[group_name]
    for col in h5_set.keys():
        my_array = np.asarray(h5_set[col])
        col_dict[col] = my_array
    df = pd.DataFrame(col_dict)
    return df

def df_to_h5(df, h5_store, group_name):
    """
    Stores DataFrame series as indivdual to arrays in an h5 container. 
    """
    # delete store store if exists   
    if group_name in h5_store:
        del h5_store[group_name]
        my_group = h5_store.create_group(group_name)
        print("Group Skims Exists. Group deleSted then created")
        #If not there, create the group
    else:
        my_group = h5_store.create_group(group_name)
        print("Group Skims Created")
    for col in df.columns:
        if col == 'block_group_id':
            # int64 cannot be read into daysim appropriately through hdf5dotnet interface,because the read function will read it as int32 which will
            # generate a memory access error. But it is fine to keep it in int64 in h5.
            h5_store[group_name].create_dataset(col, data=df[col], dtype = 'int64', compression = 'gzip')
        else:
            h5_store[group_name].create_dataset(col, data=df[col], dtype = 'int', compression = 'gzip')
            

def backupScripts(source, dest):
    import os
    import shutil
    shutil.copyfile(source, dest)

def setup_logger_file(output_dir, log_name = "parcel_processing.log") -> logging.Logger:
    global _LOGGING_CONFIGURED 
    if _LOGGING_CONFIGURED:
        return logging.getLogger(__name__)

    log_filename = os.path.join(output_dir, log_name)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler = logging.FileHandler(log_filename, mode = 'w')

    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(file_handler)
    root.addHandler(console_handler)

    _LOGGING_CONFIGURED = True

    logger = logging.getLogger(__name__)
    logger.info(
        "Logging initialized at %s", log_filename
    )  
    return logger

def controlled_rounding(data_df, attr_name, control_total, index_attr_name):
    # find residential parcels within taz     
    updated_data_df = data_df.loc[data_df[attr_name] > 0].copy()
    total_rows = updated_data_df.shape[0]
    if total_rows != 0:
        already_assigned = updated_data_df[attr_name].sum()
    else:
        already_assigned = 0
    
    # how many need to be assigned or removed to match the control total
    diff = int(control_total - already_assigned)
    if (diff == 0) | total_rows == 0:
        return data_df

    if total_rows >= abs(diff):
        selected_indices = np.random.choice(updated_data_df.index, size = abs(diff), replace = False) 
    else:
        selected_indices = np.random.choice(updated_data_df.index, size = abs(diff), replace = True) 
    
    unique_indices, counts = np.unique(selected_indices, return_counts=True)
    sorted_zipped = sorted(zip(unique_indices, counts), key=lambda x: x[1], reverse=True)

    index_for_2nd_round = []
    if control_total >= already_assigned: # need to add to match the control total, 
        for index, count in sorted_zipped:
            updated_data_df.loc[index, attr_name] += count   
    else:  # need to remove to match the control total. more complicated.
        remaining = 0
        for index, count in sorted_zipped:
            count += remaining
            # need to ensure no negative values
            if updated_data_df.loc[index, attr_name] >= count:
                updated_data_df.loc[index, attr_name] -= count
                remaining = 0
                index_for_2nd_round.append(index)
            else:
                remaining = count - updated_data_df.loc[index, attr_name]
                updated_data_df.loc[index, attr_name] = 0

        if (remaining > 0):
            for index in index_for_2nd_round:
                curValue = updated_data_df.loc[index, attr_name]
                if curValue >= remaining:
                    updated_data_df.loc[index, attr_name] = curValue - remaining
                    remaining = 0
                    break
                else:
                    remaining -= updated_data_df.loc[index, attr_name]
                    updated_data_df.loc[index, attr_name] = 0
    
    new_data_df = data_df.copy()
    new_data_df = new_data_df.loc[~new_data_df[index_attr_name].isin(updated_data_df[index_attr_name])]
    new_data_df = pd.concat([new_data_df, updated_data_df])
    
    return new_data_df
 