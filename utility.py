import h5py
import numpy as np
import pandas as pd

#2/3/2022
# upgrade to python 3.7


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
    