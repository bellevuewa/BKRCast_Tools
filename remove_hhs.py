import os
import pandas as pd
import h5py
import sys
import numpy as np
sys.path.append(os.getcwd())
#from utility import *

### inputs
hh_person_folder = r'D:\2035BKRCastBaseline\2035BKRCastBaseline\inputs'
hh_person_file = "hh_and_persons_bkr.h5"
out_h5_file = h5py.File(os.path.join(hh_person_folder, 'hh_and_persons.h5'), 'w')


hhs_list = []
parcels_list = [717277]

def h5_to_df(h5_file, group_name):
    """
    Converts the arrays in a H5 store to a Pandas DataFrame. 
    """
    col_dict = {}
    h5_set = hdf_file[group_name]
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
        print "Group Skims Exists. Group deleSted then created"
        #If not there, create the group
    else:
        my_group = h5_store.create_group(group_name)
        print "Group Skims Created"
    for col in df.columns:
        h5_store[group_name].create_dataset(col, data=df[col].values.astype('int32'))

print 'Loading hh and person file...'
hdf_file = h5py.File(os.path.join(hh_person_folder, hh_person_file), "r")

person_df = h5_to_df(hdf_file, 'Person')
hh_df = h5_to_df(hdf_file, 'Household')

#if SWITCH == 'HHNO':
#hh_df = hh_df[~hh_df['hhno'].isin(hhs_list)]
#person_df = person_df[~person_df['hhno'].isin(hhs_list)]
##elif SWITCH == 'PARCEL':
##    hh_df = hh_df[~hh_df['hhparcel'].isin(parcels_list)]

print 'Export households and persons...'
df_to_h5(hh_df, out_h5_file, 'Household')
df_to_h5(person_df, out_h5_file, 'Person')
out_h5_file.close()


print 'Done'
