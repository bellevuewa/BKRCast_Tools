import pandas as pd
import h5py
import numpy as np
import random 
import os

BKRCast_Parcel_File = r'Z:\Modeling Group\BKRCast\Job Conversion Test\parcel_level\test11\parcels_urbansim.txt'
Output_Parcel_Folder = r'Z:\Modeling Group\BKRCast\2014Jobs_2035HH_parcels'
Hh_and_person_file = r'D:\2035BKRCastBaseline\2035BKRCastBaseline\inputs\hh_and_persons.h5'

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

print 'Loading parcels ... '     
BKRCast_Parcel_DF = pd.DataFrame.from_csv(BKRCast_Parcel_File, sep = ' ')
BKRCast_Parcel_DF.reset_index(inplace = True)
BKRCast_Parcel_DF.set_index('PARCELID', inplace = True)

print 'Loading hh_and_persons.h5 ...'
hdf_file = h5py.File(Hh_and_person_file, "r")
hh_df = h5_to_df(hdf_file, 'Household')
hh_df.set_index('hhparcel', inplace = True)

hhs = hh_df.groupby('hhparcel')['hhexpfac', 'hhsize'].sum()
BKRCast_Parcel_DF = BKRCast_Parcel_DF.join(hhs, how = 'left')
BKRCast_Parcel_DF['HH_P']  = BKRCast_Parcel_DF['hhexpfac']
BKRCast_Parcel_DF.fillna(0, inplace = True)
BKRCast_Parcel_DF.drop(['hhexpfac', 'hhsize'], axis = 1, inplace = True)


print 'Exporting parcel file...'
BKRCast_Parcel_DF.to_csv(os.path.join(Output_Parcel_Folder, 'parcels_urbansim_14jobs_35hh.txt'), sep = ' ')



