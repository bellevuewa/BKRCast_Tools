
import pandana as pdna
import os, sys
sys.path.append(os.getcwd())
import pandas as pd
import numpy as np
import re
from pyproj import Proj, transform
import copy
from shutil import copyfile
import h5py


input_Parcel_Folder = r"Z:\Modeling Group\BKRCast\1995DerivedEmploymentData"
input_Parcel_File_Name = r"BKRCast_Parcel_1995data.csv"
input_hh_person_file_name = r"hh_and_persons.h5"
Output_Parcel_File_Name = "parcels_urbansim_1995.txt"

universal_factor_for_uncovered_ESD_data = 1.075   # can b

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

print "Loading input files ..."
parcels = pd.DataFrame.from_csv(os.path.join(input_Parcel_Folder, input_Parcel_File_Name), sep = ",")

columns_drop = ['OBJECTID', 'TAZ', 'PSRC_ID', 'parcel_id', 'Jurisdiction', 'BKRCastTAZ', 'Total', 'Sum_EMPTOT_P', 'Sum_HH_P', 'TRACTID']
ratio = parcels['Total'] / parcels['Sum_EMPTOT_P']
JOB_CATEGORY = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPOTH_P', 'EMPRET_P', 'EMPSVC_P']
parcels['EMPTOT_P'] = 0

for job_cat in JOB_CATEGORY:
    parcels[job_cat] = parcels[job_cat] * ratio * universal_factor_for_uncovered_ESD_data
    parcels['EMPTOT_P'] = parcels['EMPTOT_P'] + parcels[job_cat]

hdf_file = h5py.File(os.path.join(input_Parcel_Folder, input_hh_person_file_name), "r")
hh_df = h5_to_df(hdf_file, 'Household')
hhs_by_parcel = hh_df.groupby('hhparcel')['hhno'].count()

parcels = parcels.join(hhs_by_parcel, on = 'PARCELID') 
parcels['HH_P'] = parcels['hhno']
parcels = parcels.reset_index()
parcels.drop(columns_drop, inplace = True, axis = 1)
parcels.drop('hhno', inplace = True, axis = 1)
parcels.sort_values('PARCELID', inplace = True)

parcels.to_csv(os.path.join(input_Parcel_Folder, Output_Parcel_File_Name), index = False, sep = ' ')
print 'done'


