### purpose:
### to summarize households and persons of input file by jurisdiction, mma and taz level.

import os
import pandas as pd
import h5py
import sys
import numpy as np

### inputs
hh_person_folder = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\backcast\1990HH_persons'
hh_person_file = "hh_and_persons.h5"
TAZ_Subarea_File_Name = r'Z:\Modeling Group\BKRCast\Job Conversion Test\TAZ_subarea.csv'

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
taz_subarea = pd.DataFrame.from_csv(TAZ_Subarea_File_Name, sep = ",", index_col = "TAZNUM")


person_df = h5_to_df(hdf_file, 'Person')
hh_df = h5_to_df(hdf_file, 'Household')

hh_taz = hh_df.join(taz_subarea, on = 'hhtaz')
hh_taz['total_persons'] = hh_taz['hhexpfac'] * hh_taz['hhsize']
hh_taz['total_hhs'] = hh_taz['hhexpfac']
summary_by_jurisdiction = hh_taz.groupby('Jurisdiction')['total_hhs', 'total_persons'].sum()   
summary_by_mma = hh_taz.groupby('Subarea')['total_hhs', 'total_persons'].sum()

taz_subarea.reset_index()
subarea_def = taz_subarea[['Subarea', 'SubareaName']]
subarea_def = subarea_def.drop_duplicates(keep = 'first')
subarea_def.set_index('Subarea', inplace = True)
summary_by_mma = summary_by_mma.join(subarea_def)
summary_by_taz = hh_taz.groupby('hhtaz')['total_hhs', 'total_persons'].sum()

print 'exporting summary by Jurisdiction ... '
summary_by_jurisdiction.to_csv(os.path.join(hh_person_folder, "summary_by_jurisdiction.csv"), header = True)
print 'exporting summary by mma... '
summary_by_mma.to_csv(os.path.join(hh_person_folder, "summary_by_mma.csv"), header = True)
print 'exporting summary by taz... '
summary_by_taz.to_csv(os.path.join(hh_person_folder, "summary_by_taz.csv"), header = True)
print 'done.'

