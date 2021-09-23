import pandas as pd
import numpy as np
import os
import math
import h5py
import utility

'''
This tool is used to change some workers, defined by a percet in each TAZ, from full time status to non-worker status.
The percent of full time workers is set in adjustment_factor_name file.

In each TAZ, it randomly draws a certain percent of fulltime workers from the list and change their full time status to non-worker
status. The updated synthetic population are then exported to h5 file.
'''

### input configuration
working_folder = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\WFHTesting\2014"
original_h5_file_name = '2014_hh_and_persons.h5'
TAZ_Subarea_File_Name = r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\TAZ_subarea.csv"
# percent of workers to be adjusted
adjustment_factor_name = r"TAZ_subarea_worker_adjustment.csv"

### output configuration
updated_h5_file_name = 'updated_hh_and_persons.h5'


print 'Loading hh and person file...'
hdf_file = h5py.File(os.path.join(working_folder, original_h5_file_name), "r")
person_df = utility.h5_to_df(hdf_file, 'Person')
hhs_df = utility.h5_to_df(hdf_file, 'Household')
adjustment_factor_df = pd.read_csv(os.path.join(working_folder, adjustment_factor_name), sep = ',')
person_df = pd.merge(person_df, hhs_df[['hhno', 'hhtaz']], on = 'hhno')
person_df['pid'] = person_df.index
print 'hhs: '  + str(hhs_df.shape)
print 'persons: ' + str(person_df.shape)

updated_person_df = pd.DataFrame()
total_adjusted = 0
for taz in adjustment_factor_df.itertuples():
    rate = taz.WorkerAdjFactor
    persons_in_taz = person_df.loc[(person_df['hhtaz'] == taz.BKRCastTAZ)]
    fulltime_workers_in_taz = persons_in_taz.loc[persons_in_taz['pwtyp'] == 1]
    if fulltime_workers_in_taz.shape[0] > 0:
        selected = fulltime_workers_in_taz.sample(frac = rate, random_state = 1)
        not_selected = fulltime_workers_in_taz.loc[~fulltime_workers_in_taz['pid'].isin(selected['pid'])]
        # change the worker type from full time (1) to non-worker (0)
        selected['pwtyp'] = 0 
        total_adjusted += selected.shape[0]
        updated_person_df = updated_person_df.append(not_selected)
        updated_person_df = updated_person_df.append(selected)
        print 'taz ' + str(taz.BKRCastTAZ)  + ': ' + str(selected.shape[0]) + ' full time workers are changed to non-worker.'

updated_person_df = updated_person_df.append(person_df.loc[person_df['pwtyp'] <> 1])
total_workers_before = person_df.loc[person_df['pwtyp'] == 1, 'psexpfac'].sum()
total_workers_after = updated_person_df.loc[updated_person_df['pwtyp'] == 1, 'psexpfac'].sum()

print str(total_workers_before) + ' workers before the change.'
print str(total_workers_after) + ' workders after the change.'
print str(total_adjusted) + ' workers have been changed.'
updated_person_df.drop(columns = ['pid'], axis = 1, inplace = True)

output_h5_file = h5py.File(os.path.join(working_folder, updated_h5_file_name), 'w')
utility.df_to_h5(hhs_df, output_h5_file, 'Household')
utility.df_to_h5(updated_person_df, output_h5_file, 'Person')
output_h5_file.close()

print 'Done'             
