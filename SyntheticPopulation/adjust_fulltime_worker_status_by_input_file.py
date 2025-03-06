import pandas as pd
import numpy as np
import os
import math
import h5py
from pickle import pack
import utility

'''
This tool is used to change some workers from worker (fulltime or part time) status to non-worker status.
These workers are defined in baseline_converted_workers_file. 

the purpose of this tool is to make sure the new generated popsim are identical with the baseline popsim.
'''

### input configuration
working_folder = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\KirklandSupport\Kirkland2044Complan\WFH\target2044_30%_WFH_by_baseline_worker_conversion_file"
original_h5_file_name = '2044_kirk_complan_target_hh_and_persons_reallocated_from_baseline.h5'
TAZ_Subarea_File_Name = r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\TAZ_subarea.csv"
# percent of workers to be adjusted
adjustment_factor_name = r"TAZ_subarea_worker_adjustment.csv"
baseline_converted_workers_file = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\KirklandSupport\Kirkland2044Complan\WFH\baseline2044_30%WFH\converted_non_workers.csv"

### output configuration
updated_h5_file_name = '2044_kirk_complan_target_hh_and_persons_reallocated_from_baseline_forWFH_30%.h5'
converted_nonworker_file_name = 'converted_non_workers.csv'
report_file_name = 'workers_conversion_report.txt'


print ('Loading hh and person file...')   
hdf_file = h5py.File(os.path.join(working_folder, original_h5_file_name), "r")
person_df = utility.h5_to_df(hdf_file, 'Person')
hhs_df = utility.h5_to_df(hdf_file, 'Household')
hdf_file.close()
adjustment_factor_df = pd.read_csv(os.path.join(working_folder, adjustment_factor_name), sep = ',')
person_df = pd.merge(person_df, hhs_df[['hhno', 'hhtaz', 'hhparcel']], on = 'hhno')
person_df['pid'] = person_df.index
print('hhs: '  + str(hhs_df.shape) )
print('persons: ' + str(person_df.shape))

updated_person_df = pd.DataFrame()
total_adjusted = 0
converted_df = pd.DataFrame()

baseline_converted_workers_df = pd.read_csv(baseline_converted_workers_file)
report = []

total_workers_before = person_df.loc[person_df['pwtyp'] > 0, 'psexpfac'].sum()
person_df['pid'] = person_df.index

updated_person_df = person_df.merge(baseline_converted_workers_df[['hhno', 'pno']], on = ['hhno', 'pno'], how = 'inner')
mask = person_df['pid'].isin(updated_person_df['pid'])
person_df.loc[mask, 'pwtyp'] = 0
person_df.loc[mask & (person_df['pptyp'].isin([1,2])), 'pptyp'] = 0
total_workers_after = person_df.loc[person_df['pwtyp'] > 0, 'psexpfac'].sum()

person_df.drop(columns = ['pid', 'hhtaz', 'hhparcel'], inplace = True)
print(f'{total_workers_before} workers before the change.' )
print(f'{total_workers_after} workders after the change.')
print(f'{total_workers_before - total_workers_after} workers have been changed.')

output_h5_file = h5py.File(os.path.join(working_folder, updated_h5_file_name), 'w')
utility.df_to_h5(hhs_df, output_h5_file, 'Household')
utility.df_to_h5(person_df, output_h5_file, 'Person')
output_h5_file.close()
updated_person_df[['hhno', 'pno', 'hhtaz', 'hhparcel']].to_csv(os.path.join(working_folder, converted_nonworker_file_name), index = False)


utility.backupScripts(__file__, os.path.join(working_folder, os.path.basename(__file__)))
print('Done')             
