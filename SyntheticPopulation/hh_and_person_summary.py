### purpose:
### to summarize households and persons of input file by jurisdiction, mma and taz level.
### 7/5/19 added summary by block groups

#2/3/2022
# upgrade to python 3.7


from math import exp
import os
import pandas as pd
import h5py
import sys
import numpy as np
sys.path.append(os.getcwd())
import utility

### inputs
hh_person_folder = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\KirklandSupport\Kirkland2044Complan\WFH\target2044_30%_WFH_by_baseline_worker_conversion_file'                                       
hh_person_file = '2044_kirk_complan_target_hh_and_persons_reallocated_from_baseline_forWFH_30%.h5'
TAZ_Subarea_File_Name = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\TAZ_subarea.csv'
parcel_filename = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'
export_parcel_level_dataset = False

print('Loading hh and person file...')
hdf_file = h5py.File(os.path.join(hh_person_folder, hh_person_file), "r")
person_df = utility.h5_to_df(hdf_file, 'Person')
workers_df = person_df[['hhno', 'pwtyp', 'psexpfac']].copy()
workers_df['ft_w'] = 0
workers_df['pt_w'] = 0
workers_df.loc[workers_df['pwtyp'] == 1, 'ft_w'] = 1
workers_df.loc[workers_df['pwtyp'] == 2, 'pt_w'] = 1
workers_by_hhs_df = workers_df.groupby('hhno').sum().reset_index()

hh_df = utility.h5_to_df(hdf_file, 'Household')
hdf_file.close()
hh_df = hh_df.merge(workers_by_hhs_df, on = 'hhno', how = 'left')
taz_subarea = pd.read_csv(TAZ_Subarea_File_Name, sep = ",", index_col = "BKRCastTAZ")

hh_taz = hh_df.join(taz_subarea, on = 'hhtaz')
hh_taz['total_persons'] = hh_taz['hhexpfac'] * hh_taz['hhsize']
hh_taz['total_hhs'] = hh_taz['hhexpfac']

summary_by_jurisdiction = hh_taz.groupby('Jurisdiction')[['total_hhs', 'total_persons', 'ft_w', 'pt_w']].sum()   
summary_by_mma = hh_taz.groupby('Subarea')[['total_hhs', 'total_persons',  'ft_w', 'pt_w']].sum()
summary_by_parcels = hh_taz.groupby('hhparcel')[['total_hhs', 'total_persons',  'ft_w', 'pt_w']].sum()

taz_subarea.reset_index()
subarea_def = taz_subarea[['Subarea', 'SubareaName']]
subarea_def = subarea_def.drop_duplicates(keep = 'first')
subarea_def.set_index('Subarea', inplace = True)
summary_by_mma = summary_by_mma.join(subarea_def)
summary_by_taz = hh_taz.groupby('hhtaz')[['total_hhs', 'total_persons',  'ft_w', 'pt_w']].sum()


print('exporting summary by Jurisdiction ... ')
summary_by_jurisdiction.to_csv(os.path.join(hh_person_folder, "hh_summary_by_jurisdiction.csv"), header = True)
print('exporting summary by mma... ')
summary_by_mma.to_csv(os.path.join(hh_person_folder, "hh_summary_by_mma.csv"), header = True)
print('exporting summary by taz... ')
summary_by_taz.to_csv(os.path.join(hh_person_folder, "hh_summary_by_taz.csv"), header = True)
print('exporting summary by parcel...')
summary_by_parcels.to_csv(os.path.join(hh_person_folder, 'hh_summary_by_parcel.csv'), header = True)

parcel_df = pd.read_csv(parcel_filename, low_memory=False) 
hh_taz = hh_taz.merge(parcel_df, how = 'left', left_on = 'hhparcel', right_on = 'PSRC_ID')
summary_by_geoid10 = hh_taz.groupby('GEOID10')[['total_hhs', 'total_persons',  'ft_w', 'pt_w']].sum()
print('exporting summary by block groups...')
summary_by_geoid10.to_csv(os.path.join(hh_person_folder, 'hh_summary_by_geoid10.csv'), header = True)

if export_parcel_level_dataset == True:
    print('exporting households and persons by parcel...')
    hh_df.to_csv(os.path.join(hh_person_folder, 'households.csv'), header = True)
    person_df.to_csv(os.path.join(hh_person_folder, 'persons.csv'), header = True)

print('done.')

