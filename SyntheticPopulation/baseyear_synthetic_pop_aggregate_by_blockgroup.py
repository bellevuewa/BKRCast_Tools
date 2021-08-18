import pandas as pd
import h5py
import numpy as np
import random 
import os
import utility

'''
This program is to generate total households and persons by census blockgroups for base year. It replaces households and persons by blockgroup
in the control file downloaded from ACS with OFM estimate (small area estimate). It then further replaces OFM data with with local estimate from PCD. Local estimate from PCD are housing
units. Use occupancy rate factors (for SF and MF) to convert housing units to households. Local estimate does not include population. So an average-per-unit (calculated from 2016
ACS 5-year estimate) is used to estimate population. The new control file will be used by PopulationSim to generate synthetic population that matches PSRC's 
control total. (PSRC uses OFM data)

Three files will be generated by this program.
PSRC's control total, merged control file, and an error file with missing values in any blockgroup.

02/08/2021
to ensure the program is compatible with OFM estimate in other years, remove year number from column names from 
OFM_estimate_file. For instance, OFM18_hhs now becomes OFM_hhs, OFM18_persons becomes OFM_persons.

03/16/2021
Add special treatment on blockgroup 530619900020,  530619901000, and 530610521042 so that this tool can be applied to future year synthetic population development.

'''

########### configuration #########################
## input files
working_folder = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2044'   
new_local_estimated_file_name = r'2044_COB_hhs_estimate.csv'
parcel_filename = 'I:/psrcpopsim/popsimproject/parcelize/parcel_TAZ_2014_lookup.csv'
baseyear_control_file = 'acecon0403.csv'
local_estimate_by_GOEID10_file_name = '2044_COB_hh_estimate_by_GEOID10.csv';
local_estimate_choice_file_name = 'Local_estimate_choice.csv'
OFM_estimate_file = '2040_OFM_estimate.csv'

## output files
acs_existing_control_file_name = r'ACS2016_controls_2044_estimate.csv'
parcels_for_allocation_filename = '2044_parcels_for_allocation_local_estimate.csv'

sf_occupancy_rate = 0.952  # from Gwen
mf_occupancy_rate = 0.895  # from Gwen
avg_persons_per_sfhh =  2.82 # from Gwen
avg_persons_per_mfhh =  2.03 # from Gwen
################### End of configuration

parcel_df = pd.read_csv(parcel_filename, low_memory=False) 

# calculate local estimate of total jobs and persons by blockgroup
new_local_estimate_df = pd.read_csv(os.path.join(working_folder,new_local_estimated_file_name), sep = ',')
new_local_estimate_df['SF'] = new_local_estimate_df['SFUnits'] * sf_occupancy_rate
new_local_estimate_df['MF'] = new_local_estimate_df['MFUnits'] * mf_occupancy_rate
new_local_estimate_df['total_hhs'] = new_local_estimate_df['SF'] + new_local_estimate_df['MF']
new_local_estimate_df['total_persons'] =  new_local_estimate_df['SF'] * avg_persons_per_sfhh + new_local_estimate_df['MF'] * avg_persons_per_mfhh
new_local_estimate_df = new_local_estimate_df.merge(parcel_df[['PSRC_ID', 'GEOID10']], how = 'left', left_on = 'PSRC_ID', right_on = 'PSRC_ID')
new_local_estimate_by_parcel_df = new_local_estimate_df.groupby('PSRC_ID')['total_hhs', 'total_persons'].sum()
new_local_estimate_by_parcel_df.reset_index(inplace = True)
new_local_estimate_by_parcel_df = new_local_estimate_by_parcel_df.merge(parcel_df[['PSRC_ID','GEOID10']], how = 'left', left_on = 'PSRC_ID', right_on = 'PSRC_ID')
new_local_estimate_df = new_local_estimate_df.groupby('GEOID10')['SF', 'MF', 'SFUnits', 'MFUnits', 'total_hhs', 'total_persons'].sum()
new_local_estimate_df.reset_index(inplace = True)
new_local_estimate_df[['total_hhs', 'total_persons']] = new_local_estimate_df[['total_hhs', 'total_persons']].astype(int)

print 'Total local estimate hhs: ',  int(new_local_estimate_df['total_hhs'].sum())
print 'Total local estimate persons: ', int(new_local_estimate_df['total_persons'].sum())

local_estimate_choice_df = pd.read_csv(os.path.join(working_folder, local_estimate_choice_file_name), sep = ',')
new_local_estimate_df = new_local_estimate_df.merge(local_estimate_choice_df, how = 'left', left_on = 'GEOID10', right_on = 'GEOID10')
new_local_estimate_df = new_local_estimate_df.loc[new_local_estimate_df['Use_Local'] == 'Y']

# assign OFM estimate by blockgroup to control file
OFM_estimate_df = pd.read_csv(os.path.join(working_folder, OFM_estimate_file), sep = ',', index_col = 'GEOID10')

#blockgroup 530619900020 and 530619901000 have no hhs and pop in 2016 ACS, but they have in 2035 PSRC's estimate
#move these hhs and pops to blockgroup 530610521042.
OFM_estimate_df.loc[530610521042] = OFM_estimate_df.loc[530610521042] + OFM_estimate_df.loc[530619900020] + OFM_estimate_df.loc[530619901000]
OFM_estimate_df.loc[530619900020] = 0
OFM_estimate_df.loc[530619901000] = 0

OFM_estimate_df.reset_index(inplace = True)

baseyear_control_df = pd.read_csv(os.path.join(working_folder, baseyear_control_file), sep = ',')
print baseyear_control_df['hh_bg_weight'].sum(), ' hhs in old control file'
print baseyear_control_df['pers_bg_weight'].sum(), ' persons in old control file'

baseyear_control_df = baseyear_control_df.merge(OFM_estimate_df, how = 'left', left_on = 'block_group_id', right_on = 'GEOID10')
baseyear_control_df['hh_bg_weight'] = baseyear_control_df['OFM_hhs']
baseyear_control_df['hh_tract_weight'] = baseyear_control_df['OFM_hhs']
baseyear_control_df['pers_bg_weight'] = baseyear_control_df['OFM_persons']
baseyear_control_df['pers_tract_weight'] = baseyear_control_df['OFM_persons']
baseyear_control_df.drop(OFM_estimate_df.columns, axis = 1, inplace = True)

print baseyear_control_df['hh_bg_weight'].sum(), ' hhs in new control file'
print baseyear_control_df['pers_bg_weight'].sum(), ' persons in new control file'

# assign local estimate to control file
baseyear_control_df = baseyear_control_df.merge(new_local_estimate_df[['GEOID10', 'total_hhs', 'total_persons']], how = 'left', left_on = 'block_group_id', right_on = 'GEOID10')
baseyear_control_df.loc[~baseyear_control_df['total_hhs'].isnull(), ['hh_bg_weight', 'hh_tract_weight']] = baseyear_control_df['total_hhs']
baseyear_control_df.loc[~baseyear_control_df['total_persons'].isnull(), ['pers_bg_weight', 'pers_tract_weight']] = baseyear_control_df['total_persons']

baseyear_control_df.drop(new_local_estimate_df[['GEOID10', 'total_hhs', 'total_persons']].columns, axis = 1, inplace = True)

print int(baseyear_control_df['hh_bg_weight'].sum()), ' hhs in final control file (after local estimate is incorporated).'
print int(baseyear_control_df['pers_bg_weight'].sum()), ' persons in final control file (after local estimate is incorporated).'

baseyear_control_df.fillna(0, inplace = True)
baseyear_control_df.to_csv(os.path.join(working_folder, acs_existing_control_file_name), sep = ',')
new_local_estimate_by_parcel_df.to_csv(os.path.join(working_folder, parcels_for_allocation_filename), sep = ',')

utility.backupScripts(__file__, os.path.join(working_folder, os.path.basename(__file__)))

print 'Done.'


