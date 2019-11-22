import pandas as pd
import h5py 
import os
import utility

'''
This program is to create populationsim control file only for household growth. The growth is calculated by 
multiplying dwelling units (permit_file) witih occupancy rate (bel_occ_file) saved in permit_file. The growth will be 
either replacement to new addition. We need to calculate the net growth from replacement. Net growth is 
calculated by subtracting base year number of hhs from permit data.

The net growth will be exported to hhs_output_file. The populationsim control file is also created. This control
file is an input file to populationSim.
'''

### configuration
working_folder = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2021ConcurrencyPretest'
permit_file = r'Z:\Modeling Group\BKRCast\2021concurrencyPretest\2020_permits_psrcid_plus_project_X.csv'
bel_occ_file = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\Bellevue_occupancy_rates.csv'
base_year_hhs_by_parcel = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2020ConcurrencyPretest\hh_summary_by_parcel.csv'
lookup_file = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'
popsim_base_control_file = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2020ConcurrencyPretest\popsim_control_for_2020concurrencypretest.csv'

### output files
popsim_control_output_file = 'popsim_control_for_growth_in_2021concurrencypretest.csv'
hhs_output_comparison_file = '2021concurrency_pretest_hhs_growth_comparison.csv'
hhs_output_file = '2021concurrency_pretest_hhs_growth.csv'
dwellingunits_outputs_file = '2021concurrency_pretest_units_growth.csv'

avg_persons_per_sfhh =  2.82 # from Gwen
avg_persons_per_mfhh =  2.03 # from Gwen
### end of configuration

permit_df = pd.read_csv(permit_file, sep = ',', low_memory = False)
permit_df.fillna(0, inplace = True)
permit_sum_df = permit_df.groupby('PSRC_ID')[['SFUnits', 'MFUnits']].sum().reset_index()
permit_sum_df = permit_sum_df.merge(permit_df[['PSRC_ID', 'Action']].drop_duplicates(), how = 'left', on = 'PSRC_ID')
blkgrp_lookup_df = pd.read_csv(lookup_file, sep = ',', low_memory = False)

## bellevue occupancy rates
bel_occ_rate_df = pd.read_csv(bel_occ_file, sep = ',', low_memory = False)
bel_occ_rate_df = bel_occ_rate_df.loc[~bel_occ_rate_df['PSRC_ID'].duplicated()]

# calculate number of households
growth_hhs_df = permit_sum_df.merge(bel_occ_rate_df, how = 'left', on = 'PSRC_ID')
growth_hhs_df['SFOR_e'].fillna(1, inplace = True)
growth_hhs_df['MFOR_e'].fillna(1, inplace = True)
growth_hhs_df['SFHH'] = growth_hhs_df['SFUnits'] * growth_hhs_df['SFOR_e']
growth_hhs_df['MFHH'] = growth_hhs_df['MFUnits'] * growth_hhs_df['MFOR_e']
growth_hhs_df.SFHH = growth_hhs_df.SFHH.round().astype(int)
growth_hhs_df.MFHH = growth_hhs_df.MFHH.round().astype(int)
growth_hhs_df['total_hhs'] = growth_hhs_df['SFHH'] + growth_hhs_df['MFHH']
growth_hhs_df['total_persons'] = growth_hhs_df['SFHH'] * avg_persons_per_sfhh + growth_hhs_df['MFHH'] * avg_persons_per_mfhh
growth_hhs_df.total_persons = growth_hhs_df.total_persons.round().astype(int)

growth_replace_df = growth_hhs_df.loc[growth_hhs_df['Action'] == "Replace", ['PSRC_ID', 'total_hhs', 'total_persons']]
growth_add_df =  growth_hhs_df.loc[growth_hhs_df['Action'] == "Add", ['PSRC_ID', 'total_hhs', 'total_persons']]
growth_add_df.rename(columns = {'total_hhs': 'HHGrowth', 'total_persons':'PersonsGrowth'}, inplace = True)

base_year_hhs_by_parcel_df = pd.read_csv(base_year_hhs_by_parcel, sep = ',', low_memory = False) 
base_year_hhs_by_parcel_df.rename(columns = {'hhparcel':'PSRC_ID', 'total_hhs':'base_total_hhs','total_persons':'base_total_persons'}, inplace = True)
growth_replace_df = growth_replace_df.merge(base_year_hhs_by_parcel_df[['PSRC_ID', 'base_total_hhs', 'base_total_persons']], how = 'left', left_on = 'PSRC_ID', right_on = 'PSRC_ID')
growth_replace_df.fillna(0, inplace = True)
growth_replace_df['HHGrowth'] = growth_replace_df['total_hhs'] - growth_replace_df['base_total_hhs']
growth_replace_df['PersonsGrowth'] = growth_replace_df['total_persons'] - growth_replace_df['base_total_persons']

growth_combined_df = growth_replace_df
growth_combined_df = growth_combined_df.append(growth_add_df)
growth_combined_df.to_csv(os.path.join(working_folder, hhs_output_comparison_file), sep = ',')

growth_combined_df = growth_combined_df[['PSRC_ID', 'HHGrowth', 'PersonsGrowth']]
growth_combined_df.rename(columns = {'HHGrowth': 'total_hhs', 'PersonsGrowth':'total_persons'}, inplace = True)
growth_combined_df = growth_combined_df.loc[growth_combined_df['total_hhs'] > 0]
permit_sum_df.to_csv(os.path.join(working_folder, dwellingunits_outputs_file), sep = ',')


growth_combined_df = growth_combined_df[['PSRC_ID', 'total_hhs', 'total_persons']].merge(blkgrp_lookup_df[['PSRC_ID', 'GEOID10', 'BKRCastTAZ']], how = 'left', on = 'PSRC_ID')
growth_combined_df.to_csv(os.path.join(working_folder, hhs_output_file), sep = ',')

# create control file for populationsim
growth_combined_df = growth_combined_df.groupby('GEOID10').sum().reset_index()
popsim_base_control_df = pd.read_csv(popsim_base_control_file, sep = ',')
growth_hhs_control_df = growth_combined_df.merge(popsim_base_control_df, how = 'left', left_on = 'GEOID10', right_on = 'block_group_id')
growth_hhs_control_df.fillna(0, inplace = True)
growth_hhs_control_df['hh_bg_weight'] =  growth_hhs_control_df['total_hhs'].astype(int)
growth_hhs_control_df['hh_tract_weight'] =  growth_hhs_control_df['total_hhs']
growth_hhs_control_df['pers_bg_weight'] =  growth_hhs_control_df['total_persons']
growth_hhs_control_df['pers_tract_weight'] =  growth_hhs_control_df['total_persons']
growth_hhs_control_df.drop(growth_combined_df.columns, axis = 1, inplace = True)
growth_hhs_control_df.hh_bg_weight = growth_hhs_control_df.hh_bg_weight.round().astype(int)
growth_hhs_control_df.hh_tract_weight = growth_hhs_control_df.hh_tract_weight.round().astype(int)
growth_hhs_control_df.pers_bg_weight = growth_hhs_control_df.pers_bg_weight.round().astype(int)
growth_hhs_control_df.pers_tract_weight = growth_hhs_control_df.pers_tract_weight.round().astype(int)

growth_hhs_control_df.to_csv(os.path.join(working_folder, popsim_control_output_file), sep = ',')



print 'Done'

