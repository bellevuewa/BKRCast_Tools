import pandas as pd
import h5py 
import os
import utility

'''
   This program is used to create control file (for concurrency test) for PopulationSim. The permit data (permit_file) includes two types of land use: one is
to replace the existing land use; the other is to add to the existing land use. The type is flagged by 'Action' column. Permitted land use will be placed on top of the base year
land use (base_year_Bellevue_LU_file). The final output is saved in parcel_output_file. This file is one of many input files for replace_kingcounty_sqft_with_local_estimate.py.

Sf units and mf units are included in the land use input file. They will be converted to households by occupancy rate (bel_occ_file). The households by parcel will be aggregated into
census blockgroups and then the control file (popsim_control_output_file) for populationsim is created.  

output files for parcelization:
    dwellunits_output_file
    hhs_output_file
'''

### configuration
working_folder = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2020ConcurrencyPretest'
base_year_Bellevue_LU_file = r'Z:\Modeling Group\BKRCast\2018LU\2018_Bellevue_LU_by_parcel.csv'
permit_file = r'Z:\Modeling Group\BKRCast\2020concurrencyPretest\2018_2019_permits_updated_pscrid+19-118270LP.csv'
bel_occ_file = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\Bellevue_occupancy_rates.csv'
base_year_hhs_by_parcel = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2018\hh_summary_by_parcel.csv'
lookup_file = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'
popsim_2018_control_file = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2018\ACS2016_controls_OFM2018estimate.csv'
popsim_control_output_file = 'popsim_control_for_2020concurrencypretest.csv'

### output files
parcel_output_file = r'Z:\Modeling Group\BKRCast\2020concurrencyPretest\cob_parcels_for_2020_concurrency_pretest.csv'
dwellunits_output_file = '2020concurrency_pretest_hhs_estimate.csv'
hhs_output_file = '2020concurrency_pretest_parcel_for_allocation_local_estimates.csv'

avg_persons_per_sfhh =  2.82 # from Gwen
avg_persons_per_mfhh =  2.03 # from Gwen
### end of configuration

emp_cats = ['Edu', 'Foo', 'Gov', 'Ind', 'Med', 'Ofc', 'Oth', 'Ret', 'Svc', 'NoEmp']
density_levels = ['VeryLow', 'Low', 'Med', 'High', 'VeryHigh']

emp_den_cats = []
for emp in emp_cats:
    if emp != 'NoEmp':
        for den in density_levels:
            emp_den_cats.append(emp + '_' + den)
    else:
        emp_den_cats.append(emp)

hhs_emp_columns = list(emp_den_cats)
hhs_emp_columns.extend(['SFUnits', 'MFUnits'])
seleted_columns = list(hhs_emp_columns)
seleted_columns.append('PSRC_ID')

permit_df = pd.read_csv(permit_file, sep = ',', low_memory = False)
permit_df.fillna(0, inplace = True)
permit_sum_df = permit_df[seleted_columns]
base_year_LU_df = pd.read_csv(base_year_Bellevue_LU_file, sep = ',', low_memory = False)
base_year_LU_df.fillna(0, inplace = True)
base_year_LU_df = base_year_LU_df.groupby('PSRC_ID')[hhs_emp_columns].sum().reset_index()
blkgrp_lookup_df = pd.read_csv(lookup_file, sep = ',', low_memory = False)

permit_replace_df = permit_df.loc[permit_df['Action'] == 'Replace', seleted_columns]
original_parcel_df = base_year_LU_df.loc[base_year_LU_df['PSRC_ID'].isin(permit_replace_df['PSRC_ID'])]
print 'base year total dwell units: ', base_year_LU_df['SFUnits'].sum() + base_year_LU_df['MFUnits'].sum()
print 'base year dwell units in parcels to be replaced: ', original_parcel_df['SFUnits'].sum() + original_parcel_df['MFUnits'].sum()
print 'concurrency new added dwell units: ', permit_df['SFUnits'].sum() + permit_df['MFUnits'].sum()

concurrency_LU_df = base_year_LU_df.loc[~base_year_LU_df['PSRC_ID'].isin(permit_replace_df['PSRC_ID'])]
concurrency_LU_df = concurrency_LU_df.append(permit_sum_df)

print 'total dwell units in concurrency: ', concurrency_LU_df['SFUnits'].sum() + concurrency_LU_df['MFUnits'].sum()

concurrency_LU_df[['PSRC_ID', 'SFUnits', 'MFUnits']].to_csv(os.path.join(working_folder, dwellunits_output_file), sep = ',')

## bellevue occupancy rates
bel_occ_rate_df = pd.read_csv(bel_occ_file, sep = ',', low_memory = False)
bel_occ_rate_df = bel_occ_rate_df.loc[~bel_occ_rate_df['PSRC_ID'].duplicated()]

# calculate number of households
concurrency_hhs = concurrency_LU_df[['PSRC_ID', 'SFUnits', 'MFUnits']].merge(bel_occ_rate_df, how = 'left', on = 'PSRC_ID')
concurrency_hhs['SFOR_e'].fillna(1, inplace = True)
concurrency_hhs['MFOR_e'].fillna(1, inplace = True)
concurrency_hhs['SFHH'] = concurrency_hhs['SFUnits'] * concurrency_hhs['SFOR_e']
concurrency_hhs['MFHH'] = concurrency_hhs['MFUnits'] * concurrency_hhs['MFOR_e']
concurrency_hhs['total_hhs'] = concurrency_hhs['SFHH'] + concurrency_hhs['MFHH']
concurrency_hhs['total_persons'] = concurrency_hhs['SFHH'] * avg_persons_per_sfhh + concurrency_hhs['MFHH'] * avg_persons_per_mfhh
concurrency_hhs = concurrency_hhs.merge(blkgrp_lookup_df[['PSRC_ID', 'GEOID10']], how = 'left', on = 'PSRC_ID')
concurrency_hhs[['PSRC_ID', 'total_hhs', 'total_persons', 'GEOID10']].to_csv(os.path.join(working_folder, hhs_output_file), sep = ',')

base_year_hhs_by_parcel_df = pd.read_csv(base_year_hhs_by_parcel, sep = ',', low_memory = False)
print 'total hhs in base year: ', base_year_hhs_by_parcel_df['total_hhs'].sum()
print 'hhs in the pacels to be replaced: ', base_year_hhs_by_parcel_df.loc[base_year_hhs_by_parcel_df['hhparcel'].isin(concurrency_hhs['PSRC_ID']), 'total_hhs'].sum()
con_year_hhs_by_parcel_df = base_year_hhs_by_parcel_df.loc[~base_year_hhs_by_parcel_df['hhparcel'].isin(concurrency_hhs['PSRC_ID'])]
con_year_hhs_by_parcel_df = con_year_hhs_by_parcel_df.rename(columns = {'hhparcel':'PSRC_ID'})
con_year_hhs_by_parcel_df = con_year_hhs_by_parcel_df.append(concurrency_hhs[['PSRC_ID', 'total_hhs', 'total_persons']])

print 'new hhs in the parcels to be replaced: ',  concurrency_hhs['total_hhs'].sum()
print 'total hhs in the concurrency: ', con_year_hhs_by_parcel_df['total_hhs'].sum()

print 'base year total sqft: ', base_year_LU_df[emp_den_cats].sum().sum()
print 'base year total sqft in parcels to be replaced: ', original_parcel_df[emp_den_cats].sum().sum()
print 'concurrency new sqft: ', permit_df[emp_den_cats].sum().sum()
print 'total sqft in concurrency: ', concurrency_LU_df[emp_den_cats].sum().sum()

# create control file for populationsim
con_year_hhs_by_parcel_df = con_year_hhs_by_parcel_df.merge(blkgrp_lookup_df[['PSRC_ID', 'GEOID10']], how = 'left', on = 'PSRC_ID')
con_year_hhs_by_blkgrp = con_year_hhs_by_parcel_df.groupby('GEOID10').sum().reset_index()
popsim_2018_control_df = pd.read_csv(popsim_2018_control_file, sep = ',')
concurrency_control_df = popsim_2018_control_df.merge(con_year_hhs_by_blkgrp, how = 'left', left_on = 'block_group_id', right_on = 'GEOID10')
concurrency_control_df.fillna(0, inplace = True)
concurrency_control_df['hh_bg_weight'] =  concurrency_control_df['total_hhs'].astype(int)
concurrency_control_df['hh_tract_weight'] =  concurrency_control_df['total_hhs']
concurrency_control_df['pers_bg_weight'] =  concurrency_control_df['total_persons']
concurrency_control_df['pers_tract_weight'] =  concurrency_control_df['total_persons']
concurrency_control_df.drop(con_year_hhs_by_blkgrp.columns, axis = 1, inplace = True)
concurrency_control_df.hh_bg_weight = concurrency_control_df.hh_bg_weight.round().astype(int)
concurrency_control_df.hh_tract_weight = concurrency_control_df.hh_tract_weight.round().astype(int)
concurrency_control_df.pers_bg_weight = concurrency_control_df.pers_bg_weight.round().astype(int)
concurrency_control_df.pers_tract_weight = concurrency_control_df.pers_tract_weight.round().astype(int)

concurrency_control_df.to_csv(os.path.join(working_folder, popsim_control_output_file), sep = ',')

# updated COB parcels
concurrency_LU_df.to_csv(parcel_output_file, sep = ',')


print 'Done'

