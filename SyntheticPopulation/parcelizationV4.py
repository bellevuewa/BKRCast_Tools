from xml.dom import HIERARCHY_REQUEST_ERR
import pandas as pd
import numpy as np
import os, sys
sys.path.append(os.getcwd())
import utility
import h5py

'''
This program takes synthetic households and synthetic persons( from PopulationSim) as inputs,
and allocates them to parcels, under the guidance of parcels_for_allocation_filename which is an output file from 
Prepare_Hhs_for_future_using_KR_oldTAZ_COB_parcel_forecast.py or prepare_hhs_for_baseyear_using_ofm.py. It also reformats household and person
data columns to match BKRCast input requirement. The output, h5_file_name, can be directly loaded into BKRCast.

Number of households per parcel in synthetic population should be consistent with the parcel file. It can be done by calling 
sync_population_parcel.py. 

3/9/2022
upgrade to python 3.7

5/13/2026
Major improvement:
- Much faster parcel allocation
- No concat inside loops
- Vectorized person processing
- Stable random parcel assignment
- Reduced memory usage
- Safer error handling
'''


###############Start of configuration
working_folder = r'I:\Modeling and Analysis Group\09_IndividualFolders\Hu Dong\2025SynPop'
synthetic_households_file_name = 'synthetic_households.csv'
synthetic_population_file_name = 'synthetic_persons.csv'

# number of hhs per parcel
parcels_for_allocation_filename = r"2025_final_hhs_by_parcel.csv"

## output
updated_hhs_file_name = 'updated_2025_baseyear_synthetic_households.csv'
updated_persons_file_name = 'updated_2025_baseyear_synthetic_persons.csv'
h5_file_name = '2025_baseyear_hh_and_persons.h5'

############## End of configuration
   
###
print('Loading....')
hhs_df = pd.read_csv(os.path.join(working_folder, synthetic_households_file_name))
hhs_df['hhparcel'] = 0
if 'VEH' in hhs_df.columns:
    hhs_df.rename(columns = {'VEH': 'hhvehs'}, inplace = True)
hhs_by_GEOID10 = hhs_df[['block_group_id', 'hhexpfac']].groupby('block_group_id').sum()

parcels_for_allocation_df = pd.read_csv(os.path.join(working_folder, parcels_for_allocation_filename))
# remove any blockgroup ID is Nan.
all_blcgrp_ids = hhs_df['block_group_id'].unique()
mask = np.isnan(all_blcgrp_ids)
all_blcgrp_ids = sorted(all_blcgrp_ids[~mask])

# special treatment on GEOID10 530619900020. Since in 2016 ACS no hhs lived in this census blockgroup, when creating popsim control file
# we move all hhs in this blockgroup to 530610521042. We need to do the same thing when we allocate hhs to parcels.
parcels_for_allocation_df.loc[(parcels_for_allocation_df['GEOID10'] == 530619900020) & (parcels_for_allocation_df['total_hhs'] > 0), 'GEOID10'] = 530610521042
parcels_for_allocation_df = parcels_for_allocation_df.loc[parcels_for_allocation_df['total_hhs'] > 0].copy()

parcel_groups = {k: v.copy() for k, v in parcels_for_allocation_df.groupby('GEOID10')}
hh_groups = {k: v.copy() for k, v in hhs_df.groupby('block_group_id')}

hhs_by_GOEID10 = hhs_df.groupby('block_group_id')[['hhexpfac']].sum()

final_hhs_list = []

print('Allocating households to parcels...')

for idx, blcgrpid in enumerate(all_blcgrp_ids):
    if idx % 100 == 0:
        print(f'{idx} block group processed.')

    if blcgrpid not in parcel_groups:
        print(f'No parcel records for GEOID10 {blcgrpid}')
        continue

    if blcgrpid not in hh_groups:
        print(f'No households for GEOID10 {blcgrpid}')
        continue

    parcels_in_GEOID10_df = parcel_groups[blcgrpid]
    selected_hhs_df = hh_groups[blcgrpid].copy()

    control_total = int(parcels_in_GEOID10_df['total_hhs'].sum())

    numhhs_avail_for_alloc = int(selected_hhs_df['hhexpfac'].sum())

    # create repeated parcel list
    parcel_ids = np.repeat(parcels_in_GEOID10_df['PSRC_ID'].to_numpy(), parcels_in_GEOID10_df['total_hhs'].astype(int).to_numpy())

    allocation_size = min(len(parcel_ids), selected_hhs_df.shape[0])

    selected_hhs_df.iloc[:allocation_size, selected_hhs_df.columns.get_loc('hhparcel')] = parcel_ids[:allocation_size]

    unallocated_num = (numhhs_avail_for_alloc - control_total)

    if unallocated_num < 0: 
        print(f'Error GEOID10 {blcgrpid}: parcel control {control_total} is greater than available hhs {numhhs_avail_for_alloc}. ')
        continue

    if unallocated_num > 0:
        valid_pids = parcels_in_GEOID10_df['PSRC_ID']

        if len(valid_pids) == 0:
            print(f'Warning: No valid parcels for unallocated hhs in GEOID10 {blcgrpid}. Unallocated hhs: {unallocated_num}')
            continue

        random_picked_pids = valid_pids.sample(n = unallocated_num, replace = True).to_numpy()

        start = allocation_size
        end = min(allocation_size + unallocated_num, selected_hhs_df.shape[0])
        selected_hhs_df.iloc[start:end, selected_hhs_df.columns.get_loc('hhparcel')] = random_picked_pids[: end - start]

    final_hhs_list.append(selected_hhs_df)

    print(f'GEOID10 {blcgrpid}: control: {control_total} available: {numhhs_avail_for_alloc} allocated: {allocation_size}')

print('Concatenating households...')
final_hhs_df = pd.concat(final_hhs_list, ignore_index = True, sort = False)

print('Adding TAZ info...')
parcel_lookup = parcels_for_allocation_df[['PSRC_ID', 'BKRCastTAZ']].drop_duplicates('PSRC_ID')
final_hhs_df = final_hhs_df.merge(parcel_lookup, how='left', left_on='hhparcel', right_on='PSRC_ID')
final_hhs_df.rename(columns={'BKRCastTAZ': 'hhtaz'}, inplace=True)
final_hhs_df.drop(columns=['PSRC_ID'], inplace=True, errors='ignore')


print('processing persons...')
### process other attributes to match required columns
pop_df = pd.read_csv(os.path.join(working_folder, synthetic_population_file_name)) 
pop_df.rename(columns={'household_id':'hhno', 'SEX':'pgend'}, inplace = True)
pop_df.sort_values(by = 'hhno', inplace = True)

# initialize columns
pop_df['pdairy'] = -1
pop_df['ppaidprk'] = -1
pop_df['psexpfac'] = 1
pop_df['pspcl'] = -1
pop_df['pstaz'] = -1
pop_df['pptyp'] = -1
pop_df['ptpass'] = -1
pop_df['puwarrp'] = -1
pop_df['puwdepp'] = -1
pop_df['puwmode'] = -1
pop_df['pwpcl'] = -1
pop_df['pwtaz'] = -1

print('assigning person numbers...')
pop_df['pno'] = pop_df.groupby('hhno').cumcount() + 1

print('processing person types...')
pop_df['WKW'] = pop_df['WKW'].fillna(-1)
pop_df['pstyp'] = pop_df['pstyp'].fillna(-1)
ages = pop_df['pagey']
# full time and part time workers: WKW from PUMS data
fullworkers=[1, 2]
partworkers=[3, 4, 5, 6]
nonworkers=[-1]
fullstudents = list(range(3, 17))
nonstudents = [-1, 0, 1, 2]
pp5=[15, 16]
pp6=[13, 14]
pp7=list(range(2, 13))
pp8=[1]

pop_df['pwtyp'] = 0

pop_df.loc[pop_df['WKW'].isin(fullworkers), 'pwtyp'] = 1
pop_df.loc[pop_df['WKW'].isin(partworkers), 'pwtyp'] = 2
pop_df.loc[pop_df['pstyp'].isin(nonstudents), 'pstyp'] = 0
pop_df.loc[pop_df['pstyp'].isin(fullstudents), 'pstyp'] = 1
pop_df.loc[pop_df['WKW'].isin(partworkers) & pop_df['pstyp'] == 1, 'pstyp'] = 2
pop_df['pptyp'] = 4
mask_nonworkers = pop_df['WKW'].isin(nonworkers) 
pop_df.loc[mask_nonworkers & (ages >= 65), 'pptyp'] = 3
pop_df.loc[mask_nonworkers & (ages.between(16, 64)), 'pptyp'] = 4 # inclusive on both ends
pop_df.loc[mask_nonworkers & (ages.between(5, 15)), 'pptyp'] = 7
pop_df.loc[mask_nonworkers & (ages < 5), 'pptyp'] = 8
pop_df.loc[pop_df['WKW'].isin(fullworkers), 'pptyp'] = 1
pop_df.loc[pop_df['WKW'].isin(partworkers), 'pptyp'] = 2
pop_df.loc[pop_df['pstyp'].isin(pp5), 'pptyp'] = 5
pop_df.loc[pop_df['pstyp'].isin(pp6), 'pptyp'] = 6
pop_df.loc[pop_df['pstyp'].isin(pp7), 'pptyp'] = 7
pop_df.loc[pop_df['pstyp'].isin(pp8), 'pptyp'] = 8

print('updating household size...')
hhsize_df = pop_df.groupby('hhno')[['psexpfac']].sum().reset_index()
final_hhs_df.rename(columns = {'household_id': 'hhno'}, inplace = True)
final_hhs_df = final_hhs_df.merge(hhsize_df, how = 'inner', on = 'hhno')
final_hhs_df['hhsize'] = final_hhs_df['psexpfac']
final_hhs_df.drop(columns = ['psexpfac'], inplace = True)

dropcols = ['block_group_id', 'hh_id', 'PUMA', 'WKW']
pop_df.drop(columns = dropcols, inplace = True, errors = 'ignore')
if 'hownrent' in final_hhs_df.columns:
    final_hhs_df.drop(columns = ['hownrent'], inplace = True)

final_hhs_df['hownrent'] = -1
pop_df = pop_df.loc[pop_df['hhno'].isin(final_hhs_df['hhno'])]

print('exporting h5 file...')
output_h5_file = h5py.File(os.path.join(working_folder, h5_file_name), 'w')
utility.df_to_h5(final_hhs_df, output_h5_file, 'Household')
utility.df_to_h5(pop_df, output_h5_file, 'Person')
output_h5_file.close()

print('exporting csv files...')
pop_df.to_csv(os.path.join(working_folder, updated_persons_file_name), index = False)
final_hhs_df.to_csv(os.path.join(working_folder, updated_hhs_file_name), index = False)
utility.backupScripts(__file__, os.path.join(working_folder, os.path.basename(__file__)))

print('Total census block groups: ', len(all_blcgrp_ids))
print('Final number of households: ', final_hhs_df.shape[0])
print('Final number of persons: ', pop_df.shape[0])
print('Done')
