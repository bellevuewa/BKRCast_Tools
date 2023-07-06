import pandas as pd
import os
import h5py
import math
import utility


'''
This program will descide how many households each parcel should have. 
'''
### configuration #####
### input files
working_folder = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2021baseyear' 
lookup_file = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'
hhs_by_parcel = '2021_hhs_by_parcels_from_PSRC_2014_2050.csv' # output file from interpolate_hhs_and_persons_by_GEOID_btw_two_horizon_years.py
cob_du_file = '2021_COB_housingunits.csv'
popsim_control_file = 'acecon0403.csv'
# ofm_control_file = '2019_OFM_estimate.csv'  no need to use OFM as control total for now.

# TAZ level control total (households) from Kirkland and Redmond. (can be any TAZ)
hhs_control_total_by_TAZ = ''

# output files
adjusted_hhs_by_parcel_file = '2021_final_hhs_by_parcel.csv'
popsim_control_output_file = r'ACS2016_controls_2021_estimate.csv'
parcels_for_allocation_filename = '2021_baseyear_parcels_for_allocation_local_estimate.csv'
#maybe we do not need this file. we can use an output file from prepare_land_use_step_1.py
housing_units_file = '2021_COB_du.csv' 

sf_occupancy_rate = 0.952  # from Gwen
mf_occupancy_rate = 0.895  # from Gwen
avg_persons_per_sfhh =  2.82 # from Gwen
avg_persons_per_mfhh =  2.03 # from Gwen

lookup_df = pd.read_csv(lookup_file, low_memory = False)
hhs_by_parcel_df = pd.read_csv(os.path.join(working_folder, hhs_by_parcel))
cob_du_df = pd.read_csv(os.path.join(working_folder, cob_du_file))

adjusted_hhs_by_parcel_df = hhs_by_parcel_df.copy()
adjusted_hhs_by_parcel_df = adjusted_hhs_by_parcel_df.rename(columns = {'total_hhs_by_parcel': 'adj_hhs_by_parcel', 'total_persons_by_parcel':'adj_persons_by_parcel'})

cob_total_parcels_df = hhs_by_parcel_df.loc[hhs_by_parcel_df['Jurisdiction'] == 'BELLEVUE']
cob_parcels_provided = cob_du_df.shape[0]
if cob_total_parcels_df.shape[0] != cob_parcels_provided:
    print('COB forecast does not cover all parcels. Please cehck the missing parcel files for further investigation.')
    cob_missing_parcels_df = cob_total_parcels_df.loc[~cob_total_parcels_df['PSRC_ID'].isin(cob_du_df['PSRC_ID'])]
    cob_missing_parcels_df.to_csv(os.path.join(working_folder, 'cob_missing_parcels.csv'), index = False)
    print(f'{cob_missing_parcels_df.shape[0]} parcels are missing in {cob_du_file}.')

cob_du_df['sfhhs'] = cob_du_df['SFUnits'] * sf_occupancy_rate 
cob_du_df['mfhhs'] = cob_du_df['MFUnits'] * mf_occupancy_rate
cob_du_df['sfpersons'] = cob_du_df['sfhhs'] * avg_persons_per_sfhh
cob_du_df['mfpersons'] = cob_du_df['mfhhs'] * avg_persons_per_mfhh
cob_du_df['cobflag'] = 'cob'

adjusted_hhs_by_parcel_df = adjusted_hhs_by_parcel_df.merge(cob_du_df[['PSRC_ID', 'cobflag', 'sfhhs', 'mfhhs', 'sfpersons', 'mfpersons']], on = 'PSRC_ID', how = 'left')
# reset hhs and persons in all COB parcels to zero. Only use local forecast.
adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['Jurisdiction'] == 'BELLEVUE', ['adj_hhs_by_parcel', 'adj_persons_by_parcel']] = 0

# it is importand to use cobflag rather than Jurisdiction, because (hhs and persons in) parcels flagged by cobflag are provided by COB staff.
adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['cobflag'] == 'cob', 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['sfhhs'] + adjusted_hhs_by_parcel_df['mfhhs']
adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['cobflag'] == 'cob', 'adj_persons_by_parcel'] = adjusted_hhs_by_parcel_df['sfpersons'] + adjusted_hhs_by_parcel_df['mfpersons']


### hhs should not be fractions, so round the hhs to integer, controlled by BKRCastTAZ
### we will use the rounded hhs by parcel as guidance to allocate synthetic households. So controlled rounding is very important here, otherwise we will have more or less 
### total households due to rounding error, and we cannot allocate a fraction of a household.
### we rely on this rounded hhs by parcel to generate control file (in census block group level) for populationsim
### to get correct number of persons by block group, instead of doing controlled rounding, we simply summarize persons by block group before controlled rounding on hhs.
adj_persons_by_GEOID10 = adjusted_hhs_by_parcel_df[['GEOID10', 'adj_persons_by_parcel']].groupby('GEOID10').sum()
total_hhs_before_rounding = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'].sum()

print('Rounding households to integer. Controlled by BKRCastTAZ subtotal....')
# in ACS 2016 there is no hhs in Census block group 530619900020, but in PSRC's future hhs forecast there are. We need to relocate these households from this blockgroup to  
# block group 530610521042 but keep them in the same BKRCastTAZ. 
special_parcels_flag = (adjusted_hhs_by_parcel_df['GEOID10'] == 530619900020) & (adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] > 0)
special_hhs_by_TAZ = adjusted_hhs_by_parcel_df.loc[special_parcels_flag, ['BKRCastTAZ', 'adj_hhs_by_parcel', 'adj_persons_by_parcel']].groupby('BKRCastTAZ').sum().reset_index()
# move all persons in 530619900020 to 530610521042
adj_persons_by_GEOID10.loc[530610521042, 'adj_persons_by_parcel'] += adj_persons_by_GEOID10.loc[530619900020, 'adj_persons_by_parcel']


# move hhs from parcels in 530619900020 to parcels in 530610521042 && same TAZ
for row in special_hhs_by_TAZ.itertuples():
    mf_parcels_flag = (adjusted_hhs_by_parcel_df['GEOID10'] == 530610521042) & (adjusted_hhs_by_parcel_df['BKRCastTAZ'] == row.BKRCastTAZ) & (adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] > 1)
    mf_parcels_count = adjusted_hhs_by_parcel_df.loc[mf_parcels_flag].shape[0]
    if mf_parcels_count > row.adj_hhs_by_parcel:
        selected_ids = adjusted_hhs_by_parcel_df.sample(n = int(row.adj_hhs_by_parcel))['PSRC_ID']
        adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['PSRC_ID'].isin(selected_ids), 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] + 1
    else:
        increase = math.floor(row.adj_hhs_by_parcel / mf_parcels_count)
        adjusted_hhs_by_parcel_df.loc[mf_parcels_flag, 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] + increase
        diff = row.adj_hhs_by_parcel - increase * mf_parcels_count
        selected_ids = adjusted_hhs_by_parcel_df.sample(n = 1)['PSRC_ID']
        adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['PSRC_ID'].isin(selected_ids), 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] + diff
    adjusted_hhs_by_parcel_df.loc[special_parcels_flag, ['adj_hhs_by_parcel', 'adj_persons_by_parcel']] = 0


adj_hhs_by_BKRCastTAZ = adjusted_hhs_by_parcel_df[['BKRCastTAZ', 'adj_hhs_by_parcel']].groupby('BKRCastTAZ').sum().round(0).astype(int)
controlled_taz_hhs = adj_hhs_by_BKRCastTAZ.reset_index().to_dict('record')

for record in controlled_taz_hhs:
    adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['BKRCastTAZ'] == record['BKRCastTAZ'], 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'].round(0)
    subtotal = adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['BKRCastTAZ'] == record['BKRCastTAZ'], 'adj_hhs_by_parcel'].sum()
    diff = subtotal - record['adj_hhs_by_parcel']
    mf_parcel_flags = (adjusted_hhs_by_parcel_df['BKRCastTAZ'] == record['BKRCastTAZ']) & (adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] >= 2)
    sf_parcel_flags = (adjusted_hhs_by_parcel_df['BKRCastTAZ'] == record['BKRCastTAZ']) & (adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] == 1)
    mf_parcels_count = adjusted_hhs_by_parcel_df.loc[mf_parcel_flags].shape[0]
    sf_parcels_count = adjusted_hhs_by_parcel_df.loc[sf_parcel_flags].shape[0]
    if diff > 0: 
        # too many hhs in this TAZ after rounding. need to bring down subtotal 
        # start from mf parcels. 
        if mf_parcels_count > 0:
            if mf_parcels_count < diff:
                adjusted_hhs_by_parcel_df.loc[mf_parcel_flags, 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] - 1
                diff = diff - mf_parcels_count
            else: # number of mf parcels are more than diff,  randomly pick diff number of mf parcels and reduce adj_hhs_by_parcel in each parcel  by 1
                selected_parcel_ids = adjusted_hhs_by_parcel_df.loc[mf_parcel_flags].sample(n = int(diff))['PSRC_ID']
                adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['PSRC_ID'].isin(selected_parcel_ids), 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] - 1
                diff = 0
        # if rounding issue is not resolved yet, deal with it in sf parcel
        if (diff > 0) and (sf_parcels_count > 0):
            if sf_parcels_count < diff: 
                adjusted_hhs_by_parcel_df.loc[sf_parcel_flags, 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] - 1
                diff = diff - sf_parcels_count
            else: # number of sf parcels are more than diff, randomly pick diff number of sf parcels and reduce adj_hhs_by_parcel in each by 1 (set to zero)
                selected_parcel_ids = adjusted_hhs_by_parcel_df.loc[sf_parcel_flags].sample(n = int(diff))['PSRC_ID']
                adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['PSRC_ID'].isin(selected_parcel_ids), 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] - 1
                diff = 0
         # last option, if rounding issue is still not resolved, 
        if diff > 0:
            print(f"TAZ {record['BKRCastTAZ']}: rounding issue is not resolved. Difference is {diff}")
    elif diff < 0:
        # too less hhs in this TAZ after rounding. need to increase subtotal
        if mf_parcels_count > 0:
            # evenly distribute diff to all mf parcel, then the remaining to a ramdomly selected one
            if mf_parcels_count < abs(diff):
                increase = math.floor(abs(diff) / mf_parcels_count)
                adjusted_hhs_by_parcel_df.loc[mf_parcel_flags, 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] + increase
                diff = diff + increase * mf_parcels_count
                selected_parcel_ids = adjusted_hhs_by_parcel_df.loc[mf_parcel_flags].sample(n = 1)['PSRC_ID']
                adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['PSRC_ID'].isin(selected_parcel_ids), 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] + abs(diff)
                diff = diff + abs(diff)
            else:
                selected_parcel_ids = adjusted_hhs_by_parcel_df.loc[mf_parcel_flags].sample(n = int(abs(diff)))['PSRC_ID']
                adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['PSRC_ID'].isin(selected_parcel_ids), 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] + 1
                diff = diff + abs(diff)
                
        else: # if no mf parcel is available, add diff to sf parcels
            if sf_parcels_count > 0:
                if sf_parcels_count < abs(diff):
                    increase = math.floor(abs(diff) / sf_parcels_count)
                    adjusted_hhs_by_parcel_df.loc[sf_parcel_flags, 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] + increase
                    diff = diff + increase * sf_parcels_count
                    selected_parcel_ids = adjusted_hhs_by_parcel_df.loc[sf_parcel_flags].sample(n = 1)['PSRC_ID']
                    adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['PSRC_ID'].isin(selected_parcel_ids), 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] + abs(diff)
                    diff = diff + abs(diff)
                else:
                    selected_parcel_ids = adjusted_hhs_by_parcel_df.loc[sf_parcel_flags].sample(n = int(abs(diff)))['PSRC_ID']
                    adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['PSRC_ID'].isin(selected_parcel_ids), 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] + 1
                    diff = diff + abs(diff)
            else:  # last option, add diff to a ramdomly selected parcel
                applicable_parcels_flags = (adjusted_hhs_by_parcel_df['BKRCastTAZ'] == record['BKRCastTAZ'])
                selected_parcel_ids = adjusted_hhs_by_parcel_df.loc[applicable_parcels_flags].sample(n = 1)['PSRC_ID']
                adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['PSRC_ID'].isin(selected_parcel_ids), 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] + abs(diff)

adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'].astype(int)
total_hhs_after_rounding = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'].sum()
print('Controlled rounding is complete. ')
print(f'Total hhs before rounding: {total_hhs_before_rounding}, after: {total_hhs_after_rounding}')
# export adjusted hhs by parcel to file
adjusted_hhs_by_parcel_df[['PSRC_ID', 'GEOID10', 'BKRCastTAZ','adj_hhs_by_parcel']].rename(columns = {'adj_hhs_by_parcel':'total_hhs'}).to_csv(os.path.join(working_folder, adjusted_hhs_by_parcel_file), index = False)
        

### Create control file for PopulationSim
popsim_control_df = pd.read_csv(os.path.join(working_folder, popsim_control_file), sep = ',')
hhs_by_geoid10_df =  adjusted_hhs_by_parcel_df[['GEOID10', 'adj_hhs_by_parcel']].groupby('GEOID10').sum()
hhs_by_geoid10_df = hhs_by_geoid10_df.merge(adj_persons_by_GEOID10, left_index = True, right_index = True, how = 'left')
hhs_by_geoid10_df.fillna(0, inplace = True)
popsim_control_df = popsim_control_df.merge(hhs_by_geoid10_df, left_on = 'block_group_id', right_on = 'GEOID10', how = 'left')
error_blkgrps_df = popsim_control_df.loc[popsim_control_df.isna().any(axis = 1)]
if error_blkgrps_df.shape[0] > 0:
    print('Some blockgroups are missing values. Please check the error_census_blockgroup.csv')
    print('The missing values are all replaced with zeros.')
    error_blkgrps_df.to_csv(os.path.join(working_folder, 'error_census_blockgroup.csv'), index = False)

popsim_control_df.fillna(0, inplace = True)
popsim_control_df['hh_bg_weight'] = popsim_control_df['adj_hhs_by_parcel'].round(0).astype(int)
popsim_control_df['hh_tract_weight'] = popsim_control_df['adj_hhs_by_parcel'].round(0).astype(int)
popsim_control_df['pers_bg_weight'] = popsim_control_df['adj_persons_by_parcel'].round(0).astype(int)
popsim_control_df['pers_tract_weight'] = popsim_control_df['adj_persons_by_parcel'].round(0).astype(int)
popsim_control_df.drop(hhs_by_geoid10_df.columns, axis = 1, inplace = True)
popsim_control_df.to_csv(os.path.join(working_folder, popsim_control_output_file), index = False)

total_hhs = popsim_control_df['hh_bg_weight'].sum()
total_persons = popsim_control_df['pers_bg_weight'].sum()
print(f'{total_hhs} households, {total_persons} persons are in the control file.')

### generate other support files for parcelization
bel_parcels_du_df = cob_total_parcels_df[['PSRC_ID']].merge(cob_du_df[['PSRC_ID', 'SFUnits', 'MFUnits']], on = 'PSRC_ID', how = 'left').fillna(0)
bel_parcels_du_df.to_csv(os.path.join(working_folder, housing_units_file), index = False)

bel_parcels_hhs_df = adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['Jurisdiction'] == 'BELLEVUE'][['PSRC_ID', 'adj_hhs_by_parcel', 'sfhhs', 'mfhhs', 'adj_persons_by_parcel', 'Jurisdiction', 'GEOID10']]
bel_parcels_hhs_df.rename(columns = {'adj_hhs_by_parcel':'total_hhs', 'adj_persons_by_parcel':'total_persons'}, inplace = True)
bel_parcels_hhs_df.to_csv(os.path.join(working_folder, parcels_for_allocation_filename), index = False)

utility.backupScripts(__file__, os.path.join(working_folder, os.path.basename(__file__)))


print('Done')