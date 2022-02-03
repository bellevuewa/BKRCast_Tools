import pandas as pd
import os
import h5py
import utility

### configuration #####
### input files
working_folder = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\TFP\2033_horizon_year' 
lookup_file = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'
hhs_by_parcel = '2033_hhs_by_parcels_from_PSRC_2014_2050.csv' # output file from interpolate_hhs_and_persons_by_GEOID_btw_two_horizon_years.py
cob_du_file = '2033TFP_COB_housingunits.csv'
popsim_control_file = 'acecon0403.csv'

# TAZ level control total (households) from Kirkland and Redmond. (can be any TAZ)
hhs_control_total_by_TAZ = r"Z:\Modeling Group\BKRCast\LandUse\TFP\2033_horizonyear_TFP\Redmond_Kirkland_2033_jobs_hhs_by_tripmodel_TAZ.csv"

# output files
hhs_by_taz_comparison_file = '2033_PSRC_hhs_and_forecast_from_kik_Red_by_trip_model_TAZ_comparison.csv'
adjusted_hhs_by_parcel_file = '2033_final_hhs_by_parcel.csv'
popsim_control_output_file = r'ACS2016_controls_2033TFP_estimate.csv'
parcels_for_allocation_filename = '2033_horizon_TFP_parcels_for_allocation_local_estimate.csv'
#maybe we do not need this file. we can use an output file from prepare_land_use_step_1.py
housing_units_file = '2033TFP_COB_du.csv' 
parcels_for_allocation_filename = '2033TFP_parcels_for_allocation_local_estimate.csv'

####

avg_person_per_hh_Redmond = 2.3146
avg_person_per_hh_Kirkland = 2.2576

sf_occupancy_rate = 0.952  # from Gwen
mf_occupancy_rate = 0.895  # from Gwen
avg_persons_per_sfhh =  2.82 # from Gwen
avg_persons_per_mfhh =  2.03 # from Gwen

###


lookup_df = pd.read_csv(lookup_file, low_memory = False)
hhs_by_parcel_df = pd.read_csv(os.path.join(working_folder, hhs_by_parcel))
hhs_control_total_by_TAZ_df = pd.read_csv(hhs_control_total_by_TAZ)
cob_du_df = pd.read_csv(os.path.join(working_folder, cob_du_file))

hhs_control_total_by_TAZ_df['total_persons'] = 0
hhs_control_total_by_TAZ_df.loc[hhs_control_total_by_TAZ_df['Jurisdiction'] == 'Kirkland', 'total_persons'] = hhs_control_total_by_TAZ_df['total_hhs'] * avg_person_per_hh_Kirkland
hhs_control_total_by_TAZ_df.loc[hhs_control_total_by_TAZ_df['Jurisdiction'] == 'Redmond','total_persons'] = hhs_control_total_by_TAZ_df['total_hhs'] * avg_person_per_hh_Redmond

# get parcels within trip model Redmond and Kirkland TAZ (old taz system)
parcels_in_trip_model_TAZ_df = pd.merge(hhs_by_parcel_df[['PSRC_ID', 'total_hhs_by_parcel', 'total_persons_by_parcel']], lookup_df.loc[lookup_df['BKRTMTAZ'].notna(), ['PSRC_ID', 'Jurisdiction', 'BKRTMTAZ']], on = 'PSRC_ID', how = 'inner')
parcels_in_trip_model_TAZ_df = parcels_in_trip_model_TAZ_df.merge(hhs_control_total_by_TAZ_df[['BKRTMTAZ']], on  = 'BKRTMTAZ', how = 'inner')

hhs_by_TAZ_df = parcels_in_trip_model_TAZ_df[['BKRTMTAZ', 'total_hhs_by_parcel', 'total_persons_by_parcel']].groupby('BKRTMTAZ').sum()
hhs_by_TAZ_df = pd.merge(hhs_by_TAZ_df, hhs_control_total_by_TAZ_df.loc[hhs_control_total_by_TAZ_df['total_hhs'] >= 0, ['BKRTMTAZ', 'total_hhs', 'total_persons']], on = 'BKRTMTAZ', how = 'outer')
hhs_by_TAZ_df.fillna(value = {'total_hhs' : 0, 'total_persons' : 0}, inplace = True)
hhs_by_TAZ_df.to_csv(os.path.join(working_folder, hhs_by_taz_comparison_file), index = False)
# make a deep copy of hhs_by_parcel_df
adjusted_hhs_by_parcel_df = hhs_by_parcel_df.copy()
adjusted_hhs_by_parcel_df = adjusted_hhs_by_parcel_df.rename(columns = {'total_hhs_by_parcel': 'adj_hhs_by_parcel', 'total_persons_by_parcel':'adj_persons_by_parcel'})
adjusted_hhs_by_parcel_df = adjusted_hhs_by_parcel_df.merge(parcels_in_trip_model_TAZ_df[['PSRC_ID', 'BKRTMTAZ']], on = 'PSRC_ID', how = 'left')
# reset hhs and persons to zero in Kirkland and Redmond parcels that are not included in local estimates. We will use their local forecast.
adjusted_hhs_by_parcel_df.loc[(adjusted_hhs_by_parcel_df['Jurisdiction'] == 'KIRKLAND') & adjusted_hhs_by_parcel_df['BKRTMTAZ'].isna(), ['adj_hhs_by_parcel', 'adj_persons_by_parcel']] = 0
adjusted_hhs_by_parcel_df.loc[(adjusted_hhs_by_parcel_df['Jurisdiction'] == 'REDMOND') & adjusted_hhs_by_parcel_df['BKRTMTAZ'].isna(), ['adj_hhs_by_parcel', 'adj_persons_by_parcel']] = 0

# for a TAZ that have no hhs in PSRC erstimate but have hhs in local jurisdiction estimate, evenly distribute hhs to all parcels in that TAZ
tazs_for_evenly_distri_df = hhs_by_TAZ_df.loc[hhs_by_TAZ_df['total_hhs_by_parcel'] == 0]

for row in tazs_for_evenly_distri_df.itertuples():
    print(row.BKRTMTAZ, row.total_hhs_by_parcel, row.total_hhs)
    # find parcels within this taz
    counts = adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['BKRTMTAZ'] == row.BKRTMTAZ].shape[0]
    if counts == 0 and row.total_hhs > 0:
        print(f'TAZ {row.BKRTMTAZ} is has no parcels but has {row.total_hhs} households.')
        continue
    adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['BKRTMTAZ'] == row.BKRTMTAZ, 'adj_hhs_by_parcel'] = row.total_hhs / counts
    adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['BKRTMTAZ'] == row.BKRTMTAZ, 'adj_persons_by_parcel'] = row.total_persons / counts

# for other parcels, scale up hhs to match local jurisdiction's forecast by applying factors calculated in TAZ level
tazs_for_proportional_distri_df = hhs_by_TAZ_df.loc[hhs_by_TAZ_df['total_hhs_by_parcel'] > 0].copy()
tazs_for_proportional_distri_df['ratio_hhs'] = tazs_for_proportional_distri_df['total_hhs'] / tazs_for_proportional_distri_df['total_hhs_by_parcel']
tazs_for_proportional_distri_df['ratio_persons'] = tazs_for_proportional_distri_df['total_persons'] / tazs_for_proportional_distri_df['total_persons_by_parcel']

adjusted_hhs_by_parcel_df = adjusted_hhs_by_parcel_df.merge(tazs_for_proportional_distri_df[['BKRTMTAZ', 'ratio_hhs', 'ratio_persons']], on = 'BKRTMTAZ', how = 'left')
adjusted_hhs_by_parcel_df = adjusted_hhs_by_parcel_df.fillna(value = {'ratio_hhs' : 1, 'ratio_persons' : 1})
adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] * adjusted_hhs_by_parcel_df['ratio_hhs']
adjusted_hhs_by_parcel_df['adj_persons_by_parcel'] = adjusted_hhs_by_parcel_df['adj_persons_by_parcel'] * adjusted_hhs_by_parcel_df['ratio_persons']
adjusted_hhs_by_parcel_df.drop(columns = ['ratio_hhs', 'ratio_persons'], inplace = True)

# Replace hhs estimate with COB's forecast
# if some parcels are missing from the cob_du_df, export them for further investigation.
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

### Create control file for PopulationSim
popsim_control_df = pd.read_csv(os.path.join(working_folder, popsim_control_file), sep = ',')
hhs_by_geoid10_df =  adjusted_hhs_by_parcel_df[['GEOID10', 'adj_hhs_by_parcel', 'adj_persons_by_parcel']].groupby('GEOID10').sum()
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

bel_parcels_hhs_df = adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['Jurisdiction'] == 'BELLEVUE', ['PSRC_ID', 'adj_hhs_by_parcel', 'adj_persons_by_parcel', 'GEOID10']]
bel_parcels_hhs_df.rename(columns = {'adj_hhs_by_parcel':'total_hhs', 'adj_persons_by_parcel':'total_persons'}, inplace = True)
bel_parcels_hhs_df.to_csv(os.path.join(working_folder, parcels_for_allocation_filename), index = False)

utility.backupScripts(__file__, os.path.join(working_folder, os.path.basename(__file__)))

print('Done')

