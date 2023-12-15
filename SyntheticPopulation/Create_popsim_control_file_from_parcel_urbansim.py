import pandas as pd
import h5py
import numpy as np
import os
import utility

# input configuration
working_folder = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\KirklandSupport\Kirkland2044Complan\baseline2044'
local_estimate_parcel_file = r'Z:\Modeling Group\BKRCast\KirklandSupport\Kirkland2044Complan\baseline 2044\Adjusted_2044_Kirkland_baseline_parcels.csv'
control_template_file = 'acecon0403.csv'
hh_person_file = '2044_interpolated_synthetic_population_from_SC.h5'
lookup_filename = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'

# output configuration
control_file_name = 'ACS2016_controls_2044_kirkcomplan_baseline_estimate.csv'
parcels_for_allocation_filename = '2044_kirkcomplan_baseline_parcels_for_allocation_local_estimate.csv'

sf_occupancy_rate = 0.952  # from Gwen
mf_occupancy_rate = 0.895  # from Gwen
avg_persons_per_sfhh =  2.82 # from Gwen
avg_persons_per_mfhh =  2.03 # from Gwen

# load hhs and persons estimated by local jurisdiction
local_parcels_df = pd.read_csv(os.path.join(working_folder, local_estimate_parcel_file))
local_hhs_df = local_parcels_df[['PARCELID', 'SF_2044', 'MF_2044']].copy()
local_hhs_df['TotHhs_2044'] = local_hhs_df['SF_2044'] + local_hhs_df['MF_2044']
local_hhs_df['SFPersons_2044'] = local_hhs_df['SF_2044'] * avg_persons_per_sfhh
local_hhs_df['MFPersons_2044'] = local_hhs_df['MF_2044'] * avg_persons_per_mfhh
local_hhs_df['TotPersons_2044'] = local_hhs_df['SFPersons_2044'] + local_hhs_df['MFPersons_2044']
local_hhs_df.set_index('PARCELID', inplace = True)

# local popsim h5
hdf_file = h5py.File(os.path.join(working_folder, hh_person_file), "r")
hh_df = utility.h5_to_df(hdf_file, 'Household')
hhs_parcel_df = hh_df[['hhparcel', 'hhsize', 'hhexpfac']].groupby('hhparcel').sum()
hdf_file.close()
hhs_parcel_df.rename(columns = {'hhexpfac':'TotHhs_2044', 'hhsize':'TotPersons_2044'}, inplace = True)

# replace with local estimate. 
# do not use pd.update(). Because some parcels in h5 do not have hhs but they do in local estimate.
before_df = hhs_parcel_df.loc[hhs_parcel_df.index.isin(local_hhs_df.index)]
print(f'Before update in local jurisdiction, hhs = {before_df["TotHhs_2044"].sum()} persons = {before_df["TotPersons_2044"].sum()}')
updated_hhs_parcel_df = hhs_parcel_df[~hhs_parcel_df.index.isin(local_hhs_df.index)].copy()
updated_hhs_parcel_df = pd.concat([updated_hhs_parcel_df, local_hhs_df[['TotHhs_2044', 'TotPersons_2044']]])
after_df = updated_hhs_parcel_df.loc[updated_hhs_parcel_df.index.isin(local_hhs_df.index)]
print(f'After update in local jurisdiction, hhs = {after_df["TotHhs_2044"].sum()} persons = {after_df["TotPersons_2044"].sum()}')


# summarize parcel hhs by GEOID10
lookup_df = pd.read_csv(lookup_filename, low_memory=False)
updated_hhs_parcel_df = updated_hhs_parcel_df.merge(lookup_df[['PSRC_ID','BKRCastTAZ', 'BKRTMTAZ', 'GEOID10']], left_index = True, right_on = 'PSRC_ID', how = 'left')
updated_hhs_summary_by_GEOID10 = updated_hhs_parcel_df[['GEOID10', 'TotHhs_2044', 'TotPersons_2044']].groupby('GEOID10').sum()
#blockgroup 530619900020 and 530619901000 have no hhs and pop in 2016 ACS, but they have in 2035 PSRC's estimate
#move these hhs and pops to blockgroup 530610521042.
updated_hhs_summary_by_GEOID10.loc[530610521042] = updated_hhs_summary_by_GEOID10.loc[530610521042] + updated_hhs_summary_by_GEOID10.loc[530619900020]
updated_hhs_summary_by_GEOID10.loc[530619900020] = 0

updated_hhs_summary_by_GEOID10['TotPersons_2044'] = updated_hhs_summary_by_GEOID10['TotPersons_2044'].round(0).astype(int)
updated_hhs_summary_by_GEOID10.rename(columns = {'TotHhs_2044':'hh_bg_weight', 'TotPersons_2044':'pers_bg_weight'}, inplace = True)

# create popsim control file 
control_df = pd.read_csv(os.path.join(working_folder, control_template_file), index_col = 'block_group_id')
control_df.update(updated_hhs_summary_by_GEOID10)
control_df['hh_tract_weight'] = control_df['hh_bg_weight']
control_df['pers_tract_weight'] = control_df['pers_bg_weight']
control_df.to_csv(os.path.join(working_folder, control_file_name), index = True)

# create final parcel hhs file for allocating hhs to parcels
updated_hhs_parcel_df[['PSRC_ID', 'GEOID10', 'BKRCastTAZ', 'BKRTMTAZ', 'TotHhs_2044']].rename(columns = {'TotHhs_2044':'total_hhs'}).to_csv(os.path.join(working_folder, parcels_for_allocation_filename), index = False)

utility.backupScripts(__file__, os.path.join(working_folder, os.path.basename(__file__)))
print('Done')
