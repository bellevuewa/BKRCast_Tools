import pandas as pd
import os
import numpy as np

# inputa
working_folder = r'Z:\Modeling Group\BKRCast\KirklandSupport\Kirkland2044Complan\preferred_2044'
kirk_2044_baseline_file = r"Z:\Modeling Group\BKRCast\KirklandSupport\Kirkland2044Complan\baseline 2044\2044_kirkland_baseline_land_use.csv"
kirk_2044_target_file = 'parcel_fixed_Kirkland_Complan_2044_target_Landuse.csv'
lookup_file = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'
baseline_parcel_file = r"Z:\Modeling Group\BKRCast\KirklandSupport\Kirkland2044Complan\baseline2044_01092024\parcels_urbansim.txt"
target_parcel_file = r"2044_kirkcomplan_target_parcels_urbansim_sync_with_popsim.txt"

# output files
ratio_file_name = '2044_jobs_target_over_baseline_ratio.csv'
baseline_no_jobs_target_has_jobs_file_name = 'baseline_no_jobs_target_has_jobs.csv'
final_target_parcel_file_name = '2044_kirkcomplan_target_parcels_urbansim_sync_with_popsim_jobs_scaled_from_baseline.txt'


print('loading...')
baseline_df = pd.read_csv(kirk_2044_baseline_file)
lookup_df = pd.read_csv(lookup_file, low_memory = False)
target_df = pd.read_csv(os.path.join(working_folder, kirk_2044_target_file))

# cleaning
baseline_df = baseline_df.merge(lookup_df[['PSRC_ID', 'BKRCastTAZ', 'Jurisdiction']], on = 'PSRC_ID', how = 'left')
baseline_df = baseline_df.loc[baseline_df['Jurisdiction'] == 'KIRKLAND']

target_df = target_df.merge(lookup_df[['PSRC_ID', 'BKRCastTAZ', 'Jurisdiction']], on = 'PSRC_ID', how = 'left')
target_df = target_df.loc[target_df['Jurisdiction'] == 'KIRKLAND']


baseline_by_TAZ_df = baseline_df.groupby('BKRCastTAZ').agg({'EMPTOT_2044':'sum', 'Jurisdiction':'first'}).reset_index()
baseline_by_TAZ_df.rename(columns= {'EMPTOT_2044':'EMPTOT_2044_Baseline'}, inplace = True)
target_by_TAZ_df = target_df.groupby('BKRCastTAZ').agg({'EMPTOT_2044':'sum', 'Jurisdiction':'first'}).reset_index()
target_by_TAZ_df.rename(columns= {'EMPTOT_2044':'EMPTOT_2044_Target'}, inplace = True)

combined_df = baseline_by_TAZ_df.merge(target_by_TAZ_df, on = 'BKRCastTAZ', how = 'outer').reset_index()
combined_df.fillna(0, inplace = True)
combined_df['ratio'] = combined_df['EMPTOT_2044_Target'] / combined_df['EMPTOT_2044_Baseline']

combined_df.to_csv(os.path.join(working_folder, ratio_file_name))

# if no jobs in baseline but has jobs in target year. ratio is inf. need to find out these special tazs.
new_land_use_TAZ = combined_df.loc[(combined_df['EMPTOT_2044_Baseline'] == 0) & (combined_df['EMPTOT_2044_Target'] > 0), 'BKRCastTAZ']
new_land_use_TAZ.to_csv(os.path.join(working_folder, baseline_no_jobs_target_has_jobs_file_name), index = False)

baseline_parcel_df = pd.read_csv(baseline_parcel_file, sep = ' ', low_memory = False)
final_target_parcel_df = baseline_parcel_df.copy()
final_target_parcel_df = final_target_parcel_df.merge(combined_df[['BKRCastTAZ', 'ratio']], left_on = 'TAZ_P', right_on = 'BKRCastTAZ', how = 'left')
final_target_parcel_df['ratio'].fillna(1, inplace = True)
# for ratio == inf (baseline job is zero but target job > 0.) For now change the ratio to 1. Will handle this situation later
final_target_parcel_df['ratio'].replace(np.inf, 1, inplace = True)

job_fields = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPOTH_P', 'EMPRET_P', 'EMPSVC_P']
for job in job_fields:
    final_target_parcel_df[job] = final_target_parcel_df[job] * final_target_parcel_df['ratio']

# for those baseline job == 0 but target job > 0
# replace subset of final_target_parcel_df with the matching subset of target_parcel_df
target_parcel_df = pd.read_csv(os.path.join(working_folder, target_parcel_file), sep = ' ', low_memory = False)
final_target_parcel_df.loc[final_target_parcel_df['TAZ_P'].isin(new_land_use_TAZ), job_fields] = target_parcel_df.loc[final_target_parcel_df['TAZ_P'].isin(new_land_use_TAZ), job_fields]

final_target_parcel_df['EMPTOT_P'] = 0
for job in job_fields:
    final_target_parcel_df[job] = final_target_parcel_df[job].round(0).astype(int)
    final_target_parcel_df['EMPTOT_P'] += final_target_parcel_df[job]

final_target_parcel_df.drop(columns = ['BKRCastTAZ', 'ratio'], inplace = True)
final_target_parcel_df.to_csv(os.path.join(working_folder, final_target_parcel_file_name), sep = ' ', index = False)

print('Done')

