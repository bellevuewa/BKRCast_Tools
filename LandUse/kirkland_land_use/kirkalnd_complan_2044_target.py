import pandas as pd
import os
import numpy as np
import utility


# Use this script to generate land use parcel file for Kirkland Target (complan preferred alternative) by scaling up the baseline land use.
# The target control (by taz) are calculated from a generalized target parcel urbansim file which is an output from integerize_parcel_land_use.py.
# The reason we do this scaling method is to maintain consistency by job category.

# inputa
working_folder = r'Z:\Modeling Group\BKRCast\KirklandSupport\Kirkland2044Complan\preferred_2044'
lookup_file = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'
baseline_parcel_file = r"Z:\Modeling Group\BKRCast\KirklandSupport\Kirkland2044Complan\baseline2044_01092024\parcels_urbansim.txt"
target_parcel_file = r"2044_kirkcomplan_target_parcels_urbansim.txt"

# output files
ratio_file_name = '2044_jobs_target_over_baseline_ratio.csv'
baseline_no_jobs_target_has_jobs_file_name = 'baseline_no_jobs_target_has_jobs.csv'
final_target_parcel_file_name = '2044_kirkcomplan_target_parcels_urbansim_jobs_scaled_from_baseline.txt'


print('loading...')
baseline_df = pd.read_csv(baseline_parcel_file, sep = ' ', low_memory = False)
lookup_df = pd.read_csv(lookup_file, low_memory = False)
target_df = pd.read_csv(os.path.join(working_folder, target_parcel_file), sep = ' ', low_memory = False)

baseline_df = baseline_df.merge(lookup_df[['PSRC_ID', 'Jurisdiction']], left_on = 'PARCELID', right_on = 'PSRC_ID', how = 'left')
kirk_baseline_df = baseline_df.loc[baseline_df['Jurisdiction'] == 'KIRKLAND']

target_df = target_df.merge(lookup_df[['PSRC_ID', 'Jurisdiction']], left_on = 'PARCELID', right_on = 'PSRC_ID', how = 'left')
kirk_target_df = target_df.loc[target_df['Jurisdiction'] == 'KIRKLAND']

# create control total by TAZ
baseline_by_TAZ_df = kirk_baseline_df.groupby('TAZ_P').agg({'EMPTOT_P':'sum', 'Jurisdiction':'first'}).reset_index()
baseline_by_TAZ_df.rename(columns= {'EMPTOT_P':'EMPTOT_2044_Baseline'}, inplace = True)
target_by_TAZ_df = kirk_target_df.groupby('TAZ_P').agg({'EMPTOT_P':'sum', 'Jurisdiction':'first'}).reset_index()
target_by_TAZ_df.rename(columns= {'EMPTOT_P':'EMPTOT_2044_Target'}, inplace = True)

combined_df = baseline_by_TAZ_df.merge(target_by_TAZ_df, left_on = 'TAZ_P', right_on = 'TAZ_P', how = 'outer').reset_index()
combined_df.fillna(0, inplace = True)
combined_df['EMPTOT_2044_Baseline'] = combined_df['EMPTOT_2044_Baseline'].round(0).astype(int)
combined_df['EMPTOT_2044_Target'] = combined_df['EMPTOT_2044_Target'].round(0).astype(int)

combined_df['ratio'] = combined_df['EMPTOT_2044_Target'] / combined_df['EMPTOT_2044_Baseline']
combined_df.to_csv(os.path.join(working_folder, ratio_file_name))

# if no jobs in baseline but has jobs in target year. ratio is inf. need to find out these special tazs.
special_TAZ = combined_df.loc[(combined_df['EMPTOT_2044_Baseline'] == 0) & (combined_df['EMPTOT_2044_Target'] > 0), 'TAZ_P']
special_TAZ.to_csv(os.path.join(working_folder, baseline_no_jobs_target_has_jobs_file_name), index = False)

kirk_parcels_df = baseline_df.loc[baseline_df['Jurisdiction'] == 'KIRKLAND'].copy()
kirk_parcels_df = kirk_parcels_df.merge(combined_df[['TAZ_P', 'ratio']], left_on = 'TAZ_P', right_on = 'TAZ_P', how = 'left')
kirk_parcels_df['ratio'].fillna(0, inplace = True)
# for ratio == inf (baseline job is zero but target job > 0.) For now change the ratio to 1. Will handle this situation later
kirk_parcels_df['ratio'].replace(np.inf, 1, inplace = True)

# scale up the baseline jobs to match the control total (Target)
job_fields = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPOTH_P', 'EMPRET_P', 'EMPSVC_P']
kirk_parcels_df[job_fields] = kirk_parcels_df[job_fields].multiply(kirk_parcels_df['ratio'], axis = 0)

# for those baseline job == 0 but target job > 0
# replace subset of final_target_parcel_df with the matching subset of target_parcel_df
# kirk_parcels_df.drop(columns = ['PSRC_ID', 'Jurisdiction', 'BKRCastTAZ', 'ratio'], inplace = True)
special_parcels_df = target_df.loc[target_df['TAZ_P'].isin(special_TAZ)]
kirk_parcels_df = kirk_parcels_df.loc[~kirk_parcels_df['TAZ_P'].isin(special_TAZ)]
kirk_parcels_df = pd.concat([kirk_parcels_df, special_parcels_df])
kirk_parcels_df[job_fields] = kirk_parcels_df[job_fields].round(0).astype(int)

total_jobs_ctrl = combined_df['EMPTOT_2044_Target'].sum()
total_assigned_jobs = kirk_parcels_df[job_fields].sum(axis = 1).sum()
diff = total_jobs_ctrl - total_assigned_jobs

for job_cat in job_fields:
    assigned_by_job_cat = kirk_parcels_df[job_cat].sum()
    job_cat_ctrl = int(round((assigned_by_job_cat / total_jobs_ctrl) * diff + assigned_by_job_cat, 0))
    kirk_parcels_df = utility.controlled_rounding(kirk_parcels_df, job_cat, job_cat_ctrl, 'PARCELID')

kirk_parcels_df[job_fields] = kirk_parcels_df[job_fields].round(0).astype(int)
kirk_parcels_df['EMPTOT_P'] = kirk_parcels_df[job_fields].sum(axis = 1)

kirk_parcels_df.drop(columns = ['PSRC_ID', 'Jurisdiction', 'ratio'], inplace = True)

print(f"Total control jobs: {combined_df['EMPTOT_2044_Target'].sum()}, Total assigned jobs: {kirk_parcels_df['EMPTOT_P'].sum()}")

final_target_parcel_df = baseline_df.copy()
final_target_parcel_df.loc[final_target_parcel_df['Jurisdiction'] == 'KIKRLAND', job_fields] = 0
final_target_parcel_df.drop(columns = ['PSRC_ID', 'Jurisdiction'], inplace = True)
final_target_parcel_df = final_target_parcel_df.loc[~final_target_parcel_df['PARCELID'].isin(kirk_parcels_df['PARCELID'])]
final_target_parcel_df = pd.concat([final_target_parcel_df, kirk_parcels_df])

# for some unknown reason, after .update operation, many columns' datatype are changed from int to float.
# checked both kirk_parcels_df and final_target_parcel_df do not have any NaN.
col_for_dtype_change = ['PARCELID', 'APARKS', 'EMPRSC_P', 'HH_P', 'NPARKS', 'LUTYPE_P', 'PARKDY_P', 'PARKHR_P', 'STUGRD_P', 'STUHGH_P', 'STUUNI_P', 'TAZ_P']
final_target_parcel_df[col_for_dtype_change] = final_target_parcel_df[col_for_dtype_change].astype(int)

final_target_parcel_df['EMPTOT_P'] = 0
for job in job_fields:
    final_target_parcel_df[job] = final_target_parcel_df[job].round(0).astype(int)
    final_target_parcel_df['EMPTOT_P'] += final_target_parcel_df[job]

final_target_parcel_df.to_csv(os.path.join(working_folder, final_target_parcel_file_name), sep = ' ', index = False)

utility.backupScripts(__file__, os.path.join(working_folder, os.path.basename(__file__)))
print('Done')

