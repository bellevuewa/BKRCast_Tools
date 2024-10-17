import pandas as pd
import os
import numpy as np


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
kirk_parcels_df['EMPTOT_P'] = 0
for job in job_fields:
    kirk_parcels_df[job] = kirk_parcels_df[job] * kirk_parcels_df['ratio']
    kirk_parcels_df[job] = kirk_parcels_df[job].round(0).astype(int)
    kirk_parcels_df['EMPTOT_P'] += kirk_parcels_df[job]


# for those baseline job == 0 but target job > 0
# replace subset of final_target_parcel_df with the matching subset of target_parcel_df
# kirk_parcels_df.drop(columns = ['PSRC_ID', 'Jurisdiction', 'BKRCastTAZ', 'ratio'], inplace = True)
special_parcels_df = target_df.loc[target_df['TAZ_P'].isin(special_TAZ)]
kirk_parcels_df = kirk_parcels_df.loc[~kirk_parcels_df['TAZ_P'].isin(special_TAZ)]
kirk_parcels_df = pd.concat([kirk_parcels_df, special_parcels_df])

# because of rounding, scaled jobs do not equal control total in many TAZs. Sometimes the difference is relative big.
# below is to address the difference by going through each TAZ.
kirk_parcels_df = kirk_parcels_df.sort_values(by = 'PARCELID', ascending = True)
kirk_taz_list = sorted(kirk_parcels_df['TAZ_P'].unique().tolist())
for kirk_taz in kirk_taz_list:
    assigned_jobs = kirk_parcels_df.loc[kirk_parcels_df['TAZ_P'] == kirk_taz, 'EMPTOT_P'].sum()
    control_jobs = combined_df.loc[combined_df['TAZ_P'] == kirk_taz, 'EMPTOT_2044_Target'].sum()
    diff = control_jobs - assigned_jobs

    print(f'TAZ: {kirk_taz}, Jobs assigned: {assigned_jobs}, Control: {control_jobs}, Diff: {diff}')
    if diff == 0:
       continue

    # find out parcels with employment. If no employment in baseline, fidn all parcels in that taz
    if assigned_jobs == 0:
        parcels_w_job_in_taz = kirk_parcels_df.loc[(kirk_parcels_df['TAZ_P'] == kirk_taz)]
    else:
        parcels_w_job_in_taz = kirk_parcels_df.loc[(kirk_parcels_df['TAZ_P'] == kirk_taz) & (kirk_parcels_df['EMPTOT_P'] > 0)]

    num_parcels = parcels_w_job_in_taz.shape[0]

    # add or subtract jobs on each applicable parcel to make the total jobs equal control total
    if num_parcels >= abs(diff):  # must use dot notation. bracket notion does not work.
        selected_indices = np.random.choice(parcels_w_job_in_taz.PARCELID, size = abs(diff), replace = False)
    else:
        selected_indices = np.random.choice(parcels_w_job_in_taz.PARCELID, size = abs(diff), replace = True)

    unique_indices, counts = np.unique(selected_indices, return_counts = True)
    sorted_zipped = sorted(zip(unique_indices, counts), key = lambda x:x[1], reverse = True )

    if control_jobs >= assigned_jobs: # add additional jobs to the job category with the highest number of jobs in a selected parcel
        for index, count in sorted_zipped:
            row = kirk_parcels_df.loc[kirk_parcels_df['PARCELID'] == index, job_fields]
            max_job_cat = job_fields[np.argmax(row.values)]
            kirk_parcels_df.loc[kirk_parcels_df['PARCELID'] == index, max_job_cat] += count
    else:  # need to reduce jobs from scaled job numbers. be careful we cannot have negative number in job category.
        for index, count in sorted_zipped:
            if kirk_parcels_df.loc[kirk_parcels_df['PARCELID'] == index, 'EMPTOT_P'].values[0] > count:
                remaining = count
                while remaining > 0:
                    row = kirk_parcels_df.loc[kirk_parcels_df['PARCELID'] == index, job_fields]
                    max_job_cat = job_fields[np.argmax(row.values)]
                    max_jobs = kirk_parcels_df.loc[kirk_parcels_df['PARCELID'] == index, max_job_cat].values[0]
                    if max_jobs >= remaining:
                        kirk_parcels_df.loc[kirk_parcels_df['PARCELID'] == index, max_job_cat] -= remaining
                        remaining = 0
                    else:
                        kirk_parcels_df.loc[kirk_parcels_df['PARCELID'] == index, max_job_cat] = 0
                        remaining = remaining - max_jobs
            else:
                kirk_parcels_df.loc[kirk_parcels_df['PARCELID'] == index, job_fields] = 0

# a little duplicated here, only because want to see the updated total jobs in the middle. 
kirk_parcels_df['EMPTOT_P'] = 0
for job in job_fields:
    kirk_parcels_df[job] = kirk_parcels_df[job].round(0).astype(int)
    kirk_parcels_df['EMPTOT_P'] += kirk_parcels_df[job]

kirk_parcels_df.drop(columns = ['PSRC_ID', 'Jurisdiction', 'ratio'], inplace = True)

print(f"Total control jobs: {combined_df['EMPTOT_2044_Target'].sum()}, Total assigned jobs: {kirk_parcels_df['EMPTOT_P'].sum()}")

kirk_parcels_df.set_index('PARCELID', inplace = True)
final_target_parcel_df = baseline_df.copy()
final_target_parcel_df.loc[final_target_parcel_df['Jurisdiction'] == 'KIKRLAND', job_fields] = 0
final_target_parcel_df.drop(columns = ['PSRC_ID', 'Jurisdiction'], inplace = True)
final_target_parcel_df.set_index('PARCELID', inplace = True)
final_target_parcel_df.update(kirk_parcels_df)

# for some unknown reason, after .update operation, many columns' datatype are changed from int to float.
# checked both kirk_parcels_df and final_target_parcel_df do not have any NaN.
col_for_dtype_change = ['APARKS', 'EMPRSC_P', 'HH_P', 'NPARKS', 'LUTYPE_P', 'PARKDY_P', 'PARKHR_P', 'STUGRD_P', 'STUHGH_P', 'STUUNI_P', 'TAZ_P']
final_target_parcel_df[col_for_dtype_change] = final_target_parcel_df[col_for_dtype_change].astype(int)

final_target_parcel_df['EMPTOT_P'] = 0
for job in job_fields:
    final_target_parcel_df[job] = final_target_parcel_df[job].round(0).astype(int)
    final_target_parcel_df['EMPTOT_P'] += final_target_parcel_df[job]

final_target_parcel_df.to_csv(os.path.join(working_folder, final_target_parcel_file_name), sep = ' ', index = True)

print('Done')

