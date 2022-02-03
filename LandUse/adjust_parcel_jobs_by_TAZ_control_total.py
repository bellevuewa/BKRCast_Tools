from numpy.lib.arraypad import pad
import pandas as pd
import os
import utility



working_folder = r'Z:\Modeling Group\BKRCast\LandUse\TFP\2033_horizonyear_TFP'
job_file_by_taz = 'Redmond_Kirkland_2033_jobs_hhs_by_tripmodel_TAZ.csv'
lookup_file = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'
subarea_file = r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\TAZ_subarea.csv"
input_parcel_file = r'interpolated_parcel_file_2033_from_PSRC_2014_2050.txt'
output_parcel_file = r'2033_parcel_file_after_scaled_up_to_old_BKRTAZ_control_total.txt'

parcel_df = pd.read_csv(os.path.join(working_folder, input_parcel_file), sep = ' ', low_memory = False)
lookup_df = pd.read_csv(lookup_file, low_memory = False)
job_by_taz_df = pd.read_csv(os.path.join(working_folder, job_file_by_taz))
subarea_df = pd.read_csv(subarea_file)
job_cats = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPOTH_P', 'EMPRET_P', 'EMPSVC_P']

adjusted_parcel_df = pd.merge(parcel_df, lookup_df[['PSRC_ID', 'BKRTMTAZ']], left_on = 'PARCELID', right_on = 'PSRC_ID', how = 'left')
adjusted_parcel_df.drop('PSRC_ID', axis = 1, inplace = True)
adjusted_parcel_df = pd.merge(adjusted_parcel_df, lookup_df[['PSRC_ID', 'Jurisdiction']], left_on = 'PARCELID', right_on = 'PSRC_ID', how = 'left')
adjusted_parcel_df.drop('PSRC_ID', axis = 1, inplace = True)
jobs_by_trip_model_TAZ = adjusted_parcel_df[['BKRTMTAZ', 'EMPTOT_P']].groupby('BKRTMTAZ').sum()
jobs_scale_rate_by_TAZ = pd.merge(jobs_by_trip_model_TAZ, job_by_taz_df[['BKRTMTAZ', 'total_jobs']], left_on = 'BKRTMTAZ', right_on = 'BKRTMTAZ', how = 'inner' )

# find out TAZ that EMPTOT_P == 0  but total_jobs > 0
problematic_TAZ = jobs_scale_rate_by_TAZ.loc[(jobs_scale_rate_by_TAZ['EMPTOT_P'] == 0) & (jobs_scale_rate_by_TAZ['total_jobs'] > 0)]
if problematic_TAZ.shape[0] > 0:
    print('Please check the error file (error_taz.txt) before moving one')
    with open(os.path.join(working_folder, 'error_taz.txt'), 'w') as output:
        output.write('Jobs in the TAZs below cannot be scaled up to match TAZ control total because of zero jobs in their parels.\n')
        output.write('%s' % problematic_TAZ)
        err_parcels_df = adjusted_parcel_df.loc[adjusted_parcel_df['BKRTMTAZ'].isin(problematic_TAZ['BKRTMTAZ'])]
        output.write('\n')
        err_parcels_df.to_string(output)

print('Before the scale up')
parts_columns = job_cats.copy()
parts_columns.extend(['EMPTOT_P', 'Jurisdiction'])
sum_before_change = adjusted_parcel_df[parts_columns].groupby('Jurisdiction').sum()
print(sum_before_change)

jobs_scale_rate_by_TAZ['ratio'] = 1.0 * jobs_scale_rate_by_TAZ['total_jobs'] / jobs_scale_rate_by_TAZ['EMPTOT_P']
adjusted_parcel_df = pd.merge(adjusted_parcel_df, jobs_scale_rate_by_TAZ[['BKRTMTAZ', 'ratio']], on = 'BKRTMTAZ', how = 'left')
adjusted_parcel_df['ratio'] = adjusted_parcel_df['ratio'].fillna(1)

adjusted_parcel_df[job_cats] = adjusted_parcel_df[job_cats].multiply(adjusted_parcel_df['ratio'], axis = 'index').round(0)
adjusted_parcel_df['EMPTOT_P'] = adjusted_parcel_df[job_cats].sum(axis = 1)
adjusted_parcel_df[job_cats] = adjusted_parcel_df[job_cats].astype(int)
adjusted_parcel_df['EMPTOT_P'] = adjusted_parcel_df['EMPTOT_P'].astype(int)

print('After the scale up')
sum_after_change = adjusted_parcel_df[parts_columns].groupby('Jurisdiction').sum()
print(sum_after_change)

adjusted_parcel_df.drop(['BKRTMTAZ', 'ratio', 'Jurisdiction'], axis = 1, inplace = True)
adjusted_parcel_df.to_csv(os.path.join(working_folder, output_parcel_file), sep = ' ', index = False)
utility.backupScripts(__file__, os.path.join(working_folder, os.path.basename(__file__)))


print('Done')
