import pandas as pd
import os

working_folder = r'Z:\Modeling Group\BKRCast\LandUse\test\2035DTAccess'
parcel_file = '2035_parcels_urbansim.txt'

parcel_df = pd.read_csv(os.path.join(working_folder, parcel_file), sep = ' ', low_memory = False)
parcel_jobs_columns_List = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPRET_P', 'EMPRSC_P', 'EMPSVC_P', 'EMPOTH_P']

print 'total jobs by EMPTOT_P: ' + str(parcel_df['EMPTOT_P'].sum())
print 'total jobs by category '  + str(parcel_df[parcel_jobs_columns_List].sum().sum())
parcel_df['EMPTOT_P'] = parcel_df[parcel_jobs_columns_List].sum(axis = 1)
print 'total jobs after update: ' + str(parcel_df['EMPTOT_P'].sum())

parcel_df.to_csv(os.path.join(working_folder, parcel_file), sep = ' ', index = False)

print 'done'