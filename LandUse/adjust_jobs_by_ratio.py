import pandas as pd
import os
import utility

### Inputs
original_parcel_file_name = r'Z:\Modeling Group\BKRCast\LandUse\2019baseyear\parcels_urbansim.txt'
ratio_file_name = r'Z:\Modeling Group\BKRCast\LandUse\2020baseyear\rest_of_KC_TAZ_growth_ratio.csv'

### Outputs
updated_parcel_file_name = r'Z:\Modeling Group\BKRCast\LandUse\2020baseyear\updated_rest_of_KC_parcels.txt'

Job_Field = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPOTH_P', 'EMPRET_P', 'EMPSVC_P', 'EMPTOT_P']
ratio_attribute_name = '2020_sqft_ratio_over_2019'
parcel_df = pd.read_csv(original_parcel_file_name, sep = ' ')
parcel_df.drop(['Unnamed: 0', 'index'], axis = 1, inplace = True)
ratio_df = pd.read_csv(ratio_file_name)
updated_parcel_df = parcel_df.merge(ratio_df[['BKRCastTAZ', ratio_attribute_name]], left_on = 'TAZ_P', right_on = 'BKRCastTAZ', how = 'inner')

total_jobs_before =  updated_parcel_df['EMPTOT_P'].sum()
print 'Total jobs in the original file: ' + str(total_jobs_before)
for job in Job_Field:
    updated_parcel_df[job] =  updated_parcel_df[job] * updated_parcel_df[ratio_attribute_name]
updated_parcel_df[Job_Field] = updated_parcel_df[Job_Field].round(0).astype(int)
total_jobs_after =  updated_parcel_df['EMPTOT_P'].sum()
print 'Total jobs after the adjustment: ' + str(total_jobs_after)
print 'Job increase: ' + str(total_jobs_after - total_jobs_before)
print 'Exporting ...'
updated_parcel_df.drop(['BKRCastTAZ', ratio_attribute_name], axis = 1, inplace = True)
updated_parcel_df.to_csv(updated_parcel_file_name, index = False, sep = ' ')

utility.backupScripts(__file__, os.path.join(os.path.dirname(updated_parcel_file_name), os.path.basename(__file__)))
print 'Done.'

