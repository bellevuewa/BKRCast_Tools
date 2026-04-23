import pandas as pd
import os
import utility

'''
    This script is used to increase number of jobs by factors defined in each TAZ. The factor is given as an input file named 'ratio_file_name'. The factored
    number of jobs are rounded to whole integers. The new parcel file is exported in 'updated_parcel_file_name'.
'''
### Inputs
original_parcel_file_name = r"Z:\Modeling Group\BKRCast\LandUse\Complan\Complan2044\preferred_regional_buildout_alt\complan_preferred_parcels_urbansim.txt"
ratio_file_name = r"Z:\Modeling Group\BKRCast\LandUse\Complan\Complan2044\preferred_regional_buildout_alt\job_factor_by_taz.csv"

### Outputs
updated_parcel_file_name = r'Z:\Modeling Group\BKRCast\LandUse\Complan\Complan2044\preferred_regional_buildout_alt\updated_complan_preferred_parcels_urbansim.txt'

Job_Field = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPOTH_P', 'EMPRET_P', 'EMPSVC_P']
ratio_attribute_name = 'adj_factor'
parcel_df = pd.read_csv(original_parcel_file_name, sep = ' ', low_memory = False)
ratio_df = pd.read_csv(ratio_file_name)
updated_parcel_df = parcel_df.merge(ratio_df[['BKRCastTAZ', ratio_attribute_name]], left_on = 'TAZ_P', right_on = 'BKRCastTAZ', how = 'left')

total_jobs_before =  updated_parcel_df['EMPTOT_P'].sum()
print('Total jobs in the original file: ' + str(total_jobs_before))
updated_parcel_df['EMPTOT_P'] = 0
for job in Job_Field:
    updated_parcel_df[job] =  updated_parcel_df[job] * updated_parcel_df[ratio_attribute_name]
    updated_parcel_df[job] = updated_parcel_df[job].round(0).astype(int)
    updated_parcel_df['EMPTOT_P'] += updated_parcel_df[job]


total_jobs_after =  updated_parcel_df['EMPTOT_P'].sum()
print('Total jobs after the adjustment: ' + str(total_jobs_after))
print('Job increase: ' + str(total_jobs_after - total_jobs_before))
print('Exporting ...')
updated_parcel_df.drop(['BKRCastTAZ', ratio_attribute_name], axis = 1, inplace = True)
updated_parcel_df.to_csv(updated_parcel_file_name, index = False, sep = ' ')

utility.backupScripts(__file__, os.path.join(os.path.dirname(updated_parcel_file_name), os.path.basename(__file__)))
print('Done.')

