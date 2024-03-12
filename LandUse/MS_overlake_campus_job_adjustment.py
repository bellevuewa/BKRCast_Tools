import pandas as pd
import os
import datetime
import utility

'''
It is found that PSRC 2050 parcel data has significant job allocation issues in MS campus around overlake area. All job growth from 2014 has been 
allocated to the headquarter while other locations has received job reductions, big or small.

This script is used to address this MS job allocation issue. General idea is to scale base year 2014 jobs inside the concerned area (MS campus) up
to match total jobs in 2050. (we think 2014 job distribution in MS campus makes much more sense.)

First, manually calculate total job growth in MS campus. Update ratio_file_name.
The new parcel file is exported to updated_parcel_file_name.

'''
### Inputs
original_parcel_file_name = r"Z:\Modeling Group\BKRCast\KirklandSupport\Kirkland2044Complan\baseline 2044\parcels_urbansim.txt"
ratio_file_name = r"Z:\Modeling Group\BKRCast\KirklandSupport\Kirkland2044Complan\baseline2044_01092024\kirkland_2019_scale_factor_selected_TAZ.csv"
baseyear_parcel_file_name = r"Z:\Modeling Group\BKRCast\LandUse\2019baseyear-new_popsim_approach\parcels_urbansim.txt"

### Outputs
updated_parcel_file_name = r'Z:\Modeling Group\BKRCast\KirklandSupport\Kirkland2044Complan\baseline2044_01092024\Kirk_Complan_2044_parcels_urbansim_job_rebalanced.txt'
ms_parcels_file = r'Z:\Modeling Group\BKRCast\KirklandSupport\Kirkland2044Complan\baseline2044_01092024\Kirkland_Complan_2044_rebalanced_parcels.xlsx'

Job_Field = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPOTH_P', 'EMPRET_P', 'EMPSVC_P']
ratio_attribute_name = 'adj_factor'
parcel_df = pd.read_csv(original_parcel_file_name, sep = ' ', low_memory = False)
base_parcels_df = pd.read_csv(baseyear_parcel_file_name, sep = ' ', low_memory = False)

# update total jobs in both dataframe to ensure data consistency
parcel_df['EMPTOT_P'] = 0
base_parcels_df['EMPTOT_P'] = 0
for col in Job_Field:
    parcel_df['EMPTOT_P'] += parcel_df[col]
    base_parcels_df['EMPTOT_P'] += base_parcels_df[col]

ratio_df = pd.read_csv(ratio_file_name)
updated_parcel_df = base_parcels_df.merge(ratio_df[['BKRCastTAZ', ratio_attribute_name]], left_on = 'TAZ_P', right_on = 'BKRCastTAZ', how = 'inner')

total_jobs_in_MS_campus =  parcel_df.loc[parcel_df['PARCELID'].isin(updated_parcel_df['PARCELID']),'EMPTOT_P'].sum()
total_jobs_before = parcel_df['EMPTOT_P'].sum()

print(f'Total jobs in the original file: {total_jobs_before}')
print(f'Total jobs at MS campus before adjustment: {total_jobs_in_MS_campus} ')

# export parcels in MS campus for job comparison, before and after adjustment.
with pd.ExcelWriter(ms_parcels_file, engine = 'xlsxwriter') as writer:
    wksheet = writer.book.add_worksheet('readme')
    wksheet.write(0, 0, str(datetime.datetime.now())) 
    wksheet.write(1, 0, original_parcel_file_name)       
    wksheet.write(2, 0, baseyear_parcel_file_name)    
    wksheet.write(3, 0, updated_parcel_file_name)
    wksheet.write(4, 0, ratio_file_name)

        
    updated_parcel_df['EMPTOT_P'] = 0
    for job in Job_Field:
        updated_parcel_df[job] =  updated_parcel_df[job] * updated_parcel_df[ratio_attribute_name]
        updated_parcel_df[job] = updated_parcel_df[job].round(0).astype(int)
        updated_parcel_df['EMPTOT_P'] += updated_parcel_df[job]

    updated_parcel_df.to_excel(writer, sheet_name = 'MS campus after adjustment')
    ms_parcels_before_adj = parcel_df.loc[parcel_df['PARCELID'].isin(updated_parcel_df['PARCELID'])]
    ms_parcels_before_adj.to_excel(writer, sheet_name = 'original MS campus')        


total_jobs_in_MS_campus_new =  updated_parcel_df['EMPTOT_P'].sum()
print(f'Total jobs at MS campus after the adjustment: {total_jobs_in_MS_campus_new}')
print('MS job increase: ' + str(total_jobs_in_MS_campus_new - total_jobs_in_MS_campus))

All_Job_Fields = Job_Field.copy()
All_Job_Fields.append('EMPTOT_P')

updated_parcel_df.drop(['BKRCastTAZ', ratio_attribute_name], axis = 1, inplace = True)
parcel_df.set_index('PARCELID', inplace = True)
updated_parcel_df.set_index('PARCELID', inplace = True)
parcel_df.update(updated_parcel_df[All_Job_Fields])

parcel_df['EMPTOT_P'] = 0
for col in Job_Field:
    parcel_df[col] = parcel_df[col].astype(int)
    parcel_df['EMPTOT_P'] += parcel_df[col]

    
total_jobs_after = parcel_df['EMPTOT_P'].sum()
print(f'Total jobs after adjustment: {total_jobs_after}')


print('Exporting ...')
parcel_df.to_csv(updated_parcel_file_name, index = True, sep = ' ')

utility.backupScripts(__file__, os.path.join(os.path.dirname(updated_parcel_file_name), os.path.basename(__file__)))
print('Done.')


