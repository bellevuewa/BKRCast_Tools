import os, sys
sys.path.append(os.getcwd())
import pandas as pd
import utility

# this script is used to reformat land use files from City of Redmond to satisfy the input file 
# requirement for BKRCast.
# No other data adjustment except rounding to nearest integer.
# 8/20/2024

### input files
working_folder = r'Z:\Modeling Group\BKRCast\LandUse\Redmond Land Use\base_year_2023'
original_redmond_job_file = 'FinalResults_Employment.csv'
original_redmond_du_file = 'FinalResults_Residential.csv'
lookup_file = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'
subarea_file = r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\TAZ_subarea.csv"
###

### Output fiels
redmond_job_file = '2023_redmond_jobs_reformatted.csv'
redmond_du_file = '2023_Redmond_housing_units_reformatted.csv'
job_error_parcel_file = 'job_parcels_not_in_2014_PSRC_parcels.csv'
resident_error_parcel_file = 'resident_parcels_not_in_2014_PSRC_parcels.csv'###

job_rename_dict = {'EMP_EDU':'EMPEDU_P', 'EMP_FOOD':'EMPFOO_P', 'EMP_GOV':'EMPGOV_P', 'EMP_IND':'EMPIND_P',
    'EMP_MED':'EMPMED_P', 'EMP_OFF':'EMPOFC_P', 'EMP_RTL':'EMPRET_P', 'EMP_RSV':'EMPRSC_P', 'EMP_SEV':'EMPSVC_P', 'EMP_OTHER':'EMPOTH_P',
    'EMP_TOT':'EMPTOT_P'}

du_rename_dict = {'DU_Single':'SFUnits', 'DU_Multi':'MFUnits'}

jobs_columns_List = ['PSRC_ID', 'EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPRET_P', 'EMPRSC_P', 'EMPSVC_P', 'EMPOTH_P', 'EMPTOT_P']
sqft_columns_list = ['PSRC_ID', 'SQFT_EDU', 'SQFT_FOO', 'SQFT_GOV', 'SQFT_IND', 'SQFT_MED', 'SQFT_OFC', 'SQFT_RET', 'SQFT_RSV', 'SQFT_SVC', 'SQFT_OTH', 'SQFT_TOT']
dwellingunits_list = ['PSRC_ID', 'SFUnits', 'MFUnits']

print('Loading....')
lookup_df = pd.read_csv(lookup_file, sep = ',', low_memory = False)
redmond_job_df = pd.read_csv(os.path.join(working_folder, original_redmond_job_file), sep = ',', low_memory = False)
redmond_du_df = pd.read_csv(os.path.join(working_folder, original_redmond_du_file), sep = ',', low_memory = False)
subarea_df = pd.read_csv(subarea_file, sep = ',')

## rename columns to fit modeling input format
redmond_job_df.rename(columns = job_rename_dict, inplace = True)
redmond_du_df.rename(columns = du_rename_dict, inplace = True)
redmond_job_df.fillna(0, inplace = True)
redmond_du_df.fillna(0, inplace = True)

job_cat = [col for col in redmond_job_df.columns if col.startswith('EMP')]
du_cat = [col for col in redmond_du_df.columns if col.endswith('Units')]

b4rounding_jobs = redmond_job_df['EMPTOT_P'].sum()
b4rounding_du = redmond_du_df[du_cat].sum().sum()

redmond_job_df[job_cat] = redmond_job_df[job_cat].round().astype(int)
redmond_du_df[du_cat] = redmond_du_df[du_cat].round().astype(int)

afterrounding_jobs = redmond_job_df['EMPTOT_P'].sum()
afterrounding_du = redmond_du_df[du_cat].sum().sum()

print(f'jobs before rounding: {b4rounding_jobs}, after: {afterrounding_jobs}')
print(f'housing units before rounding: {b4rounding_du}, after: {afterrounding_du}')

print('Exporting job file...')
updated_jobs_df = redmond_job_df[jobs_columns_List].merge(lookup_df[['PSRC_ID', 'Jurisdiction', 'BKRCastTAZ']], left_on = 'PSRC_ID', right_on = 'PSRC_ID', how = 'left')
updated_jobs_df = updated_jobs_df.merge(subarea_df[['BKRCastTAZ', 'Subarea', 'SubareaName']], left_on = 'BKRCastTAZ', right_on = 'BKRCastTAZ', how = 'left')
updated_jobs_df.to_csv(os.path.join(working_folder, redmond_job_file), sep = ',', index = False)

print('Exporting housing unit file...')
updated_housing_df = redmond_du_df[dwellingunits_list].merge(lookup_df[['PSRC_ID', 'Jurisdiction', 'BKRCastTAZ']], left_on = 'PSRC_ID', right_on = 'PSRC_ID', how = 'left')
updated_housing_df = updated_housing_df.merge(subarea_df[['BKRCastTAZ', 'Subarea', 'SubareaName']], left_on = 'BKRCastTAZ', right_on = 'BKRCastTAZ', how = 'left')
updated_housing_df.to_csv(os.path.join(working_folder, redmond_du_file), sep = ',', index = False)

print('Exporting error file...')
job_error_parcels = updated_jobs_df[~updated_jobs_df['PSRC_ID'].isin(lookup_df['PSRC_ID'])]
job_error_parcels.to_csv(os.path.join(working_folder, job_error_parcel_file), sep = ',', index = False)
resident_error_parcels = updated_housing_df[~updated_housing_df['PSRC_ID'].isin(lookup_df['PSRC_ID'])]
resident_error_parcels.to_csv(os.path.join(working_folder, resident_error_parcel_file), sep = ',', index = False)

if job_error_parcels.shape[0] > 0:
    print(f'Please check the {job_error_parcel_file}file first.')

if resident_error_parcels.shape[0] > 0:
    print(f'Please check the {resident_error_parcel_file}file first.')

utility.backupScripts(__file__, os.path.join(working_folder, os.path.basename(__file__)))
print('Done')



