import pandas as pd
import h5py
import numpy as np
import random 
import os
import utility

###
### Since CD will provide numebr of jobs instead of sqft, we will use this sript to 
### replace PSRC's pacel data within King County with sqft converted jobs from CD.
### We do not need to run other scripts to handle sqft and conversion so the land use
### preparation process becomes more straightforward and clean.
### 2/10/2021

## 3/9/2022
## upgraded to python 3.7

# 2023
# allow input jobs file using old trip model TAZ (originally for kirkland complan support)

# allow Redmond parcel data.
# 8/21/2024

###################### configuration
### inputs ###
working_folder = r"Z:\Modeling Group\BKRCast\LandUse\2023baseyear"
new_Bellevue_parcel_data_file_name = '2023_BKR_Jobs_new_method.csv'
new_Redmond_parcel_data_file_name = '2023_redmond_jobs_reformatted.csv'
Original_parcel_file_name = r"interpolated_parcel_file_2023_from_PSRC_2014_2050.txt"
lookup_file = r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv"

Set_Jobs_to_Zeros_All_Bel_Parcels_Not_in_New_Parcel_Data_File = True
Set_Jobs_to_Zeros_All_Redmond_Parcels_Not_in_New_Parcel_Data_File = True

Jobs_by_old_BKRTMTAZ_file = '' # r'Kirkland_2022_jobs_by_BKRTMTAZ.csv'

### output files ###
Updated_parcel_file_name =  r"2023_parcels_urbansim.txt"
Old_Subset_parcel_file_name = r"Old_parcels_subset.txt"

Columns_List = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPRET_P', 'EMPRSC_P', 'EMPSVC_P', 'EMPOTH_P']
####################

new_Bellevue_parcel_data_df = pd.read_csv(os.path.join(working_folder, new_Bellevue_parcel_data_file_name), sep = ',', low_memory = False)
new_Redmond_parcel_data_df = pd.read_csv(os.path.join(working_folder, new_Redmond_parcel_data_file_name), sep = ',', low_memory = False)

original_parcel_data_df = pd.read_csv(os.path.join(working_folder,Original_parcel_file_name), sep = ' ', low_memory = False)
lookup_df = pd.read_csv(lookup_file, sep = ',', low_memory = False)

print('Processing Bellevue jobs...')
full_bellevue_parcels_df = lookup_df.loc[lookup_df['Jurisdiction'] == 'BELLEVUE']  #  a complete list of Bellevue parcels
actual_bel_parcels_df = new_Bellevue_parcel_data_df.loc[new_Bellevue_parcel_data_df['PSRC_ID'].isin(full_bellevue_parcels_df['PSRC_ID'])] # Bellevue parcels included in COB job file
not_in_full_bellevue_parcels = actual_bel_parcels_df.loc[~actual_bel_parcels_df['PSRC_ID'].isin(full_bellevue_parcels_df['PSRC_ID'])] # parcels in COB job file but not in the complete list
missing_bellevue_parcels_df = original_parcel_data_df.loc[original_parcel_data_df['PARCELID'].isin(full_bellevue_parcels_df.loc[~full_bellevue_parcels_df['PSRC_ID'].isin(new_Bellevue_parcel_data_df['PSRC_ID']), 'PSRC_ID'])]
missing_bellevue_parcels_df.to_csv(os.path.join(working_folder, 'missing_bellevue_parcels.csv'), sep = ',', index = False)
not_in_full_bellevue_parcels.to_csv(os.path.join(working_folder, 'not_valid_bellevue_parcels.csv'), sep = ',', index = False)

newBeljobs = new_Bellevue_parcel_data_df['EMPTOT_P'].sum() 
new_Bellevue_parcel_data_df = new_Bellevue_parcel_data_df.set_index('PSRC_ID')
updated_parcel_df = original_parcel_data_df.copy()
updated_parcel_df = updated_parcel_df.set_index('PARCELID')
oldBeljobs = updated_parcel_df.loc[updated_parcel_df.index.isin(new_Bellevue_parcel_data_df.index), 'EMPTOT_P'].sum()
print('Bellevue or BKR jobs before change: ' + str(oldBeljobs))
print('              after change: ' + str(newBeljobs))
print('Bellevue or BKR jobs gained ' + str(newBeljobs - oldBeljobs))
updated_parcel_df.loc[updated_parcel_df.index.isin(new_Bellevue_parcel_data_df.index), Columns_List] = new_Bellevue_parcel_data_df[Columns_List]

print('Processing Redmond jobs...')
full_redmond_parcels_df = lookup_df.loc[lookup_df['Jurisdiction'] == 'REDMOND'] # a complete list of Redmond parcels
actual_redmond_parcels_df = new_Redmond_parcel_data_df.loc[new_Redmond_parcel_data_df['PSRC_ID'].isin(full_redmond_parcels_df['PSRC_ID'])]  # Redmond parcels included in Redmond's job estimate file
not_in_full_redmond_parcels = actual_redmond_parcels_df.loc[~actual_redmond_parcels_df['PSRC_ID'].isin(full_redmond_parcels_df['PSRC_ID'])]
missing_redmond_parcels_df = original_parcel_data_df.loc[original_parcel_data_df['PARCELID'].isin(full_redmond_parcels_df.loc[~full_redmond_parcels_df['PSRC_ID'].isin(new_Redmond_parcel_data_df['PSRC_ID']), 'PSRC_ID'])]
missing_redmond_parcels_df.to_csv(os.path.join(working_folder, 'missing_redmond_parcels.csv'), sep = ',', index = False)
not_in_full_redmond_parcels.to_csv(os.path.join(working_folder, 'not_valid_redmond_parcels.csv'), sep = ',', index = False)

newRedmondjobs = new_Redmond_parcel_data_df['EMPTOT_P'].sum()
new_Redmond_parcel_data_df = new_Redmond_parcel_data_df.set_index('PSRC_ID')
oldRedmondjobs = updated_parcel_df.loc[updated_parcel_df.index.isin(new_Redmond_parcel_data_df.index), 'EMPTOT_P'].sum()
print('Redmond jobs before change: ' + str(oldRedmondjobs))
print('              after change: ' + str(newRedmondjobs))
print('Redmond jobs gained ' + str(newRedmondjobs - oldRedmondjobs))
updated_parcel_df.loc[updated_parcel_df.index.isin(new_Redmond_parcel_data_df.index), Columns_List] = new_Redmond_parcel_data_df[Columns_List]


if Set_Jobs_to_Zeros_All_Bel_Parcels_Not_in_New_Parcel_Data_File == True:
    jobs_to_be_zeroed_out = updated_parcel_df.loc[updated_parcel_df.index.isin(missing_bellevue_parcels_df['PARCELID']), 'EMPTOT_P'].sum()
    updated_parcel_df.loc[updated_parcel_df.index.isin(missing_bellevue_parcels_df['PARCELID']), Columns_List] = 0
    print('-----------------------------------------')
    print('Some COB parcels are not provided in the ' + new_Bellevue_parcel_data_file_name + '.')
    print('But they exist in ' + Original_parcel_file_name + '.')
    print('Number of jobs in these parcels are now zeroed out: ' + str(jobs_to_be_zeroed_out))

if Set_Jobs_to_Zeros_All_Redmond_Parcels_Not_in_New_Parcel_Data_File == True:
    jobs_to_be_zeroed_out = updated_parcel_df.loc[updated_parcel_df.index.isin(missing_redmond_parcels_df['PARCELID']), 'EMPTOT_P'].sum()
    updated_parcel_df.loc[updated_parcel_df.index.isin(missing_redmond_parcels_df['PARCELID']), Columns_List] = 0
    print('-----------------------------------------')
    print('Some Redmond parcels are not provided in the ' + new_Redmond_parcel_data_file_name + '.')
    print('But they exist in ' + Original_parcel_file_name + '.')
    print('Number of jobs in these parcels are now zeroed out: ' + str(jobs_to_be_zeroed_out))
    
# update the total jobs 
updated_parcel_df['EMPTOT_P'] = updated_parcel_df[Columns_List].sum(axis = 1)
# updated_parcel_df['EMPTOT_P'] = 0
# for col in Columns_List:
#     updated_parcel_df['EMPTOT_P'] += updated_parcel_df[col]     

if Jobs_by_old_BKRTMTAZ_file != '':
    print('processing Kirkland land use input...')
    # if jobs by old trip model taz (BKRTMTAZ) is provided (either by Kirkland or Redmond), we need to apply the following assumptions to 
    # scale BKRCast jobs up to match the provided jobs.
    '''
    Left is job category in old BKR model. Kirkland is still using these land use categories.
    Right is job category used by BKRCast.
    Use Kirkland's job estimate as control total for each BKRTMTAZ, scale the PSRC jobs in each parcel such that total scaled jobs matches Kirkland's estimate
    in BKRTMTAZ level.
    '''
    kirk_control_jobs_by_BKRTMTAZ_df = pd.read_csv(os.path.join(working_folder, Jobs_by_old_BKRTMTAZ_file))
    kirk_parcels_df = lookup_df.loc[lookup_df['Jurisdiction'] == 'KIRKLAND']
    kirk_parcels_df = updated_parcel_df.reset_index().merge(kirk_parcels_df[['PSRC_ID', 'BKRTMTAZ']], left_on = 'PARCELID', right_on = 'PSRC_ID')
    temp_col_list = Columns_List.copy()
    temp_col_list.append('EMPTOT_P')
    temp_col_list.append('BKRTMTAZ')
    psrc_kirk_jobs = kirk_parcels_df['EMPTOT_P'].sum()
    kirk_psrc_jobs_by_BKRTMTAZ_df = kirk_parcels_df[temp_col_list].groupby('BKRTMTAZ').sum()
    kirk_control_jobs_by_BKRTMTAZ_df = kirk_control_jobs_by_BKRTMTAZ_df.merge(kirk_psrc_jobs_by_BKRTMTAZ_df.reset_index(), on = 'BKRTMTAZ', how = 'left')
    kirk_control_jobs_by_BKRTMTAZ_df.loc[kirk_control_jobs_by_BKRTMTAZ_df['EMPTOT_P'] != 0, 'scale'] = kirk_control_jobs_by_BKRTMTAZ_df['KirklandControlTotal'] / kirk_control_jobs_by_BKRTMTAZ_df['EMPTOT_P']
    kirk_control_jobs_by_BKRTMTAZ_df.loc[kirk_control_jobs_by_BKRTMTAZ_df['EMPTOT_P'] == 0, 'scale'] = 1
    kirk_control_jobs_by_BKRTMTAZ_df.to_csv(os.path.join(working_folder, 'Kirkland_job_scale_comparison.csv'), index = True)

    kirk_parcels_df = kirk_parcels_df.merge(kirk_control_jobs_by_BKRTMTAZ_df[['BKRTMTAZ', 'scale']], on = 'BKRTMTAZ', how = 'left')
    kirk_parcels_df['EMPTOT_P'] = 0
    for col in Columns_List:
        kirk_parcels_df[col] = kirk_parcels_df[col] * kirk_parcels_df['scale']
        kirk_parcels_df[col] = kirk_parcels_df[col].round(0).astype(int)
        kirk_parcels_df['EMPTOT_P'] += kirk_parcels_df[col]    

    kirk_parcels_df.to_csv(os.path.join(working_folder, 'adjusted_jobs_by_parcel_in_Kirkland.csv'), index = False)
    kirk_jobs_by_BKRTMTAZ_df = kirk_parcels_df[temp_col_list].groupby('BKRTMTAZ').sum()
    kirk_jobs_by_BKRTMTAZ_df = kirk_jobs_by_BKRTMTAZ_df.merge(kirk_control_jobs_by_BKRTMTAZ_df[['BKRTMTAZ', 'KirklandControlTotal']], on = 'BKRTMTAZ')
    kirk_jobs_by_BKRTMTAZ_df.to_csv(os.path.join(working_folder, 'Kirkland_job_comparison_by_BKRTMTAZ.csv'), index = True)

    updated_parcel_df = updated_parcel_df.loc[~updated_parcel_df.index.isin(kirk_parcels_df['PARCELID'])]
    kirk_parcels_df.drop(columns = ['PSRC_ID', 'BKRTMTAZ', 'scale'], inplace = True)
    updated_parcel_df = pd.concat([updated_parcel_df.reset_index(), kirk_parcels_df])
    updated_parcel_df = updated_parcel_df.sort_values(by = ['PARCELID'], ascending = True)

    # update total jobs in each parcel
    updated_parcel_df['EMPTOT_P'] = updated_parcel_df[Columns_List].sum(axis = 1)
    
    new_kirk_jobs = updated_parcel_df.loc[updated_parcel_df.index.isin(kirk_parcels_df['PARCELID']), 'EMPTOT_P'].sum()

    print('Kirkland jobs before change: ' + str(psrc_kirk_jobs))
    print('              after change: ' + str(new_kirk_jobs))
    print('Kirkland jobs gained ' + str(new_kirk_jobs - psrc_kirk_jobs))

print('total jobs before change: ' + str(original_parcel_data_df['EMPTOT_P'].sum()))
print('total jobs after change: ' + str(updated_parcel_df['EMPTOT_P'].sum()))
print('')

print('Exporting parcel files...')
updated_parcel_df.to_csv(os.path.join(working_folder, Updated_parcel_file_name), sep = ' ')
utility.backupScripts(__file__, os.path.join(working_folder, os.path.basename(__file__)))


print('Done')




