import pandas as pd
import numpy as np
import os,sys
sys.path.append(os.getcwd())

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

# make the bellevue parcel data file optional
# 12/23/2025
###################### configuration
### inputs ###
working_folder = r"Z:\Modeling Group\BKRCast\LandUse\2044_long_term_planning"
new_Bellevue_parcel_data_file_name = ''
Original_parcel_file_name = r"2044_Bellevue_Kirkland_parcels_urbansim.txt"
lookup_file = r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv"

Set_Jobs_to_Zeros_All_Bel_Parcels_Not_in_New_Parcel_Data_File = True

Jobs_by_old_BKRTMTAZ_file = '2044_Redmond_estimated_jobs_by_BKRTMTAZ.csv' # r'Kirkland_2022_jobs_by_BKRTMTAZ.csv'

### output files ###
Updated_parcel_file_name =  r"2044_long_term_parcels_urbansim.txt"
Old_Subset_parcel_file_name = r"Old_parcels_subset.txt"

Columns_List = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPRET_P', 'EMPRSC_P', 'EMPSVC_P', 'EMPOTH_P']
####################

original_parcel_data_df = pd.read_csv(os.path.join(working_folder,Original_parcel_file_name), sep = ' ', low_memory = False)
lookup_df = pd.read_csv(lookup_file, sep = ',', low_memory = False)

updated_parcel_df = original_parcel_data_df.copy()
if new_Bellevue_parcel_data_file_name != '':
    new_Bellevue_parcel_data_df = pd.read_csv(os.path.join(working_folder, new_Bellevue_parcel_data_file_name), sep = ',', low_memory = False)

    print('Processing Bellevue jobs...')
    full_bellevue_parcels_df = lookup_df.loc[lookup_df['Jurisdiction'] == 'BELLEVUE']  #  a complete list of Bellevue parcels
    actual_bel_parcels_df = new_Bellevue_parcel_data_df.loc[new_Bellevue_parcel_data_df['PSRC_ID'].isin(full_bellevue_parcels_df['PSRC_ID'])] # Bellevue parcels included in COB job file
    not_in_full_bellevue_parcels = actual_bel_parcels_df.loc[~actual_bel_parcels_df['PSRC_ID'].isin(full_bellevue_parcels_df['PSRC_ID'])] # parcels in COB job file but not in the complete list
    missing_bellevue_parcels_df = original_parcel_data_df.loc[original_parcel_data_df['PARCELID'].isin(full_bellevue_parcels_df.loc[~full_bellevue_parcels_df['PSRC_ID'].isin(new_Bellevue_parcel_data_df['PSRC_ID']), 'PSRC_ID'])]
    missing_bellevue_parcels_df.to_csv(os.path.join(working_folder, 'missing_bellevue_parcels.csv'), sep = ',', index = False)
    not_in_full_bellevue_parcels.to_csv(os.path.join(working_folder, 'not_valid_bellevue_parcels.csv'), sep = ',', index = False)

    newBeljobs = new_Bellevue_parcel_data_df['EMPTOT_P'].sum() 
    new_Bellevue_parcel_data_df = new_Bellevue_parcel_data_df.set_index('PSRC_ID')

    updated_parcel_df = updated_parcel_df.set_index('PARCELID')
    oldBeljobs = updated_parcel_df.loc[updated_parcel_df.index.isin(new_Bellevue_parcel_data_df.index), 'EMPTOT_P'].sum()
    print('Bellevue or BKR jobs before change: ' + str(oldBeljobs))
    print('              after change: ' + str(newBeljobs))
    print('Bellevue or BKR jobs gained ' + str(newBeljobs - oldBeljobs))
    updated_parcel_df.loc[updated_parcel_df.index.isin(new_Bellevue_parcel_data_df.index), Columns_List] = new_Bellevue_parcel_data_df[Columns_List]


    if Set_Jobs_to_Zeros_All_Bel_Parcels_Not_in_New_Parcel_Data_File == True:
        jobs_to_be_zeroed_out = updated_parcel_df.loc[updated_parcel_df.index.isin(missing_bellevue_parcels_df['PARCELID']), 'EMPTOT_P'].sum()
        updated_parcel_df.loc[updated_parcel_df.index.isin(missing_bellevue_parcels_df['PARCELID']), Columns_List] = 0
        print('-----------------------------------------')
        print('Some COB parcels are not provided in the ' + new_Bellevue_parcel_data_file_name + '.')
        print('But they exist in ' + Original_parcel_file_name + '.')
        print('Number of jobs in these parcels are now zeroed out: ' + str(jobs_to_be_zeroed_out))

    
# update the total jobs 
updated_parcel_df['EMPTOT_P'] = updated_parcel_df[Columns_List].sum(axis = 1)
# updated_parcel_df['EMPTOT_P'] = 0
# for col in Columns_List:
#     updated_parcel_df['EMPTOT_P'] += updated_parcel_df[col]     

if Jobs_by_old_BKRTMTAZ_file != '':
    print('processing land use input (by BKRTMTAZ)...')
    # if jobs by old trip model taz (BKRTMTAZ) is provided (either by Kirkland or Redmond), we need to apply the following assumptions to 
    # scale BKRCast jobs up to match the provided jobs.
    '''
    Left is job category in old BKR model. Kirkland is still using these land use categories.
    Right is job category used by BKRCast.
    Use Kirkland's job estimate as control total for each BKRTMTAZ, scale the PSRC jobs in each parcel such that total scaled jobs matches Kirkland's estimate
    in BKRTMTAZ level.
    '''
    control_jobs_by_BKRTMTAZ_df = pd.read_csv(os.path.join(working_folder, Jobs_by_old_BKRTMTAZ_file))
    target_parcels_df = lookup_df.loc[lookup_df['BKRTMTAZ'].isin(control_jobs_by_BKRTMTAZ_df['BKRTMTAZ'])]
    target_parcels_df = updated_parcel_df.reset_index().merge(target_parcels_df[['PSRC_ID', 'BKRTMTAZ']], left_on = 'PARCELID', right_on = 'PSRC_ID')
    temp_col_list = Columns_List.copy()
    temp_col_list.append('EMPTOT_P')
    temp_col_list.append('BKRTMTAZ')
    psrc_jobs = target_parcels_df['EMPTOT_P'].sum()
    psrc_jobs_by_BKRTMTAZ_df = target_parcels_df[temp_col_list].groupby('BKRTMTAZ').sum()
    control_jobs_by_BKRTMTAZ_df = control_jobs_by_BKRTMTAZ_df.merge(psrc_jobs_by_BKRTMTAZ_df.reset_index(), on = 'BKRTMTAZ', how = 'left')
    control_jobs_by_BKRTMTAZ_df.loc[control_jobs_by_BKRTMTAZ_df['EMPTOT_P'] != 0, 'scale'] = control_jobs_by_BKRTMTAZ_df['ControlTotalJobs'] / control_jobs_by_BKRTMTAZ_df['EMPTOT_P']
    control_jobs_by_BKRTMTAZ_df.loc[control_jobs_by_BKRTMTAZ_df['EMPTOT_P'] == 0, 'scale'] = 1
    control_jobs_by_BKRTMTAZ_df.to_csv(os.path.join(working_folder, 'job_scale_comparison.csv'), index = True)

    target_parcels_df = target_parcels_df.merge(control_jobs_by_BKRTMTAZ_df[['BKRTMTAZ', 'scale']], on = 'BKRTMTAZ', how = 'left')
    target_parcels_df['EMPTOT_P'] = 0
    for col in Columns_List:
        target_parcels_df[col] = target_parcels_df[col] * target_parcels_df['scale']
        target_parcels_df[col] = target_parcels_df[col].round(0).astype(int)
        target_parcels_df['EMPTOT_P'] += target_parcels_df[col]    

    target_parcels_df.to_csv(os.path.join(working_folder, 'adjusted_jobs_by_parcel.csv'), index = False)
    scaled_jobs_by_BKRTMTAZ_df = target_parcels_df[temp_col_list].groupby('BKRTMTAZ').sum()
    scaled_jobs_by_BKRTMTAZ_df = scaled_jobs_by_BKRTMTAZ_df.merge(control_jobs_by_BKRTMTAZ_df[['BKRTMTAZ', 'ControlTotalJobs']], on = 'BKRTMTAZ')
    scaled_jobs_by_BKRTMTAZ_df.to_csv(os.path.join(working_folder, 'job_comparison_by_BKRTMTAZ.csv'), index = True)

    updated_parcel_df = updated_parcel_df.loc[~updated_parcel_df['PARCELID'].isin(target_parcels_df['PARCELID'])].copy()
    target_parcels_df.drop(columns = ['index', 'PSRC_ID', 'BKRTMTAZ', 'scale'], inplace = True)
    updated_parcel_df = pd.concat([updated_parcel_df, target_parcels_df], ignore_index=True)
    updated_parcel_df = updated_parcel_df.sort_values(by = ['PARCELID'], ascending = True)

    # update total jobs in each parcel
    updated_parcel_df['EMPTOT_P'] = updated_parcel_df[Columns_List].sum(axis = 1)
    
    new_jobs = updated_parcel_df.loc[updated_parcel_df['PARCELID'].isin(target_parcels_df['PARCELID']), 'EMPTOT_P'].sum()

    print('jobs before change: ' + str(psrc_jobs))
    print('              after change: ' + str(new_jobs))
    print('jobs gained ' + str(new_jobs - psrc_jobs))

print('total jobs before change: ' + str(original_parcel_data_df['EMPTOT_P'].sum()))
print('total jobs after change: ' + str(updated_parcel_df['EMPTOT_P'].sum()))
print('')

print('Exporting parcel files...')
updated_parcel_df.to_csv(os.path.join(working_folder, Updated_parcel_file_name), sep = ' ', index = False)
utility.backupScripts(__file__, os.path.join(working_folder, os.path.basename(__file__)))


print('Done')




