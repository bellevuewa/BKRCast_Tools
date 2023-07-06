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

###################### configuration
### inputs ###
working_folder = r"Z:\Modeling Group\BKRCast\LandUse\Complan\Complan2044\test"
New_parcel_data_file_name = r"2044alt3_COB_Jobs.csv"
Original_parcel_file_name = r"interpolated_parcel_file_2044complan_from_PSRC_2014_2050.txt"
lookup_file = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'

Set_Jobs_to_Zeros_All_Kirk_Parcels_Not_in_New_Parcel_Data_File = True
### output files ###
Updated_parcel_file_name =  r"complan_alt3_parcels_urbansim.txt"
Old_Subset_parcel_file_name = r"Old_parcels_subset.txt"

Columns_List = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPRET_P', 'EMPRSC_P', 'EMPSVC_P', 'EMPOTH_P', 'EMPTOT_P']
####################
new_parcel_data_df = pd.read_csv(os.path.join(working_folder, New_parcel_data_file_name), sep = ',', low_memory = False)
original_parcel_data_df = pd.read_csv(os.path.join(working_folder,Original_parcel_file_name), sep = ' ', low_memory = False)
lookup_df = pd.read_csv(lookup_file, sep = ',', low_memory = False)

full_bellevue_parcels_df = lookup_df.loc[lookup_df['Jurisdiction'] == 'KIRKLAND']
actual_bel_parcels_df = new_parcel_data_df.loc[new_parcel_data_df['PSRC_ID'].isin(full_bellevue_parcels_df['PSRC_ID'])]
not_in_full_kirkland_parcels = actual_bel_parcels_df.loc[~actual_bel_parcels_df['PSRC_ID'].isin(full_bellevue_parcels_df['PSRC_ID'])]
missing_kirkland_parcels_df = original_parcel_data_df.loc[original_parcel_data_df['PARCELID'].isin(full_bellevue_parcels_df.loc[~full_bellevue_parcels_df['PSRC_ID'].isin(new_parcel_data_df['PSRC_ID']), 'PSRC_ID'])]
missing_kirkland_parcels_df.to_csv(os.path.join(working_folder, 'missing_kirkland_parcels.csv'), sep = ',', index = False)
not_in_full_kirkland_parcels.to_csv(os.path.join(working_folder, 'not_valid_kirkland_parcels.csv'), sep = ',', index = False)

newjobs = new_parcel_data_df['EMPTOT_P'].sum() 
print('new parcel data file has ' + str(newjobs)  + ' jobs.')
new_parcel_data_df = new_parcel_data_df.set_index('PSRC_ID')
updated_parcel_df = original_parcel_data_df.copy()
updated_parcel_df = updated_parcel_df.set_index('PARCELID')
oldjobs = updated_parcel_df.loc[updated_parcel_df.index.isin(new_parcel_data_df.index), 'EMPTOT_P'].sum()
print('parcels to be replaced have ' + str(oldjobs) + ' jobs')
print('jobs gained ' + str(newjobs - oldjobs))
updated_parcel_df.loc[updated_parcel_df.index.isin(new_parcel_data_df.index), Columns_List] = new_parcel_data_df[Columns_List]

if Set_Jobs_to_Zeros_All_Kirk_Parcels_Not_in_New_Parcel_Data_File == True:
    jobs_to_be_zeroed_out = updated_parcel_df.loc[updated_parcel_df.index.isin(missing_kirkland_parcels_df['PARCELID']), 'EMPTOT_P'].sum()
    updated_parcel_df.loc[updated_parcel_df.index.isin(missing_kirkland_parcels_df['PARCELID']), Columns_List] = 0
    print('-----------------------------------------')
    print('Some COB parcels are not provided in the ' + New_parcel_data_file_name + '.')
    print('But they exist in ' + Original_parcel_file_name + '.')
    print('Number of jobs in these parcels are now zeroed out: ' + str(jobs_to_be_zeroed_out))

print('total jobs before change: ' + str(original_parcel_data_df['EMPTOT_P'].sum()))
print('total jobs after change: ' + str(updated_parcel_df['EMPTOT_P'].sum()))
print('Exporting parcel files...')
updated_parcel_df.to_csv(os.path.join(working_folder, Updated_parcel_file_name), sep = ' ')

utility.backupScripts(__file__, os.path.join(working_folder, os.path.basename(__file__)))


print('Done')




