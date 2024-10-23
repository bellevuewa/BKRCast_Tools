import pandas as pd
import os
import utility

working_folder = r'Z:\Modeling Group\BKRCast\KirklandSupport\Kirkland2044Complan\preferred_2044'
parcel_remapping_file = 'kirkland_complan_target_wrong_parcels_fix.csv'
input_landuse_file = r"Kirkland_Complan_2044_target_Landuse.csv"
lookup_file = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'

print('loading...')
input_df = pd.read_csv(os.path.join(working_folder, input_landuse_file))
remapping_df = pd.read_csv(os.path.join(working_folder, parcel_remapping_file))
lookup_df = pd.read_csv(lookup_file, low_memory = False)

remapping_dict = pd.Series(remapping_df['new_parcel_ID'].values, index = remapping_df['PSRC_ID']).to_dict()
for oldid, newid in remapping_dict.items():
    input_df.loc[input_df['PSRC_ID'] == oldid, 'PSRC_ID'] = newid 

jobs_field = {'EMPTOT_2044':['EMPCOM_2044', 'EMPIND_2044', 'EMPOFF_2044', 'EMPINST_2044'],
                'TotalDU_2044':['SFU_2044', 'MFU_2044']}


fixed_df = input_df.groupby('PSRC_ID').sum().reset_index()
fixed_df = fixed_df.merge(lookup_df[['PSRC_ID', 'BKRCastTAZ']], on = 'PSRC_ID', how = 'left')
fixed_df['EMPOFF_2044'] =  fixed_df['EMPOFF_2044'] + fixed_df['EMPHO_2044']
fixed_df.drop(columns = ['EMPHO_2044'], inplace = True)

for key, vals in jobs_field.items():
    fixed_df[key] = 0
    for val in vals:
        fixed_df[key] += fixed_df[val]

fixed_df.to_csv(os.path.join(working_folder, 'parcel_fixed_Kirkland_Complan_2044_target_Landuse.csv'), index = False)
summary_by_TAZ_df = fixed_df.groupby('BKRCastTAZ').sum()
summary_by_TAZ_df = summary_by_TAZ_df.drop(columns = ['PSRC_ID'])
summary_by_TAZ_df.to_csv(os.path.join(working_folder, '2044_Kirk_Target_LU_by_TAZ.csv'))
# cleaning

utility.backupScripts(__file__, os.path.join(working_folder, os.path.basename(__file__)))
print('Done')



