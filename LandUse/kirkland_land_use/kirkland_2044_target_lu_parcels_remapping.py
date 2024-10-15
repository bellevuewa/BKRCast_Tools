import pandas as pd
import os
import utility

working_folder = r'Z:\Modeling Group\BKRCast\KirklandSupport\Kirkland2044Complan\preferred_2044'
parcel_remapping_file = 'kirkland_complan_target_wrong_parcels_fix.csv'
input_landuse_file = r"Kirkland_Complan_2044_target_Landuse.csv"

print('loading...')
input_df = pd.read_csv(os.path.join(working_folder, input_landuse_file))
remapping_df = pd.read_csv(os.path.join(working_folder, parcel_remapping_file))

remapping_dict = pd.Series(remapping_df['new_parcel_ID'].values, index = remapping_df['PSRC_ID']).to_dict()
for oldid, newid in remapping_dict.items():
    input_df.loc[input_df['PSRC_ID'] == oldid, 'PSRC_ID'] = newid 

fixed_df = input_df.groupby('PSRC_ID').sum().reset_index()
fixed_df.to_csv(os.path.join(working_folder, 'parcel_fixed_Kirkland_Complan_2044_target_Landuse.csv'), index = False)
# cleaning

utility.backupScripts(__file__, os.path.join(working_folder, os.path.basename(__file__)))
print('Done')



