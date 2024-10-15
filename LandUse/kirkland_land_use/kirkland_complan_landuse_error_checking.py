import pandas as pd
import os
import numpy as np
import utility

working_folder = r'Z:\Modeling Group\BKRCast\KirklandSupport\Kirkland2044Complan\preferred_2044'
lookup_file = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'
input_landuse_file = r"Kirkland_Complan_2044_target_Landuse.csv"

print('loading...')
input_df = pd.read_csv(os.path.join(working_folder, input_landuse_file))
lookup_df = pd.read_csv(lookup_file, low_memory = False)

# cleaning
input_df = input_df.merge(lookup_df[['PSRC_ID', 'BKRCastTAZ', 'Jurisdiction']], on = 'PSRC_ID', how = 'left')
wrong_parcels_df = input_df.loc[input_df['Jurisdiction'] != 'KIRKLAND']

wrong_parcels_df.to_csv(os.path.join(working_folder, 'kirkland_complan_target_wrong_parcels.csv'), index = False)

utility.backupScripts(__file__, os.path.join(working_folder, os.path.basename(__file__)))
print('Done')


