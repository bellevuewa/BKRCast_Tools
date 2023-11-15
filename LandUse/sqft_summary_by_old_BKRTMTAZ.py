import os, sys
sys.path.append(os.getcwd())
import pandas as pd
import numpy as np
import utility

# Summarize parcel urbansim file to TAZ, subarea and city level.
#2/3/2022
# upgrade to python 3.7

### Configuration
Original_Parcel_Folder = r"Z:\Modeling Group\BKRCast\LandUse\Kirkland_Complan_Support\2019LU"
lu_file_name = r"2019_kc_simplified.csv"
lookup_file_name = r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv"
###

Output_Field_sqft = ['EDU_19', 'FOO_19', 'GOV_19', 'IND_19', 'MED_19', 'OFC_19', 'OTH_19', 'RET_19', 'SVC_19']
print("Loading input files ...")
parcels_df = pd.read_csv(os.path.join(Original_Parcel_Folder, lu_file_name), sep = ',')
lookup_df = pd.read_csv(os.path.join(Original_Parcel_Folder, lookup_file_name), sep = ',')

parcels_df = parcels_df.merge(lookup_df[['PSRC_ID', 'BKRTMTAZ']], how = 'left',  on = 'PSRC_ID')
parcels_df['Total_19'] = 0
for col in Output_Field_sqft:
    parcels_df[col] = parcels_df[col].round(0).astype(int)    
    parcels_df['Total_19'] += parcels_df[col]
Output_Field_sqft.append('Total_19')    
kirkland_parcels_df = parcels_df.loc[parcels_df['JURISDICTION'] == 'KIRKLAND']
summary_by_taz = kirkland_parcels_df.groupby('BKRTMTAZ')[Output_Field_sqft].sum()
print("Exporting...")
summary_by_taz.to_csv(os.path.join(Original_Parcel_Folder, "kikland_2019_SQFT_summary_by_BKRTMTAZ.csv")) 

summary_by_BKRCastTAZ = kirkland_parcels_df.groupby('BKRCastTAZ')[Output_Field_sqft].sum()
summary_by_BKRCastTAZ.to_csv(os.path.join(Original_Parcel_Folder, 'Kirkland_2019_SQFT_summary_by_BKRCastTAZ.csv'))
# copy source file
utility.backupScripts(__file__, os.path.join(Original_Parcel_Folder, os.path.basename(__file__)))
print('Done')
