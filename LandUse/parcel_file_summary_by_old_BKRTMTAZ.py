from cgitb import lookup
import os, sys
sys.path.append(os.getcwd())
import pandas as pd
import numpy as np
import utility

# Summarize parcel urbansim file to TAZ, subarea and city level.
#2/3/2022
# upgrade to python 3.7

### Configuration
Original_Parcel_Folder = r"Z:\Modeling Group\BKRCast\LandUse\2019baseyear-new_hhs_alloc_approach"
Original_ESD_Parcel_File_Name = r"parcels_urbansim.txt"
lookup_File_Name = r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv"
###

Output_Field = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPOTH_P', 'EMPRET_P', 'EMPSVC_P', 'EMPTOT_P', 'STUGRD_P', 'STUHGH_P', 'STUUNI_P', 'HH_P']

print("Loading input files ...")
parcels_df = pd.read_csv(os.path.join(Original_Parcel_Folder, Original_ESD_Parcel_File_Name), sep = ' ')
lookup_df = pd.read_csv(os.path.join(Original_Parcel_Folder, lookup_File_Name), sep = ',')
parcels_df = parcels_df.merge(lookup_df[['PSRC_ID', 'BKRTMTAZ', 'Jurisdiction']], how = 'left',  left_on = 'PARCELID', right_on = 'PSRC_ID')
kirkland_parcels_df = parcels_df.loc[parcels_df['Jurisdiction'] == 'KIRKLAND']

summary_by_taz = kirkland_parcels_df.groupby('BKRTMTAZ')[Output_Field].sum()
print("Exporting \"summary_by_TAZ.csv\"")
summary_by_taz.to_csv(os.path.join(Original_Parcel_Folder, 'for_Kirkland_Compan_Support', "2019_Kirkland_jobs_by_old_BKRTMTAZ.csv")) 

# copy source file
utility.backupScripts(__file__, os.path.join(Original_Parcel_Folder, os.path.basename(__file__)))
print('Done')
