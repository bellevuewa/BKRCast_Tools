import os, sys
sys.path.append(os.getcwd())
import pandas as pd
import numpy as np
import utility

# Summarize parcel urbansim file to TAZ, subarea and city level.
#2/3/2022
# upgrade to python 3.7

### Configuration
Original_Parcel_Folder = r"Z:\Modeling Group\BKRCast\KirklandSupport\Kirkland2044Complan\preferred_2044"
Original_ESD_Parcel_File_Name = r"2044_kirkcomplan_target_parcels_urbansim_jobs_scaled_from_baseline_sync_with_popsim.txt"
TAZ_Subarea_File_Name = r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\TAZ_subarea.csv"
lookup_file = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'
###

Output_Field = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPOTH_P', 'EMPRET_P', 'EMPSVC_P', 'EMPTOT_P', 'STUGRD_P', 'STUHGH_P', 'STUUNI_P', 'HH_P']

print("Loading input files ...")
parcels = pd.read_csv(os.path.join(Original_Parcel_Folder, Original_ESD_Parcel_File_Name), sep = ' ')
taz_subarea = pd.read_csv(os.path.join(Original_Parcel_Folder, TAZ_Subarea_File_Name), sep = ',')
lookup_df = pd.read_csv(lookup_file)

# parcels = parcels.merge(taz_subarea, how = 'left',  left_on = 'TAZ_P', right_on = 'BKRCastTAZ')
parcels = parcels.merge(lookup_df, how = 'left',  left_on = 'PARCELID', right_on = 'PSRC_ID')

summary_by_jurisdiction = parcels.groupby('Jurisdiction')[Output_Field].sum()
print("Exporting \"summary_by_jurisdiction.csv\"")
summary_by_jurisdiction.to_csv(os.path.join(Original_Parcel_Folder, "summary_by_jurisdiction.csv"))
summary_by_taz = parcels.groupby('TAZ_P')[Output_Field].sum()
print("Exporting \"summary_by_TAZ.csv\"")
summary_by_taz.to_csv(os.path.join(Original_Parcel_Folder, "summary_by_TAZ.csv")) 
print("Exporting \"summary_by_subarea.csv\"")
parcels = parcels.merge(taz_subarea[['BKRCastTAZ', 'Subarea', 'SubareaName']], how = 'left',  left_on = 'TAZ_P', right_on = 'BKRCastTAZ')
summary_by_subarea = parcels[parcels['Subarea'] > 0].groupby('Subarea')[Output_Field].sum()
taz_subarea = parcels[['Subarea', 'SubareaName']].drop_duplicates()
taz_subarea.set_index('Subarea', inplace = True)
summary_by_subarea = summary_by_subarea.join(taz_subarea['SubareaName'])
summary_by_subarea.to_csv(os.path.join(Original_Parcel_Folder, "summary_by_subarea.csv"))

# copy source file
utility.backupScripts(__file__, os.path.join(Original_Parcel_Folder, os.path.basename(__file__)))
print('Done')
