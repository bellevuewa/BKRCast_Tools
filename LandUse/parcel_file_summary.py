import os, sys
sys.path.append(os.getcwd())
import pandas as pd
import numpy as np
import utility

# Summarize parcel urbansim file to TAZ, subarea and city level.
#2/3/2022
# upgrade to python 3.7

### Configuration
Original_Parcel_Folder = r"Z:\Modeling Group\BKRCast\LandUse\Complan\Complan2044\NA"
Original_ESD_Parcel_File_Name = r"parcels_urbansim.txt"
TAZ_Subarea_File_Name = r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\TAZ_subarea.csv"
###

Output_Field = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPOTH_P', 'EMPRET_P', 'EMPSVC_P', 'EMPTOT_P', 'STUGRD_P', 'STUHGH_P', 'STUUNI_P', 'HH_P']

print("Loading input files ...")
parcels = pd.read_csv(os.path.join(Original_Parcel_Folder, Original_ESD_Parcel_File_Name), sep = ' ')
taz_subarea = pd.read_csv(os.path.join(Original_Parcel_Folder, TAZ_Subarea_File_Name), sep = ',')

parcels = parcels.merge(taz_subarea, how = 'left',  left_on = 'TAZ_P', right_on = 'BKRCastTAZ')

summary_by_jurisdiction = parcels.groupby('Jurisdiction')[Output_Field].sum()
print("Exporting \"summary_by_jurisdiction.csv\"")
summary_by_jurisdiction.to_csv(os.path.join(Original_Parcel_Folder, "summary_by_jurisdiction.csv"))
summary_by_taz = parcels.groupby('TAZ_P')[Output_Field].sum()
print("Exporting \"summary_by_TAZ.csv\"")
summary_by_taz.to_csv(os.path.join(Original_Parcel_Folder, "summary_by_TAZ.csv")) 
print("Exporting \"summary_by_subarea.csv\"")
summary_by_subarea = parcels[parcels['Subarea'] > 0].groupby('Subarea')[Output_Field].sum()
taz_subarea = parcels[['Subarea', 'SubareaName']].drop_duplicates()
taz_subarea.set_index('Subarea', inplace = True)
summary_by_subarea = summary_by_subarea.join(taz_subarea['SubareaName'])
summary_by_subarea.to_csv(os.path.join(Original_Parcel_Folder, "summary_by_subarea.csv"))

# copy source file
utility.backupScripts(__file__, os.path.join(Original_Parcel_Folder, os.path.basename(__file__)))
print('Done')
