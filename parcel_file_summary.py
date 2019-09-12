import os, sys
sys.path.append(os.getcwd())
import pandas as pd
import numpy as np

# Summarize parcel urbansim file to TAZ, subarea and city level.

### Configuration
Original_Parcel_Folder = r"Z:\Modeling Group\BKRCast\Job Conversion Test\parcel_level\test11-2019densitytest"
Original_ESD_Parcel_File_Name = r"parcels_urbansim.txt"
TAZ_Subarea_File_Name = r"Z:\Modeling Group\BKRCast\Job Conversion Test\TAZ_subarea.csv"
###

Output_Field = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPOTH_P', 'EMPRET_P', 'EMPSVC_P', 'EMPTOT_P', 'HH_P']

print "Loading input files ..."
parcels = pd.DataFrame.from_csv(os.path.join(Original_Parcel_Folder, Original_ESD_Parcel_File_Name), sep = " ", index_col = "PARCELID")
taz_subarea = pd.DataFrame.from_csv(os.path.join(Original_Parcel_Folder, TAZ_Subarea_File_Name), sep = ",", index_col = "TAZNUM")

parcels = parcels.join(taz_subarea, on = 'TAZ_P')

summary_by_jurisdiction = parcels.groupby('Jurisdiction')[Output_Field].sum()
print "Exporting \"summary_by_jurisdiction.csv\""
summary_by_jurisdiction.to_csv(os.path.join(Original_Parcel_Folder, "summary_by_jurisdiction.csv"))
summary_by_taz = parcels.groupby('TAZ_P')[Output_Field].sum()
print "Exporting \"summary_by_TAZ.csv\""
summary_by_taz.to_csv(os.path.join(Original_Parcel_Folder, "summary_by_TAZ.csv")) 
print "Exporting \"summary_by_subarea.csv\""
summary_by_subarea = parcels[parcels['Subarea'] > 0].groupby('Subarea')[Output_Field].sum()
taz_subarea = parcels[['Subarea', 'SubareaName']].drop_duplicates()
taz_subarea.set_index('Subarea', inplace = True)
summary_by_subarea = summary_by_subarea.join(taz_subarea['SubareaName'])
summary_by_subarea.to_csv(os.path.join(Original_Parcel_Folder, "summary_by_subarea.csv"))



