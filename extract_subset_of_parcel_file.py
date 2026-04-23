# Extract a subset of parcel file, defined by a list of TAZ numbers or parcel IDs, and export it to an external file.

import os, sys
sys.path.append(os.getcwd())
import pandas as pd
import numpy as np
import utility


### Configuration
SELECT_BY_TAZ = True
SELECT_BY_PARCEL = False
Original_Parcel_Folder = r"Z:\Modeling Group\BKRCast\KirklandSupport\Kirkland2044Complan\preferred_2044"
Common_Data_Folder = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData'
Original_ESD_Parcel_File_Name = r"parcels_urbansim.txt"
TAZ_Subarea_File_Name = r"TAZ_subarea.csv"
Subset_definition_file = r"Kirkland_TAZ.txt"   # TAZ list or ParcelID list
work_folder = r"Z:\Modeling Group\BKRCast\LandUse\2044_long_term_planning"
Outputfile = '2044_Kirkland_only_Complan_parcel_file.csv'
###

print("Loading input files ...")
parcels_df = pd.read_csv(os.path.join(Original_Parcel_Folder, Original_ESD_Parcel_File_Name), sep = " ")
taz_subarea_df = pd.read_csv(os.path.join(Common_Data_Folder, TAZ_Subarea_File_Name), sep = ",")
subset_def_df = pd.read_csv(os.path.join(Common_Data_Folder, Subset_definition_file), sep = ',')
# parcels_df = parcels_df.join(taz_subarea_df, on = 'TAZ_P')

if SELECT_BY_TAZ: 
    parcels_selected_df = pd.merge(parcels_df, subset_def_df, left_on = 'TAZ_P', right_on = 'TAZ', how = 'right')
elif SELECT_BY_PARCEL:
    parcels_selected_df = pd.merge(parcels_df, subset_def_df, left_on = 'PARCELID', right_on = 'PSRC_ID', how = 'right')  # join by index

parcels_selected_df = parcels_selected_df.drop(columns = ['TAZ'] )  # drop rows with NaN in PARCELID column
parcels_selected_df.to_csv(os.path.join(work_folder, Outputfile), index = False, sep =',')

utility.backupScripts(__file__, os.path.join(work_folder, os.path.basename(__file__)))
print("Parcel subset is exported.")

