# Extract a subset of parcel file, defined by a list of TAZ numbers or parcel IDs, and export it to an external file.

import os, sys
sys.path.append(os.getcwd())
import pandas as pd
import numpy as np


### Configuration
SELECT_BY_TAZ = True
SELECT_BY_PARCEL = False
Original_Parcel_Folder = r"Z:\Modeling Group\BKRCast\LandUse\2035DTAccess"
Common_Data_Folder = r'Z:\Modeling Group\BKRCast\CommonData'
Original_ESD_Parcel_File_Name = r"parcels_urbansim.txt"
TAZ_Subarea_File_Name = r"TAZ_subarea.csv"
Subset_definition_file = r"BellevueCBDTaz.csv"   # TAZ list or ParcelID list
Outputfile = '2035_BellevueCBD.csv'
###

print "Loading input files ..."
parcels_df = pd.read_csv(os.path.join(Original_Parcel_Folder, Original_ESD_Parcel_File_Name), sep = " ", index_col = "PARCELID")
taz_subarea_df = pd.read_csv(os.path.join(Common_Data_Folder, TAZ_Subarea_File_Name), sep = ",", index_col = "TAZNUM")
subset_def_df = pd.read_csv(os.path.join(Common_Data_Folder, Subset_definition_file), sep = ',')
# parcels_df = parcels_df.join(taz_subarea_df, on = 'TAZ_P')

if SELECT_BY_TAZ: 
    parcels_selected_df = parcels_df.join(subset_def_df, on = 'TAZ_P', how = 'right')
elif SELECT_BY_PARCEL:
    parcels_selected_df = parcels_df.join(subset_def_df, how = 'right')  # join by index

parcels_selected_df.to_csv(os.path.join(Original_Parcel_Folder, Outputfile), sep =',')

print "Parcel subset is exported."

