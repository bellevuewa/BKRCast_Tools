# Extract a subset of parcel file, defined by a list of TAZ numbers or parcel IDs, and export it to an external file.

import os, sys
sys.path.append(os.getcwd())
import pandas as pd
import numpy as np


### Configuration
SELECT_BY_TAZ = False
SELECT_BY_PARCEL = True
Original_Parcel_Folder = r"Z:\Modeling Group\BKRCast\LandUse\TFP\2033_Spring_Dist_New_Development_Baseline"
Common_Data_Folder = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData'
Original_ESD_Parcel_File_Name = r"2033_horizenTFP_parcels_urbansim.txt"
TAZ_Subarea_File_Name = r"TAZ_subarea.csv"
Subset_definition_file = r"customized.csv"   # TAZ list or ParcelID list
Outputfile = 'spring_district_2033_selected_parcels.csv'
###

print("Loading input files ...")
parcels_df = pd.read_csv(os.path.join(Original_Parcel_Folder, Original_ESD_Parcel_File_Name), sep = " ")
taz_subarea_df = pd.read_csv(os.path.join(Common_Data_Folder, TAZ_Subarea_File_Name), sep = ",")
subset_def_df = pd.read_csv(os.path.join(Original_Parcel_Folder, Subset_definition_file), sep = ',')
# parcels_df = parcels_df.join(taz_subarea_df, on = 'TAZ_P')

if SELECT_BY_TAZ: 
    parcels_selected_df = pd.merge(parcels_df, subset_def_df, left_on = 'TAZ_P', right_on = 'TAZ', how = 'right')
elif SELECT_BY_PARCEL:
    parcels_selected_df = pd.merge(parcels_df, subset_def_df, left_on = 'PARCELID', right_on = 'PSRC_ID', how = 'right')  # join by index

parcels_selected_df.to_csv(os.path.join(Original_Parcel_Folder, Outputfile), index = False, sep =',')

print("Parcel subset is exported.")

