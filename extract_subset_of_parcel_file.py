# Extract a subset of parcel fille and export it to an external file.

import os, sys
sys.path.append(os.getcwd())
import pandas as pd
import numpy as np

SELECT_BY_TAZ = True
SELECT_BY_PARCEL = False

Original_Parcel_Folder = r"Z:\Modeling Group\BKRCast\Job Conversion Test\parcel_level\test11"
Original_ESD_Parcel_File_Name = r"parcels_urbansim.txt"
TAZ_Subarea_File_Name = r"TAZ_subarea.csv"
Subset_definition_file = r"Bel_CBD_TAZ.csv"
Outputfile = '2014_BelCBD_Parcels.txt'

print "Loading input files ..."
parcels_df = pd.DataFrame.from_csv(os.path.join(Original_Parcel_Folder, Original_ESD_Parcel_File_Name), sep = " ", index_col = "PARCELID")
taz_subarea_df = pd.DataFrame.from_csv(os.path.join(Original_Parcel_Folder, TAZ_Subarea_File_Name), sep = ",", index_col = "TAZNUM")
subset_def_df = pd.DataFrame.from_csv(os.path.join(Original_Parcel_Folder, Subset_definition_file), sep = ',')
parcels_df = parcels_df.join(taz_subarea_df, on = 'TAZ_P')
# parcels_selected_df = parcels_df[parcels_df['Jurisdiction'] == 'BELLEVUE']
# parcels_selected_df = parcels_df[parcels_df['TAZ_P'] == 879]

if SELECT_BY_TAZ: 
    parcels_selected_df = parcels_df.join(subset_def_df, on = 'TAZ_P', how = 'right')
elif SELECT_BY_PARCEL:
    parcels_selected_df = parcels_df.join(subset_def_df, how = 'right')  # join by index

parcels_selected_df.to_csv(os.path.join(Original_Parcel_Folder, Outputfile), sep =' ')

print "Parcel subset is exported."

