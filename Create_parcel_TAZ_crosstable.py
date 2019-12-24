# Create parcelID and BKRCast_TAZ crosswalk table through a urbansim parcle file.

import os, sys
sys.path.append(os.getcwd())
import pandas as pd
import numpy as np



Original_Parcel_Folder = r"Z:\Modeling Group\BKRCast\Job Conversion Test\parcel_level\test11"
Original_ESD_Parcel_File_Name = r"parcels_urbansim.txt"
TAZ_Subarea_File_Name = r"TAZ_subarea.csv"


print "Loading input files ..."
parcels = pd.DataFrame.from_csv(os.path.join(Original_Parcel_Folder, Original_ESD_Parcel_File_Name), sep = " ", index_col = "PARCELID")
parcels['TAZ_P'] = parcels['TAZ_P'].astype(np.int32)
crosswalk = parcels[['TAZ_P']]
crosswalk.to_csv(os.path.join(Original_Parcel_Folder, 'parcel_BKRTAZ.csv'))
print "Crosswalk table is created."



