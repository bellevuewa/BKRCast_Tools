# This program is used to categorize GOV jobs to OFFICE jobs and export 
# final parcel file.
#

import pandana as pdna
import os, sys
sys.path.append(os.getcwd())
import pandas as pd
import numpy as np
import re
from pyproj import Proj, transform


Original_Parcel_Folder = r"Z:\Modeling Group\BKRCast\2014ESD"
Original_Parcel_File_Name = r"parcels_urbansim.txt"
Output_Parcel_Folder = r"Z:\Modeling Group\BKRCast\2014ESD"
Output_Parcel_File_Name = "parcels_urbansim_noGOV.txt"

parcels = pd.DataFrame.from_csv(os.path.join(Original_Parcel_Folder, Original_Parcel_File_Name), sep = " ", index_col = None)

#capitalize all column names to avoid unexpected errors
parcels.columns = [i.upper() for i in parcels.columns]

# in regional level, no parcel file column sum should be zero. 
for col_name in parcels.columns:
    # daysim does not use EMPRSC_P
    if col_name <> 'EMPRSC_P':
        if parcels[col_name].sum() == 0:
            print col_name + ": columne sum is zero. Check your parcel file."
            sys.exit(1)


# reclassify all gove jobs to office jobs
parcels["EMPOFC_P"] = parcels["EMPOFC_P"] + parcels["EMPGOV_P"]
parcels["EMPGOV_P"] = 0

parcels.to_csv(os.path.join(Output_Parcel_Folder, Output_Parcel_File_Name), index = False, sep = ' ')



