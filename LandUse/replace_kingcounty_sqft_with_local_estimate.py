### This tool is used to update King County sqft data (from Gwen) Bellevue portion with local estimate (also provided by Gwen)
### the updated file is exported to Output_Parcel_File_Name. This output file include Bellevue local estimate of commercial sqft
### and King County Assessors' estimate of commercial sqft outside of Bellevue.

### This output is used as an input file in UpdateParcelFileByParcel.py.

import pandana as pdna
import os, sys
sys.path.append(os.getcwd())
import pandas as pd
import numpy as np
import re
from pyproj import Proj, transform
import copy
from shutil import copyfile
import ntpath

### configuration
Original_Parcel_File_Name = r"Z:\Modeling Group\BKRCast\2018LU\2018_kingcounty_LU_by_parcel.csv"
Output_Parcel_Folder = r"Z:\Modeling Group\BKRCast\2018LU"
Subset_Parcels_File_Name = r"Z:\Modeling Group\BKRCast\2018LU\2018_Bellevue_LU_by_parcel.csv"
Output_Parcel_File_Name = "updated_2018_kingcounty_LU_by_parcel.csv"
###

CONVERSION_LEVEL = ['verylow', 'low', 'med', 'high', 'veryhigh']
EMPLOYMENT_TYPE = ['EDU', 'FOO', 'GOV', 'IND', 'MED', 'OFC', 'OTH', 'RET', 'SVC']

if not os.path.exists(Output_Parcel_Folder):
    os.makedirs(Output_Parcel_Folder)

lu_type_list = []
for lu_type in EMPLOYMENT_TYPE:
    for level in CONVERSION_LEVEL:
        new_lu_type = lu_type + "_" + level
        new_lu_type = new_lu_type.upper()
        lu_type_list.append(new_lu_type)

print "Loading input files ..."
parcels = pd.read_csv(Original_Parcel_File_Name, sep = ',')
parcels.columns = [i.upper() for i in parcels.columns]
parcels = parcels.groupby('PSRC_ID')[lu_type_list].sum()
print 'original parcel total sqft: ', parcels[lu_type_list].sum().sum()

subset_parcels = pd.read_csv(Subset_Parcels_File_Name, sep = ',')
subset_parcels.columns = [i.upper() for i in subset_parcels.columns]
subset_parcels = subset_parcels.groupby('PSRC_ID')[lu_type_list].sum()
updated_parcels = parcels.loc[~parcels.index.isin(subset_parcels.index)]
updated_parcels = updated_parcels.append(subset_parcels) 

subset_new_tot_sqft = subset_parcels[lu_type_list].sum().sum()
print 'Subset new sqft {0:.0f}'.format(subset_new_tot_sqft)
subset_old_tot_sqft = parcels.loc[parcels.index.isin(subset_parcels.index), lu_type_list].sum().sum()
print 'Subset old sqft {0:.0f}'.format(subset_old_tot_sqft)

updated_tot_sqft = updated_parcels[lu_type_list].sum().sum()
print 'Updated total sqft {0:.0f}'.format(updated_tot_sqft)

print "Exporting updated urbansim parcel file ..."
updated_parcels.to_csv(os.path.join(Output_Parcel_Folder, Output_Parcel_File_Name), sep = ',')



print "Finished."  