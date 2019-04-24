### This tool is used to update a subset of parcel files with an external file.
### The external file has to be have the same format with the original file.

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
Original_Parcel_File_Name = r"Z:\Modeling Group\BKRCast\Job Conversion Test\parcel_level\test11\parcels_urbansim.txt"
Output_Parcel_Folder = r"Z:\Modeling Group\BKRCast\test"
Subset_Parcels_File_Name = r"Z:\Modeling Group\BKRCast\test\2014_BelCBD_Parcels_test.csv"
Output_Parcel_File_Name = "parcels_urbansim_Updated.txt"
###

if not os.path.exists(Output_Parcel_Folder):
    os.makedirs(Output_Parcel_Folder)

print "Loading input files ..."
parcels = pd.DataFrame.from_csv(Original_Parcel_File_Name, index_col = 'PARCELID', sep = ' ')
original_tot_jobs = parcels['EMPTOT_P'].sum()
print 'Original jobs {0:.0f}'.format(original_tot_jobs)

subset_parcels = pd.DataFrame.from_csv(Subset_Parcels_File_Name, index_col = 'PARCELID', sep = ',')
updated_parcels = parcels.loc[~parcels.index.isin(subset_parcels.index)]
updated_parcels = updated_parcels.append(subset_parcels) 

subset_new_tot_jobs = subset_parcels['EMPTOT_P'].sum()
print 'Subset new jobs {0:.0f}'.format(subset_new_tot_jobs)
subset_old_tot_jobs = parcels.loc[subset_parcels.index]['EMPTOT_P'].sum()
print 'Subset old jobs {0:.0f}'.format(subset_old_tot_jobs)

updated_tot_jobs = updated_parcels['EMPTOT_P'].sum()
print 'Updated total jobs {0:.0f}'.format(updated_tot_jobs)

print "Exporting updated urbansim parcel file ..."
parcels.to_csv(os.path.join(Output_Parcel_Folder, Output_Parcel_File_Name), index = True, sep = ' ')


# backup input files inside input folder
print "Backup input files ..."
input_backup_folder = os.path.join(Output_Parcel_Folder, 'inputs')
if not os.path.exists(input_backup_folder):
    os.makedirs(input_backup_folder) 
copyfile(Original_Parcel_File_Name, os.path.join(input_backup_folder, ntpath.basename(Original_Parcel_File_Name)))
copyfile(Subset_Parcels_File_Name, os.path.join(input_backup_folder, ntpath.basename(Subset_Parcels_File_Name)))

print "Finished."  