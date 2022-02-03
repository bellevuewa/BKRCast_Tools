### This tool is used to update a subset of parcel files with an external file.
### The external file has to be have the same format with the original file.

import os, sys
sys.path.append(os.getcwd())
import pandas as pd
import copy
from shutil import copyfile
import ntpath
import utility

### configuration
Original_Parcel_File_Name = r"Z:\Modeling Group\BKRCast\LandUse\2020baseyear\parcels_urbansim_with_local_jobs_hhs.txt"
Subset_Parcels_File_Name = r"Z:\Modeling Group\BKRCast\LandUse\2020baseyear\updated_rest_of_KC_parcels.txt"
Output_Parcel_Folder = r"Z:\Modeling Group\BKRCast\LandUse\2020baseyear"
Output_Parcel_File_Name = "parcels_urbansim.txt"
###

if not os.path.exists(Output_Parcel_Folder):
    os.makedirs(Output_Parcel_Folder)

print "Loading input files ..."
parcels = pd.read_csv(Original_Parcel_File_Name, sep = ' ')
original_tot_jobs = parcels['EMPTOT_P'].sum()
print 'Original jobs {0:.0f}'.format(original_tot_jobs)

subset_parcels = pd.read_csv(Subset_Parcels_File_Name, sep = ' ')
updated_parcels = parcels.loc[~parcels['PARCELID'].isin(subset_parcels['PARCELID'])]
updated_parcels = updated_parcels.append(subset_parcels) 

subset_new_tot_jobs = subset_parcels['EMPTOT_P'].sum()
print 'Subset new jobs {0:.0f}'.format(subset_new_tot_jobs)
subset_old_tot_jobs = parcels.loc[parcels['PARCELID'].isin(subset_parcels['PARCELID'])]['EMPTOT_P'].sum()
print 'Subset old jobs {0:.0f}'.format(subset_old_tot_jobs)

updated_tot_jobs = updated_parcels['EMPTOT_P'].sum()
print 'Updated total jobs {0:.0f}'.format(updated_tot_jobs)

print "Exporting updated urbansim parcel file ..."
updated_parcels.to_csv(os.path.join(Output_Parcel_Folder, Output_Parcel_File_Name), index = False, sep = ' ')


# backup input files inside input folder
print "Backup input files ..."
input_backup_folder = os.path.join(Output_Parcel_Folder, 'inputs')
if not os.path.exists(input_backup_folder):
    os.makedirs(input_backup_folder) 
copyfile(Original_Parcel_File_Name, os.path.join(input_backup_folder, ntpath.basename(Original_Parcel_File_Name)))
copyfile(Subset_Parcels_File_Name, os.path.join(input_backup_folder, ntpath.basename(Subset_Parcels_File_Name)))

utility.backupScripts(__file__, os.path.join(Output_Parcel_Folder, os.path.basename(__file__)))

print "Finished."  