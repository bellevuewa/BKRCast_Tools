import os, sys
sys.path.append(os.getcwd())
import pandas as pd
import copy
from shutil import copyfile
import ntpath
import utility

### configuration
work_folder = r"Z:\Modeling Group\BKRCast\LandUse\2044_long_term_planning"
Original_Parcel_File_Name = "2044_Bellevue_complan_parcels_urbansim.txt"
Subset_Parcels_File_Name = "2044_Kirkland_only_Complan_parcel_file.csv"
Output_Parcel_File_Name = "2044_Bellevue_Kirkland_parcels_urbansim.txt"
###

print("Loading input files ...")
parcels = pd.read_csv(os.path.join(work_folder, Original_Parcel_File_Name), sep = ' ')
original_tot_jobs = parcels['EMPTOT_P'].sum()
print('Original jobs {0:.0f}'.format(original_tot_jobs))

subset_parcels = pd.read_csv(os.path.join(work_folder, Subset_Parcels_File_Name), sep = ',')
updated_parcels = parcels.loc[~parcels['PARCELID'].isin(subset_parcels['PARCELID'])]
updated_parcels = pd.concat([updated_parcels, subset_parcels], ignore_index=True)

subset_new_tot_jobs = subset_parcels['EMPTOT_P'].sum()
print('Subset new jobs {0:.0f}'.format(subset_new_tot_jobs))
subset_old_tot_jobs = parcels.loc[parcels['PARCELID'].isin(subset_parcels['PARCELID'])]['EMPTOT_P'].sum()
print('Subset old jobs {0:.0f}'.format(subset_old_tot_jobs))

updated_tot_jobs = updated_parcels['EMPTOT_P'].sum()
print('Updated total jobs {0:.0f}'.format(updated_tot_jobs))

print("Exporting updated urbansim parcel file ...")
updated_parcels.to_csv(os.path.join(work_folder, Output_Parcel_File_Name), index = False, sep = ' ')


# backup input files inside input folder
print("Backup input files ..." )
input_backup_folder = os.path.join(work_folder, 'inputs')
if not os.path.exists(input_backup_folder):
    os.makedirs(input_backup_folder) 
copyfile(os.path.join(work_folder, Original_Parcel_File_Name), os.path.join(input_backup_folder, ntpath.basename(Original_Parcel_File_Name)))
copyfile(os.path.join(work_folder, Subset_Parcels_File_Name), os.path.join(input_backup_folder, ntpath.basename(Subset_Parcels_File_Name)))

utility.backupScripts(__file__, os.path.join(work_folder, os.path.basename(__file__)))

print("Finished."  )