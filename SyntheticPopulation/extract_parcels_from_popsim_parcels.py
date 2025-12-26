import os, sys
sys.path.append(os.getcwd())
import pandas as pd
import numpy as np
import utility


### Configuration
SELECT_BY_TAZ = True
Original_Parcel_Folder = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2044_long_term_plan"
Common_Data_Folder = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData'
Original_ESD_Parcel_File_Name = r"2044Kirkland_complan_preferred_hh_summary_by_parcel.csv"
Subset_definition_file = r"Kirkland_TAZ.txt"   # TAZ list or ParcelID list
work_folder = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2044_long_term_plan"
Outputfile = '2044_Kirkland_only_hh_summary_by_parcel.csv'
###

print("Loading input files ...")
parcels_df = pd.read_csv(os.path.join(Original_Parcel_Folder, Original_ESD_Parcel_File_Name), sep = ",")
subset_def_df = pd.read_csv(os.path.join(Common_Data_Folder, Subset_definition_file), sep = ',')
# parcels_df = parcels_df.join(taz_subarea_df, on = 'TAZ_P')

parcels_selected_df = parcels_df.loc[parcels_df['BKRCastTAZ'].isin(subset_def_df['TAZ'])]

parcels_selected_df.to_csv(os.path.join(work_folder, Outputfile), index = False, sep =',')

utility.backupScripts(__file__, os.path.join(work_folder, os.path.basename(__file__)))
print("Parcel subset is exported.")

