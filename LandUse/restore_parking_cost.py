import os, sys
import pandas as pd
import utility

# copy parking cost data ['PARKDY_P', 'PARKHR_P', 'PPRICDYP', 'PPRICHRP'] from the Parking_Cost_Source to Original_ESD_Parcel_File_Name file.
############ configuration
### input files
Project_Folder =  r"Z:\Modeling Group\BKRCast\LandUse\TFP\2018TFP sensitivity"
Original_ESD_Parcel_File_Name = r"interpolated_parcel_file_2030.txt"
TAZ_Subarea_File_Name = r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\TAZ_subarea.csv"
Parking_Cost_Source =  r"Z:\Modeling Group\BKRCast\LandUse\2019baseyear\parcels_urbansim.txt"
Updated_Parcel_File_Name = r'interpolated_parcel_file_2030_with_half_Bellevue_parkingcost.txt'

Parking_Cost_Attributes = ['PARKDY_P', 'PARKHR_P', 'PPRICDYP', 'PPRICHRP']
SET_BELLEVUE_PARKING_COST_HALF = False

###################

print "Loading input files ..."
original_parcel_df = pd.read_csv(os.path.join(Project_Folder, Original_ESD_Parcel_File_Name), sep = " ", index_col = "PARCELID", low_memory = False)
taz_subarea_df = pd.read_csv(os.path.join(Project_Folder,TAZ_Subarea_File_Name), sep = ",", index_col = "BKRCastTAZ")

# drop four parking attributes 
update_parcel_df = original_parcel_df.drop(Parking_Cost_Attributes, axis = 1)

parking_cost_df = pd.read_csv(Parking_Cost_Source, sep = " ", index_col = "PARCELID")
parking_cost_df =  parking_cost_df[Parking_Cost_Attributes]

update_parcel_df = update_parcel_df.join(parking_cost_df)
update_parcel_df.fillna(0, inplace = True)


if SET_BELLEVUE_PARKING_COST_HALF == True:
    update_parcel_df =  update_parcel_df.join(taz_subarea_df['Jurisdiction'], on = 'TAZ_P')
    update_parcel_df.loc[update_parcel_df['Jurisdiction'] == 'BELLEVUE', 'PPRICDYP']  = update_parcel_df['PPRICDYP'] * 0.5
    update_parcel_df.loc[update_parcel_df['Jurisdiction'] == 'BELLEVUE', 'PPRICHRP']  = update_parcel_df['PPRICHRP'] * 0.5    
    update_parcel_df = update_parcel_df.drop(['Jurisdiction'], axis = 1)  


print 'Exporting updated parcel file...'
update_parcel_df.to_csv(os.path.join(Project_Folder, Updated_Parcel_File_Name), sep = " ")

utility.backupScripts(__file__, os.path.join(Project_Folder, os.path.basename(__file__)))

print 'Done'


