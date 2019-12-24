import pandana as pdna
import os, sys
sys.path.append(os.getcwd())
import pandas as pd
import numpy as np
import re
from pyproj import Proj, transform
import copy
from shutil import copyfile

# copy parking cost data ['PARKDY_P', 'PARKHR_P', 'PPRICDYP', 'PPRICHRP'] from the source to original esd parcel file.

Project_Folder =  r"Z:\Modeling Group\BKRCast\2021concurrencyPretest"
Original_ESD_Parcel_File_Name = r"parcels_urbansim_2021concurrency_pretest.txt"
TAZ_Subarea_File_Name = r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\TAZ_subarea.csv"
Parking_Cost_Source =  r"Z:\Modeling Group\BKRCast\2020concurrencyPretest\parcels_urbansim.txt"
Updated_Parcel_File_Name = r'updated_parcels_urbansim_2021concurrency_pretest.txt'

Parking_Cost_Attributes = ['PARKDY_P', 'PARKHR_P', 'PPRICDYP', 'PPRICHRP']

print "Loading input files ..."
original_parcel_df = pd.DataFrame.from_csv(os.path.join(Project_Folder, Original_ESD_Parcel_File_Name), sep = " ", index_col = "PARCELID")
taz_subarea_df = pd.DataFrame.from_csv(os.path.join(Project_Folder,TAZ_Subarea_File_Name), sep = ",", index_col = "TAZNUM")

# drop four parking attributes 
update_parcel_df = original_parcel_df.drop(Parking_Cost_Attributes, axis = 1)

parking_cost_df = pd.DataFrame.from_csv(Parking_Cost_Source, sep = " ", index_col = "PARCELID")
parking_cost_df =  parking_cost_df[Parking_Cost_Attributes]

update_parcel_df = update_parcel_df.join(parking_cost_df)


update_parcel_df =  update_parcel_df.join(taz_subarea_df['Jurisdiction'], on = 'TAZ_P')
update_parcel_df.loc[update_parcel_df['Jurisdiction'] == 'BELLEVUE', 'PPRICDYP']  = update_parcel_df['PPRICDYP'] * 0.5
update_parcel_df.loc[update_parcel_df['Jurisdiction'] == 'BELLEVUE', 'PPRICHRP']  = update_parcel_df['PPRICHRP'] * 0.5    
update_parcel_df = update_parcel_df.drop(['Jurisdiction'], axis = 1)  


print 'Exporting updated parcel file...'
update_parcel_df.to_csv(os.path.join(Project_Folder, Updated_Parcel_File_Name), sep = " ")
print 'Done'


