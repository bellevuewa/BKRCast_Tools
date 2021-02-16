import pandas as pd
import h5py
import numpy as np
import random 
import os

# this program is used to replace a subset of parcel data

# we no longer use it.

New_parcel_data_file_name = r"Z:\Modeling Group\BKRCast\LandUse\2020baseyear\2020_KC_Jobs.csv"
Original_parcel_file_name = r"Z:\Modeling Group\BKRCast\LandUse\2020baseyear\2020_parcels_bkr_from_PSRC_with_half_Bellevue_parkingcost.txt"
Updated_parcel_file_name =  r"Z:\Modeling Group\BKRCast\LandUse\2020baseyear\parcels_urbansim.txt"
Old_Subset_parcel_file_name = r"Z:\Modeling Group\BKRCast\LandUse\2020baseyear\Old_parcels_subset.txt"


print 'Loading...'
New_parcel_data_df = pd.read_csv(New_parcel_data_file_name, sep = ',', index_col = 'PSRC_ID')
Orig_parcel_df = pd.read_csv(Original_parcel_file_name, sep = ' ', index_col = 'PARCELID', low_memory = False)

fields = New_parcel_data_df.columns
New_parcel_data_df.rename(columns = lambda col: col + '_new', inplace = True)

selected_parcels = Orig_parcel_df.loc[New_parcel_data_df.index]
selected_parcels.to_csv(Old_Subset_parcel_file_name)

selected_parcels = selected_parcels.join(New_parcel_data_df)
for field in New_parcel_data_df.columns:
    curField = field[:-4]
    selected_parcels[curField] = selected_parcels[field]
selected_parcels.drop(New_parcel_data_df.columns, axis = 1, inplace = True)

Updated_parcel_df = Orig_parcel_df.drop(selected_parcels.index)
Updated_parcel_df = Updated_parcel_df.append(selected_parcels)
Updated_parcel_df.to_csv(Updated_parcel_file_name, sep = ' ')

print 'Done.'
