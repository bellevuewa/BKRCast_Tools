import pandas as pd
import h5py
import numpy as np
import random 
import os

# this program is used to replace a subset of parcel data

New_parcel_data_file_name = r"Z:\Modeling Group\BKRCast\2035Parcel_Sqft_based\COB estimate\2035_parcels_COB_Jobs_Estimate.csv"
Original_parcel_file_name = r"Z:\Modeling Group\BKRCast\2035Parcel_Sqft_based\BKRCastFuture_parcels_urbansim.txt"
Updated_parcel_file_name =  r"Z:\Modeling Group\BKRCast\2035Parcel_Sqft_based\COB estimate\parcels_urbansim.txt"
Old_Subset_parcel_file_name = r"Z:\Modeling Group\BKRCast\2035Parcel_Sqft_based\COB estimate\Old_parcels_subset.txt"


print 'Loading...'
New_parcel_data_df = pd.DataFrame.from_csv(New_parcel_data_file_name, sep = ',', index_col = 'PARCELID')
Orig_parcel_df = pd.DataFrame.from_csv(Original_parcel_file_name, sep = ' ', index_col = 'PARCELID')

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
