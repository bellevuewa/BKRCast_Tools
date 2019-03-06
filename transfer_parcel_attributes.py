import pandas as pd
import os

Parcel_file_from = r'Z:\Modeling Group\BKRCast\2014ESD\parcels_urbansim.txt'
Parcel_file_to = r'Z:\Modeling Group\BKRCast\2035Parcel_Sqft_based\parcels_urbansim.txt'
Output_parcel_file_name = r'Z:\Modeling Group\BKRCast\2035Parcel_Sqft_based\BKRCastFuture_adjusted_updated_attributes_parcels_urbansim.txt'
Attr_list = ['PARKDY_P', 'PARKHR_P', 'PPRICDYP', 'PPRICHRP']

parcel_from_df = pd.DataFrame.from_csv(Parcel_file_from, sep = ' ', index_col = 'PARCELID')
parcel_to_df = pd.DataFrame.from_csv(Parcel_file_to, sep = ' ', index_col = 'PARCELID')

parcel_from_df = parcel_from_df[Attr_list]            
parcel_to_df.drop(Attr_list, axis = 1, inplace = True)

Output_parcel = parcel_to_df.join(parcel_from_df)

Output_parcel.to_csv(Output_parcel_file_name, sep = ' ')
print 'Done.'

