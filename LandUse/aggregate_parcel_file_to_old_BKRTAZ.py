import pandas as pd

parcel_file_name = r"Z:\Modeling Group\BKRCast\LandUse\2020baseyear-BKR\base_2020_0706.csv"
lookup_table_name = r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\BKRCast_Parcels_BKRTAZ.csv"

parcel_df = pd.read_csv(parcel_file_name)
lookup_df = pd.read_csv(lookup_table_name)

merged_parcel_df = pd.merge(parcel_df, lookup_df[['PSRC_ID', 'TAZNUM']], on = 'PSRC_ID', how = 'left')
sum_by_old_TAZ = merged_parcel_df.groupby('TAZNUM').sum()
sum_by_old_TAZ.to_csv(r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\0_11Model Platforms\BKR Model CURRENT\MP0-BASE\MP0R22\Land Use\2020_sqft_by_old_TAZ_from_parcel_data.csv')
print('Done')
