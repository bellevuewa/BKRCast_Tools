import os
import utility
import pandas as pd

parcel_lookup_File_Name = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'
parcel_data_file_name = r'Z:\Modeling Group\BKRCast\LandUse\2020Baseyear\2020_KC_Jobs.csv'

# Juridisction = Rest of KC, External, BELLEVUE, BellevueFringe, KIRKLAND, REDMOND, RedmondFridge, KirklandFringe, 
# only look into this subset of parcel_lookup_df if Jurisdiction is not set to None. Otherwise use the whole set of parcel_lookup_df,
Jurisdiction = 'BELLEVUE'

parcel_lookup_df = pd.read_csv(parcel_lookup_File_Name, sep  = ',', low_memory = False)
parcels_df = pd.read_csv(parcel_data_file_name, sep = ',')
parcels_df = parcels_df.groupby('PSRC_ID').sum()

# export parcels that are given in the parcel_data_file_name but are not included in parcel_lookup_File_Name
not_in_2014_PSRC_parcels = parcels_df.loc[~parcels_df.index.isin(parcel_lookup_df['PSRC_ID'])]
if not_in_2014_PSRC_parcels.empty == False:
    not_in_2014_PSRC_parcels.to_csv(os.path.join(os.path.dirname(parcel_data_file_name), 'parcels_not_in_2014PSRC_parcels.csv'))
else:
    print 'All parcels given are within parcel lookup file.'

if Jurisdiction != None:
    selected_parcels_lookup_df = parcel_lookup_df.loc[parcel_lookup_df['Jurisdiction'] == Jurisdiction]
else:
    selected_parcels_lookup_df = parcel_lookup_df

# export parcels that are in parcel_lookup_File_Name but not in the parcel_data_file_name.
not_in_given_parcel_dataset = selected_parcels_lookup_df.loc[~selected_parcels_lookup_df['PSRC_ID'].isin(parcels_df.index)]
if not_in_given_parcel_dataset.empty == False:
    not_in_given_parcel_dataset.to_csv(os.path.join(os.path.dirname(parcel_data_file_name), '2014PSRC_parcels_not_in_given_parcel_dataset.csv'))
else:
    print 'No 2014 PSRC parcels are missing in the given parcel dataset.'

print 'done'
