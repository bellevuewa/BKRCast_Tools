'''
Convert PSRC TAZ to BKRCast TAZ in the parcel file.
When we receive parcel file from PSRC, TAZ_P is always PSRC's TAZ. We use this tool to convert the PSRC taz to BKRCast taz. 
and assign BKRCastTAZ to TAZ_P. 

2/3/2022
upgrade to python 3.7
'''

import os, shutil
import pandas as pd
import h5py
import numpy as np
import csv

# inputs
wd = r"Z:\Modeling Group\BKRCast\SoundCast\2050_Inputs"
parcel_file_name = '2050_SC_parcels.txt'

# correspondence file
parcel_bkr_taz_file = r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel2014_BKRCastTAZ.csv"

#read parcel file
parcels_psrc_df = pd.read_csv(os.path.join(wd, parcel_file_name), sep = " ")
parcels_fields = list(parcels_psrc_df.columns)
parcels_psrc_df.columns = parcels_psrc_df.columns.str.upper()

#read parcel to bkr taz correspondence
parcel_bkr_taz = pd.read_csv(parcel_bkr_taz_file, sep = ',')

#merge bkr taz to parcel file
parcels_bkr = pd.merge(parcels_psrc_df, parcel_bkr_taz, how = 'inner', left_on = 'PARCELID', right_on = 'PARCELID')
missing_data_df = parcels_psrc_df.loc[~parcels_psrc_df['PARCELID'].isin(parcel_bkr_taz['PARCELID'])]

parcels_bkr['PSRCTAZ'] = parcels_bkr['TAZ_P']
parcels_bkr['TAZ_P'] = parcels_bkr['BKRCastTAZ'].astype(np.int32)
parcels_bkr.drop(['BKRCastTAZ'], axis = 1, inplace = True)
parcels_bkr = parcels_bkr.sort_values(by = ['PARCELID'], ascending=[True])

if (missing_data_df.shape[0] > 0):
    missing_data_df.to_csv(os.path.join(wd, 'parcels_no_BKRCastTAZ.csv'), sep = ',')
    print('Error. There are ' + str(missing_data_df.shape[0]) + 'parcels that do not have BKRCastTAZ assigned.')
    print('Output file excluded these parcels.')

#write out the updated parcel file
parcel_file_out = parcel_file_name.split(".")[0]+ "_bkr.txt"
parcel_file_out_path = os.path.join(wd, parcel_file_out)
parcels_bkr.to_csv(parcel_file_out_path, sep = ' ', index = False)

print('Done.')


