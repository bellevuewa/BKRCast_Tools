import pandas as pd
import h5py
import os
import utility

'''
This program is used to pass number of households by parcel from synthetic population to parcel file. After the program,
the households in parcel file is consistent with synthetic population file.
'''

Hh_and_person_file = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2020\2020_hh_and_persons.h5'
parcel_folder = r'Z:\Modeling Group\BKRCast\LandUse\2020baseyear'
input_parcel_file = 'parcels_urbansim.txt'
output_parcel_file = 'parcels_urbansim.txt'

print 'Loading hh_and_persons.h5...'
hdf_file = h5py.File(Hh_and_person_file, "r")
hh_df = utility.h5_to_df(hdf_file, 'Household')
hh_df.set_index('hhparcel', inplace = True)

print 'Updating number of households...'
hhs = hh_df.groupby('hhparcel')['hhexpfac', 'hhsize'].sum()
parcel_df = pd.read_csv(os.path.join(parcel_folder, input_parcel_file), sep = ' ')
parcel_df.set_index('PARCELID', inplace = True)
parcel_df = parcel_df.join(hhs, how = 'left')

parcel_df['HH_P']  = parcel_df['hhexpfac']
parcel_df.fillna(0, inplace = True)
parcel_df.drop(['hhexpfac', 'hhsize'], axis = 1, inplace = True)


print 'Exporting future parcel file...'
parcel_df.to_csv(os.path.join(parcel_folder, output_parcel_file), sep = ' ')


print 'Done.'
