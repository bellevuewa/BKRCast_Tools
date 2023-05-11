import pandas as pd
import h5py
import os
import utility

'''
This program is used to pass number of households by parcel from synthetic population to parcel file. After the program,
the households in parcel file is consistent with synthetic population file.

3/9/2022
upgraded to python 3.7
'''

Hh_and_person_file = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\Complan\complan2044\WFH\Alt3-30%WFH\final_combined_complan_alt3_hh_and_persons_forWFH_30%_outside_bel_consistent_with_NA.h5"
parcel_folder = r'Z:\Modeling Group\BKRCast\LandUse\Complan\Complan2044\LU_alt3'
input_parcel_file = 'complan_alt3_parcels_urbansim.txt'
output_parcel_file = 'updated_alt3_parcels_urbansim.txt'

print('Loading hh_and_persons.h5...')
hdf_file = h5py.File(Hh_and_person_file, "r")
hh_df = utility.h5_to_df(hdf_file, 'Household')
hh_df.set_index('hhparcel', inplace = True)

print('Updating number of households...')
hhs = hh_df.groupby('hhparcel')[['hhexpfac', 'hhsize']].sum()
parcel_df = pd.read_csv(os.path.join(parcel_folder, input_parcel_file), sep = ' ')
parcel_df.set_index('PARCELID', inplace = True)
parcel_df = parcel_df.join(hhs, how = 'left')

parcel_df['HH_P']  = parcel_df['hhexpfac']
parcel_df.fillna(0, inplace = True)
parcel_df.drop(['hhexpfac', 'hhsize'], axis = 1, inplace = True)


print('Exporting future parcel file...')
parcel_df.to_csv(os.path.join(parcel_folder, output_parcel_file), sep = ' ')

utility.backupScripts(__file__, os.path.join(parcel_folder, os.path.basename(__file__)))

print('Done.')
