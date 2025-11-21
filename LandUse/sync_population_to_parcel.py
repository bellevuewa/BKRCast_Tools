import pandas as pd
import h5py
import os,sys
sys.path.append(os.getcwd())
import utility

'''
This program is used to pass number of households by parcel from synthetic population to parcel file. After the program,
the households in parcel file is consistent with synthetic population file.

3/9/2022
upgraded to python 3.7
'''

Hh_and_person_file = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2030_DevReview_156thCorridor_Study\WFH_30%\2030_DevReview_156thCorridorStudy_hh_and_persons_forWFH_30%.h5"
parcel_folder = r"Z:\Modeling Group\BKRCast\LandUse\2030_DevReview_156th_corridor_Study"
input_parcel_file = '2030_devreview_156thCorridorStudy_parcels_urbansim.txt'
output_parcel_file = 'updated_2030_devreview_156thCorridorStudy_parcels_urbansim.txt.txt'

print('Loading hh_and_persons.h5...')
hdf_file = h5py.File(Hh_and_person_file, "r")
hh_df = utility.h5_to_df(hdf_file, 'Household')

print('Updating number of households...')
hhs = hh_df.groupby('hhparcel')[['hhexpfac', 'hhsize']].sum().reset_index()
parcel_df = pd.read_csv(os.path.join(parcel_folder, input_parcel_file), sep = ' ')
parcel_df = parcel_df.merge(hhs, how = 'left', left_on = 'PARCELID', right_on = 'hhparcel')

parcel_df['HH_P']  = 0
parcel_df['HH_P'] = parcel_df['hhexpfac']
parcel_df.fillna(0, inplace = True)
parcel_df.drop(['hhexpfac', 'hhsize', 'hhparcel'], axis = 1, inplace = True)
parcel_df['HH_P'] = parcel_df['HH_P'].round(0).astype(int)


print('Exporting future parcel file...')
parcel_df.to_csv(os.path.join(parcel_folder, output_parcel_file), sep = ' ', index = False)

utility.backupScripts(__file__, os.path.join(parcel_folder, os.path.basename(__file__)))

print('Done.')
