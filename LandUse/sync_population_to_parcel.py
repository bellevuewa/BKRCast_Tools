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

Hh_and_person_file = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\KirklandSupport\Kirkland2044Complan\target2044\final_COK_renumbered_2044_kirk_complan_target_hh_and_persons.h5"
parcel_folder = r"Z:\Modeling Group\BKRCast\KirklandSupport\Kirkland2044Complan\preferred_2044"
input_parcel_file = '2044_kirkcomplan_target_parcels_urbansim_jobs_scaled_from_baseline.txt'
output_parcel_file = '2044_kirkcomplan_target_parcels_urbansim_jobs_scaled_from_baseline_sync_with_popsim.txt'

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
