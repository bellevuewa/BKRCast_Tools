import os
import pandas as pd
import h5py
import sys
import numpy as np
sys.path.append(os.getcwd())
import utility

############# confiuration ###############
## input files
hh_person_folder = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2025baseyear\30%WFH"
hh_person_file = '2025_baseyear_hh_and_persons_forWFH_25%.h5'

## output files
error_hhs_file = 'hhs.f'
error_persons_file = 'persons.f'
##########################################


print('Loading hh and person file...')
hdf_file = h5py.File(os.path.join(hh_person_folder, hh_person_file), "r")

person_df = utility.h5_to_df(hdf_file, 'Person')
hh_df = utility.h5_to_df(hdf_file, 'Household')

error_hhs_df = hh_df[hh_df.isnull().any(axis = 1)]
error_persons_df = person_df[person_df.isnull().any(axis = 1)]

error_hhs_df.to_csv(os.path.join(hh_person_folder, error_hhs_file), sep = ',')
error_persons_df.to_csv(os.path.join(hh_person_folder, error_persons_file), sep = ',')
duplicated_hhs_df = hh_df[hh_df.duplicated('hhno', keep = False)]
if duplicated_hhs_df.shape[0] == 0:
    print('no duplicated household id (hhno) is found.')
else:
    print('found duplicated household ids (hhno). check out duplicated_hhs.csv for details.')
    duplicated_hhs_df.to_csv(os.path.join(hh_person_folder, 'duplicated_hhs.csv'), index = False)

neg_hhparcels_df = hh_df.loc[hh_df['hhparcel'] <= 0]
if neg_hhparcels_df.shape[0] > 0:
    print('Some parcel IDs are negatives.')
    neg_hhparcels_df.to_csv(os.path.join(hh_person_folder, 'negative_parcel_id.csv', index = False))
else:
    print('No negative parcel ID is found.')
    
neg_hhtaz_df = hh_df.loc[hh_df['hhtaz'] <= 0]
if neg_hhtaz_df.shape[0] > 0:
    print('Some hhtaz are negatives.')
    neg_hhtaz_df.to_csv(os.path.join(hh_person_folder, 'negative_hhtaz.csv', index = False))
else:
    print('No negative hhtaz is found.')

if hh_df['hhsize'].sum() == person_df.shape[0]:
    print('hhsize is consistent with the number of persons.')
else:
    print('hhsize is NOT consistent with the number of persons. Check out hhsize_check.csv for details.')
    hhsize_check_df = hh_df[['hhno', 'hhsize']].copy()
    person_by_hhno = person_df[['hhno', 'psexpfac']].groupby('hhno').sum()
    person_by_hhno.rename(columns = {'psexpfac': 'num_persons'}, inplace = True)
    hhsize_check_df = hhsize_check_df.merge(person_by_hhno, on = 'hhno', how = 'left')
    hhsize_check_df.loc[hhsize_check_df['hhsize'] != hhsize_check_df['num_persons']].to_csv(os.path.join(hh_person_folder, 'hhsize_check.csv'), index = False)
print('Done')