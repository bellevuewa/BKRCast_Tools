import os
import pandas as pd
import h5py
import sys
import numpy as np
sys.path.append(os.getcwd())
import utility

############# confiuration ###############
## input files
hh_person_folder = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2019-test\new_allocation_approach'
hh_person_file = '2019_hh_and_persons.h5'

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


print('Done')