import os
import pandas as pd
import h5py
import sys
import numpy as np
sys.path.append(os.getcwd())
import utility

############# confiuration ###############
## input files
hh_person_folder = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\TFP\2033_horizon_year'
hh_person_file = '2033TFP_hh_and_persons.h5'

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

print('Done')