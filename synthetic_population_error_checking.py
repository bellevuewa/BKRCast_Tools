import os
import pandas as pd
import h5py
import sys
import numpy as np
sys.path.append(os.getcwd())
import utility

hh_person_folder = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\2035SyntheticPopulation_S_DT_Access_Study'
hh_person_file = '2035_popsim_hh_and_persons.h5'

print 'Loading hh and person file...'
hdf_file = h5py.File(os.path.join(hh_person_folder, hh_person_file), "r")

person_df = utility.h5_to_df(hdf_file, 'Person')
hh_df = utility.h5_to_df(hdf_file, 'Household')

for col in hh_df.columns:
    hh_df.loc[hh_df[col].isnull()]

for col in person_df.columns:
    person_df.loc[person_df[col].isnull()]

print 'Done'