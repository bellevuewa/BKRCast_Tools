import os
import pandas as pd
import h5py
import sys
import numpy as np
sys.path.append(os.getcwd())
import utility

### to remove a list of households and their people from synthetic population.
### 7/5/2019

# swith can only be HHNO or PARCEL
SWITCH = 'HHNO' 
# comment hhs_list if switch is PARCEL. PARCLE switch is not ready yet.
hhs_list = [443303]
# comment parcels_list if switch is HHNO.
#parcels_list = [717277]

### inputs
hh_person_folder = r'D:\PopulationSim\PSRCrun0423\output'
hh_person_file = "popsim_hh_and_persons.h5"
out_h5_file = h5py.File(os.path.join(hh_person_folder, 'updated_hh_and_persons.h5'), 'w')




print 'Loading hh and person file...'
hdf_file = h5py.File(os.path.join(hh_person_folder, hh_person_file), "r")

person_df = utility.h5_to_df(hdf_file, 'Person')
hh_df = utility.h5_to_df(hdf_file, 'Household')

if SWITCH == 'HHNO':
    hh_df = hh_df[~hh_df['hhno'].isin(hhs_list)]
    person_df = person_df[~person_df['hhno'].isin(hhs_list)]
elif SWITCH == 'PARCEL':
    hh_df = hh_df[~hh_df['hhparcel'].isin(parcels_list)]

print 'Export households and persons...'
utility.df_to_h5(hh_df, out_h5_file, 'Household')
utility.df_to_h5(person_df, out_h5_file, 'Person')
out_h5_file.close()


print 'Done'
