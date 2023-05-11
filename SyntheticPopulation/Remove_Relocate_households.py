import os
import pandas as pd
import h5py
import sys
import numpy as np
sys.path.append(os.getcwd())
import utility

### configurations
working_folder = r''
action_file_name = r''
syn_pop_file_name = r''

new_syn_pop_file_name = r''
### end of configurations


### to remove a list of households and their people from synthetic population.
### 7/5/2019

## allow to remove all hhs in a taz
## 1/12/2023

# swith can only be HHNO or PARCEL or 'TAZ'
# SWITCH = 'HHNO' 
SWITCH = 'TAZ' 
# comment hhs_list if switch is PARCEL. PARCLE switch is not ready yet.
hhs_list = [443303]
# comment parcels_list if switch is HHNO.
parcels_list = [717277]

taz_list = [344]
### inputs
hh_person_folder = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\Complan\complan2044\WFH\Alt1-30%WFH'
hh_person_file = "final_2044complan_alt1_hh_and_persons_forWFH_30%.h5"
out_h5_file = h5py.File(os.path.join(hh_person_folder, 'alt1_test_hh_and_persons.h5'), 'w')




print('Loading hh and person file...')
hdf_file = h5py.File(os.path.join(hh_person_folder, hh_person_file), "r")

person_df = utility.h5_to_df(hdf_file, 'Person')
hh_df = utility.h5_to_df(hdf_file, 'Household')

if SWITCH == 'HHNO':
    hh_df = hh_df[~hh_df['hhno'].isin(hhs_list)]
    person_df = person_df[~person_df['hhno'].isin(hhs_list)]
elif SWITCH == 'PARCEL':
    hh_df = hh_df[~hh_df['hhparcel'].isin(parcels_list)]
elif SWITCH == 'TAZ':
    hh_df = hh_df[~hh_df['hhtaz'].isin(taz_list)]
    person_df = person_df[person_df['hhno'].isin(hh_df['hhno'])]

print('Export households and persons...')
utility.df_to_h5(hh_df, out_h5_file, 'Household')
utility.df_to_h5(person_df, out_h5_file, 'Person')
out_h5_file.close()


print('Done')
