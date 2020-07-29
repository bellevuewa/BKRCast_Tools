import pandas as pd
import h5py
import os
import utility

'''
   This program is used to append a new incremental growth synthetic population to the base population. The household no in the incremental growth syn pop have to be renumbered.
'''


### configuration
working_folder = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2020Concurrency'
base_syn_pop_file = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2019\2019_hh_and_persons.h5'
growth_syn_pop_file = '2020concurrencygrowth_hh_and_persons.h5'

# output
final_output_pop_file = '2020_concurrency_hh_and_persons.h5'

### end of configuration

print 'Loading hh and person file...'
base_hdf_file = h5py.File(base_syn_pop_file, "r")
base_person_df = utility.h5_to_df(base_hdf_file, 'Person')
base_hh_df = utility.h5_to_df(base_hdf_file, 'Household')

growth_hdf_file = h5py.File(os.path.join(working_folder, growth_syn_pop_file), 'r')
growth_person_df = utility.h5_to_df(growth_hdf_file, 'Person')
growth_hh_df = utility.h5_to_df(growth_hdf_file, 'Household')

#find the max hhid from base_hh_df
max_base_hhid = max(base_hh_df['hhno'])
next_hhno = max_base_hhid
for hhno in growth_hh_df['hhno']:
    next_hhno = next_hhno + 1
    growth_hh_df.loc[growth_hh_df['hhno'] == hhno, 'hhno'] = next_hhno
    growth_person_df.loc[growth_person_df['hhno'] == hhno, 'hhno'] = next_hhno

    print 'household ', hhno, ' in the growth dataset is now changed to ', next_hhno

final_hhs_df = base_hh_df.append(growth_hh_df)
final_persons_df = base_person_df.append(growth_person_df)

output_h5_file = h5py.File(os.path.join(working_folder, final_output_pop_file), 'w')
utility.df_to_h5(final_hhs_df, output_h5_file, 'Household')
utility.df_to_h5(final_persons_df, output_h5_file, 'Person')
output_h5_file.close()
print 'H5 exported.'

print 'Done.'



