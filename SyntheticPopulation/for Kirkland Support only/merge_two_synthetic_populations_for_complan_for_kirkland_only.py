import pandas as pd
import h5py
import os
import utility

'''
   This program is used to append a new incremental growth synthetic population to the base population. The household no in the incremental growth syn pop have to be renumbered.
'''


### configuration
working_folder = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\Complan\complan2044\Alt3'
base_syn_pop_file = r'final_combined_COB_renumbered_2044complan_na_hh_and_persons.h5'
growth_syn_pop_file = 'complan_alt3_hh_and_persons.h5'
parcel_filename = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'
# output
final_output_pop_file = 'final_combined_complan_alt3_hh_and_persons.h5'

### end of configuration

subarea = ['KIRKLAND']
#subarea = ['BELLEVUE', 'KIRKLAND', 'REDMOND', 'BellevueFringe', 'KirklandFringe', 'RedmondFringe']


print('Loading hh and person file...')
base_hdf_file = h5py.File(os.path.join(working_folder, base_syn_pop_file), "r")
base_person_df = utility.h5_to_df(base_hdf_file, 'Person')
base_hh_df = utility.h5_to_df(base_hdf_file, 'Household')

growth_hdf_file = h5py.File(os.path.join(working_folder, growth_syn_pop_file), 'r')
growth_person_df = utility.h5_to_df(growth_hdf_file, 'Person')
growth_hh_df = utility.h5_to_df(growth_hdf_file, 'Household')

parcel_df = pd.read_csv(parcel_filename, low_memory=False) 


## remove hhs and persons in Bellevue
base_hh_df = base_hh_df.merge(parcel_df[['PSRC_ID', 'Jurisdiction']], how = 'left', left_on = 'hhparcel', right_on = 'PSRC_ID')
base_hh_df_no_Bel = base_hh_df.loc[~base_hh_df['Jurisdiction'].isin(subarea)].copy()
base_person_df_no_Bel = base_person_df.loc[base_person_df['hhno'].isin(base_hh_df_no_Bel['hhno'])].copy()

growth_hh_df = growth_hh_df.merge(parcel_df[['PSRC_ID', 'Jurisdiction']], how = 'left', left_on = 'hhparcel', right_on = 'PSRC_ID')
growth_hh_df_Bel = growth_hh_df.loc[growth_hh_df['Jurisdiction'].isin(subarea)].copy()
growth_person_df_Bel = growth_person_df.loc[growth_person_df['hhno'].isin(growth_hh_df_Bel['hhno'])].copy()

#find the max hhid from base_hh_df
max_base_hhid = max(base_hh_df_no_Bel['hhno'])
next_hhno = max_base_hhid
for hhno in growth_hh_df_Bel['hhno']:
    next_hhno = next_hhno + 1
    growth_hh_df_Bel.loc[growth_hh_df_Bel['hhno'] == hhno, 'hhno'] = next_hhno
    growth_person_df_Bel.loc[growth_person_df_Bel['hhno'] == hhno, 'hhno'] = next_hhno

final_hhs_df = base_hh_df_no_Bel.append(growth_hh_df_Bel)
final_hhs_df = final_hhs_df.drop(['PSRC_ID', 'Jurisdiction'], axis = 1)
final_persons_df = base_person_df_no_Bel.append(growth_person_df_Bel)

output_h5_file = h5py.File(os.path.join(working_folder, final_output_pop_file), 'w')
utility.df_to_h5(final_hhs_df, output_h5_file, 'Household')
utility.df_to_h5(final_persons_df, output_h5_file, 'Person')
output_h5_file.close()

utility.backupScripts(__file__, os.path.join(working_folder, os.path.basename(__file__)))

print('H5 exported.')



