import pandas as pd
import h5py
import os
import utility

### configuration
working_folder = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\Complan\complan2044\NA\new_popsim_approach'
input_syn_pop_file = r'final_combined_complan_NA_hh_and_persons.h5'
parcel_filename = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'
# output
final_output_pop_file = 'final_combined_COB_renumbered_2044complan_na_hh_and_persons.h5'

subarea = ['BELLEVUE']
#subarea = ['BELLEVUE', 'KIRKLAND', 'REDMOND', 'BellevueFringe', 'KirklandFringe', 'RedmondFringe']


print('Loading hh and person file...')
input_hdf_file = h5py.File(os.path.join(working_folder, input_syn_pop_file), "r")
input_person_df = utility.h5_to_df(input_hdf_file, 'Person')
input_hh_df = utility.h5_to_df(input_hdf_file, 'Household')

parcel_df = pd.read_csv(parcel_filename, low_memory=False) 


## remove hhs and persons in Bellevue
input_hh_df = input_hh_df.merge(parcel_df[['PSRC_ID', 'Jurisdiction']], how = 'left', left_on = 'hhparcel', right_on = 'PSRC_ID')
input_hh_df_outside_subarea = input_hh_df.loc[~input_hh_df['Jurisdiction'].isin(subarea)].copy()
input_person_df_outside_subarea = input_person_df.loc[input_person_df['hhno'].isin(input_hh_df_outside_subarea['hhno'])].copy()

hh_df_inside_subarea = input_hh_df.loc[input_hh_df['Jurisdiction'].isin(subarea)].copy()
person_df_inside_subarea = input_person_df.loc[input_person_df['hhno'].isin(hh_df_inside_subarea['hhno'])].copy()

#find the max hhid from input_hh_df
max_hhid = max(input_hh_df_outside_subarea['hhno'])
next_hhno = max_hhid
for hhno in hh_df_inside_subarea['hhno']:
    next_hhno = next_hhno + 1
    hh_df_inside_subarea.loc[hh_df_inside_subarea['hhno'] == hhno, 'hhno'] = next_hhno
    person_df_inside_subarea.loc[person_df_inside_subarea['hhno'] == hhno, 'hhno'] = next_hhno

final_hhs_df = input_hh_df_outside_subarea.append(hh_df_inside_subarea)
final_hhs_df = final_hhs_df.drop(['PSRC_ID', 'Jurisdiction'], axis = 1)
final_persons_df = input_person_df_outside_subarea.append(person_df_inside_subarea)

output_h5_file = h5py.File(os.path.join(working_folder, final_output_pop_file), 'w')
utility.df_to_h5(final_hhs_df, output_h5_file, 'Household')
utility.df_to_h5(final_persons_df, output_h5_file, 'Person')
output_h5_file.close()

utility.backupScripts(__file__, os.path.join(working_folder, os.path.basename(__file__)))

print('H5 exported.')





