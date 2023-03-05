import pandas as pd
import h5py
import os
import utility


working_folder = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\Complan\complan2044\WFH\Alt3-30%WFH'
bel_syn_pop_file = r"final_combined_complan_alt3_hh_and_persons_forWFH_30%.h5"
non_bel_syn_pop_file = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\Complan\complan2044\WFH\NA-30%WFH\final_combined_COB_renumbered_2044complan_na_hh_and_persons_forWFH_30%.h5"
parcel_filename = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'
non_bel_converted_worker_file = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\Complan\complan2044\WFH\NA-30%WFH\converted_non_workers.csv"
bel_converted_worker_file = 'converted_non_workers.csv'
# output
final_output_pop_file = 'final_combined_complan_alt3_hh_and_persons_forWFH_30%_outside_bel_consistent_with_NA.h5'
converted_workers_file = 'final_converted_non_workers.csv'

subarea = ['BELLEVUE']

print('Loading hh and person file...')
bel_hdf_file = h5py.File(os.path.join(working_folder, bel_syn_pop_file), "r")
bel_person_df = utility.h5_to_df(bel_hdf_file, 'Person')
bel_hh_df = utility.h5_to_df(bel_hdf_file, 'Household')

non_bel_hdf_file = h5py.File(non_bel_syn_pop_file, 'r')
non_bel_person_df = utility.h5_to_df(non_bel_hdf_file, 'Person')
non_bel_hh_df = utility.h5_to_df(non_bel_hdf_file, 'Household')

parcel_df = pd.read_csv(parcel_filename, low_memory=False) 

## remove hhs and persons in Bellevue
non_bel_hh_df = non_bel_hh_df.merge(parcel_df[['PSRC_ID', 'Jurisdiction']], how = 'left', left_on = 'hhparcel', right_on = 'PSRC_ID')
non_bel_hh_df = non_bel_hh_df.loc[~non_bel_hh_df['Jurisdiction'].isin(subarea)].copy()
non_bel_person_df = non_bel_person_df.loc[non_bel_person_df['hhno'].isin(non_bel_hh_df['hhno'])].copy()

bel_hh_df = bel_hh_df.merge(parcel_df[['PSRC_ID', 'Jurisdiction']], how = 'left', left_on = 'hhparcel', right_on = 'PSRC_ID')
bel_hh_df = bel_hh_df.loc[bel_hh_df['Jurisdiction'].isin(subarea)].copy()
bel_person_df = bel_person_df.loc[bel_person_df['hhno'].isin(bel_hh_df['hhno'])].copy()


final_hhs_df = non_bel_hh_df.append(bel_hh_df)
final_hhs_df = final_hhs_df.drop(['PSRC_ID', 'Jurisdiction'], axis = 1)
final_persons_df = non_bel_person_df.append(bel_person_df)

output_h5_file = h5py.File(os.path.join(working_folder, final_output_pop_file), 'w')
utility.df_to_h5(final_hhs_df, output_h5_file, 'Household')
utility.df_to_h5(final_persons_df, output_h5_file, 'Person')
output_h5_file.close()


bel_non_workers_df = pd.read_csv(os.path.join(working_folder,  bel_converted_worker_file))
non_bel_non_workers_df = pd.read_csv(non_bel_converted_worker_file)

bel_non_workers_df  = bel_non_workers_df.merge(parcel_df[['PSRC_ID', 'Jurisdiction']], how = 'left', left_on = 'hhparcel', right_on = 'PSRC_ID')
bel_non_workers_df  = bel_non_workers_df.loc[bel_non_workers_df['Jurisdiction'].isin(subarea)].copy()

non_bel_non_workers_df  = non_bel_non_workers_df.merge(parcel_df[['PSRC_ID', 'Jurisdiction']], how = 'left', left_on = 'hhparcel', right_on = 'PSRC_ID')
non_bel_non_workers_df  = non_bel_non_workers_df.loc[~non_bel_non_workers_df['Jurisdiction'].isin(subarea)].copy()

final_non_workers_df = bel_non_workers_df.append(non_bel_non_workers_df)
final_non_workers_df = final_non_workers_df.drop(['PSRC_ID', 'Jurisdiction'], axis = 1)
final_non_workers_df.to_csv(os.path.join(working_folder, converted_workers_file), index = False)
utility.backupScripts(__file__, os.path.join(working_folder, os.path.basename(__file__)))

print('H5 exported.')

