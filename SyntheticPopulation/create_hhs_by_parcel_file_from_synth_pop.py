import pandas as pd
import h5py
import os
import utility


### Configurations ######
## input files

working_folder = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2019\new allocation approach'
synpop_h5_file = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2019\2019_hh_and_persons.h5"
parcel_filename = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'

## Output files
hhs_by_parcel_filename = r'2019_hhs_by_parcels.csv'

### end of configuration

print('Loading synthetic populations...')
hdf_file = h5py.File(synpop_h5_file, "r")
hh_df = utility.h5_to_df(hdf_file, 'Household')
person_df = utility.h5_to_df(hdf_file, 'Person')

parcel_df = pd.read_csv(parcel_filename, low_memory = False)
hh_by_parcel_df = hh_df[['hhparcel', 'hhexpfac', 'hhsize']].groupby('hhparcel').sum().reset_index()
hh_by_parcel_df = hh_by_parcel_df.merge(parcel_df[['PSRC_ID', 'GEOID10', 'Jurisdiction', 'BKRCastTAZ',]], left_on = 'hhparcel', right_on = 'PSRC_ID', how = 'right')
hh_by_parcel_df.rename(columns = {'hhexpfac':'total_hhs_by_parcel', 'hhsize':'total_persons_by_parcel'}, inplace = True)
hh_by_parcel_df.drop(['hhparcel'], axis = 1, inplace = True)
hh_by_parcel_df.fillna(0, inplace = True)
hh_by_parcel_df = hh_by_parcel_df.astype({'total_hhs_by_parcel':int,'total_persons_by_parcel':int} )

print(f'Exporting to {hhs_by_parcel_filename}')
hh_by_parcel_df.to_csv(os.path.join(working_folder, hhs_by_parcel_filename), index = False)
print('Done')

