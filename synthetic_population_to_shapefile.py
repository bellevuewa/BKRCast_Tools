import pandas as pd
import h5py
import utility

parcel_point_name = r'F:\2014_parcels_point.csv'
synthetic_population_name = r'D:\PopulationSim\PSRCrun0423\output\popsim_hh_and_persons.h5'
output_synthetic_pop_point_name = r'D:\PopulationSim\PSRCrun0423\output\synthetic_pop_gis_point_data.csv'

hdf_file = h5py.File(synthetic_population_name, "r")
person_df = utility.h5_to_df(hdf_file, 'Person')
hh_df = utility.h5_to_df(hdf_file, 'Household')

hh_sum_df = hh_df.groupby('hhparcel')['hhexpfac', 'hhsize'].sum()
hh_sum_df.reset_index(inplace = True)
parcel_df = pd.read_csv(parcel_point_name, low_memory=False) 
parcel_pop_point_df = parcel_df.merge(hh_sum_df, how = 'inner', left_on = 'PSRC_ID', right_on = 'hhparcel')
parcel_pop_point_df.rename(columns = {'hhexpfac': 'hhs', 'hhsize' : 'population'}, inplace = True)
parcel_pop_point_df.to_csv(output_synthetic_pop_point_name)

print 'Done'