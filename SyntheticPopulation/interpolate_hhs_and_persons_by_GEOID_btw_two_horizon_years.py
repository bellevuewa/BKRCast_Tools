from numpy.core.numeric import True_
import pandas as pd
import h5py
import os
import utility

'''
This tool is to create an interpolated number of households and persons by blockgroup between two horizon years. It takes synthetic population in h5 format
as input files, and parcel lookup table and ofm_estimate_template_file as well. 

The output file is an input to generate_COB_local_hhs_estimate.py.

03/03/2022
now it also exports total_hhs_by_parcel and total_persons_by_parcel to file.   Both households and persons have decimal points. 
The output file is an input to Prepare_Hhs_for_future_using_KR_oldTAZ_COB_parcel_forecast.py
'''

### Configurations ######
## input files
future_year_synpop_file = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\PSRC\2050_PSRC_hh_and_persons_bkr.h5"
base_year_synpop_file = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\PSRC\2014_psrc_hh_and_persons.h5"
parcel_filename = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'
ofm_estimate_template_file = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\OFM_estimate_template.csv"
target_year = 2044
future_year =2050
base_year = 2014

## Output files
interploated_ofm_estimate_by_GEOID = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\Complan\complan2044\Alt3\2044_ofm_estimate_from_PSRC_2014_2050.csv"
hhs_by_parcel_filename = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\Complan\complan2044\Alt3\2044_hhs_by_parcels_from_PSRC_2014_2050.csv'
final_output_pop_file = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\Complan\complan2044\Alt3\2044_interpolated_synthetic_population_from_SC.h5'

### end of configuration
print('Loading synthetic populations...')
future_hdf_file = h5py.File(future_year_synpop_file, "r")
base_hdf_file = h5py.File(base_year_synpop_file, "r")

future_hh_df = utility.h5_to_df(future_hdf_file, 'Household')
base_hh_df = utility.h5_to_df(base_hdf_file, 'Household')

future_hh_df['future_total_persons'] = future_hh_df['hhexpfac'] * future_hh_df['hhsize']
future_hh_df['future_total_hhs'] = future_hh_df['hhexpfac']

base_hh_df['base_total_persons'] = base_hh_df['hhexpfac'] * base_hh_df['hhsize']
base_hh_df['base_total_hhs'] = base_hh_df['hhexpfac']

parcel_df = pd.read_csv(parcel_filename, low_memory=False) 
future_hh_df = future_hh_df.merge(parcel_df[['PSRC_ID', 'GEOID10', 'BKRCastTAZ']], how = 'left', left_on = 'hhparcel', right_on = 'PSRC_ID')
future_hhs_by_geoid10 = future_hh_df.groupby('GEOID10')[['future_total_hhs', 'future_total_persons']].sum()
base_hh_df = base_hh_df.merge(parcel_df, how = 'left', left_on = 'hhparcel', right_on = 'PSRC_ID')
base_hhs_by_geoid10 = base_hh_df.groupby('GEOID10')[['base_total_hhs', 'base_total_persons']].sum()

print('Future total hhs: ' + str(future_hh_df['future_total_hhs'].sum()))
print('Future total persons: '  + str(future_hh_df['future_total_persons'].sum()))
print('Base total hhs: ' + str(base_hh_df['base_total_hhs'].sum()))
print('Base total persons: ' + str(base_hh_df['base_total_persons'].sum()))

ofm_df = pd.read_csv(ofm_estimate_template_file)
ofm_df = ofm_df.merge(future_hhs_by_geoid10, how = 'left', left_on = 'GEOID10', right_index = True)
ofm_df = ofm_df.merge(base_hhs_by_geoid10, how = 'left', left_on = 'GEOID10', right_index = True)

if target_year <= future_year and target_year >= base_year:
    # right between the bookends.
    print('interpolating...')
else:
    print('extropolating...')
    
ofm_df.fillna(0, inplace = True)
ratio = (target_year - base_year) * 1.0 / (future_year - base_year)
ofm_df['OFM_groupquarters'] = 0
ofm_df['OFM_hhs'] = ((ofm_df['future_total_hhs'] - ofm_df['base_total_hhs']) * ratio + ofm_df['base_total_hhs']).round(0)
ofm_df['OFM_persons'] = ((ofm_df['future_total_persons'] - ofm_df['base_total_persons']) * ratio + ofm_df['base_total_persons']).round(0)

print('Interpolated total hhs: ' + str(ofm_df['OFM_hhs'].sum()))
print('Interpolated total persons: ' + str(ofm_df['OFM_persons'].sum()))
ofm_df[['GEOID10', 'OFM_groupquarters', 'OFM_hhs', 'OFM_persons']].to_csv(interploated_ofm_estimate_by_GEOID, index = False)

base_hhs_by_parcel = base_hh_df[['PSRC_ID', 'base_total_hhs', 'base_total_persons']].groupby('PSRC_ID').sum()
future_hhs_by_parcel  = future_hh_df[['PSRC_ID', 'future_total_hhs', 'future_total_persons']].groupby('PSRC_ID').sum()
target_hhs_by_parcel = pd.merge(base_hhs_by_parcel, future_hhs_by_parcel, on = 'PSRC_ID', how = 'outer')
target_hhs_by_parcel.fillna(0, inplace = True)
target_hhs_by_parcel['total_hhs_by_parcel'] = target_hhs_by_parcel['base_total_hhs'] + (target_hhs_by_parcel['future_total_hhs'] - target_hhs_by_parcel['base_total_hhs']) * ratio
target_hhs_by_parcel['total_persons_by_parcel'] = target_hhs_by_parcel['base_total_persons'] + (target_hhs_by_parcel['future_total_persons'] - target_hhs_by_parcel['base_total_persons']) * ratio
target_hhs_by_parcel.drop(['base_total_hhs', 'base_total_persons', 'future_total_hhs', 'future_total_persons'], axis = 1, inplace = True)
target_hhs_by_parcel.reset_index(inplace = True)
target_hhs_by_parcel = parcel_df[['PSRC_ID', 'Jurisdiction', 'BKRCastTAZ', 'GEOID10']].merge(target_hhs_by_parcel[['PSRC_ID', 'total_hhs_by_parcel', 'total_persons_by_parcel']], on = 'PSRC_ID', how = 'left')
target_hhs_by_parcel.fillna(0, inplace= True)
target_hhs_by_parcel.to_csv(hhs_by_parcel_filename, index = False)

avg_person_per_hhs_df = target_hhs_by_parcel[['Jurisdiction', 'total_hhs_by_parcel', 'total_persons_by_parcel']].groupby('Jurisdiction').sum()
avg_person_per_hhs_df['avg_persons_per_hh'] = avg_person_per_hhs_df['total_persons_by_parcel'] / avg_person_per_hhs_df['total_hhs_by_parcel']
print('%s' % avg_person_per_hhs_df)

print('Generating households... ')
target_hhs_by_taz = target_hhs_by_parcel[['BKRCastTAZ', 'total_hhs_by_parcel']].groupby('BKRCastTAZ').sum().reset_index()
target_hhs_by_taz = target_hhs_by_taz.loc[target_hhs_by_taz['total_hhs_by_parcel'] > 0]
future_hh_df.drop(['PSRC_ID', 'future_total_persons', 'future_total_hhs', 'GEOID10', 'BKRCastTAZ'], axis = 1, inplace=True)
target_hhs_df = pd.DataFrame()

for taz in target_hhs_by_taz['BKRCastTAZ'].tolist():
    hhs_in_taz = future_hh_df.loc[future_hh_df['hhtaz'] == taz]
    num_hhs_popsim = hhs_in_taz['hhexpfac'].sum()
    num_hhs = int(target_hhs_by_taz.loc[target_hhs_by_taz['BKRCastTAZ'] == taz, 'total_hhs_by_parcel'].tolist()[0])
    if num_hhs_popsim > num_hhs:
        target_hhs_df = pd.concat([target_hhs_df, hhs_in_taz.sample(n = num_hhs)])
    else:
        target_hhs_df = pd.concat([target_hhs_df, hhs_in_taz.sample(n = num_hhs_popsim)])
print(f"total hhs: {target_hhs_df['hhexpfac'].sum()}")

print('Generating persons...')
future_persons_df = utility.h5_to_df(future_hdf_file, 'Person')
target_persons_df = future_persons_df.loc[future_persons_df['hhno'].isin(target_hhs_df['hhno'])]
print(f"total persons: {target_persons_df['psexpfac'].sum()}")
print('Exporting .h5...')
output_h5_file = h5py.File(final_output_pop_file, 'w')
utility.df_to_h5(target_hhs_df, output_h5_file, 'Household')
utility.df_to_h5(target_persons_df, output_h5_file, 'Person')

output_h5_file.close()



utility.backupScripts(__file__, os.path.join(os.path.dirname(interploated_ofm_estimate_by_GEOID), os.path.basename(__file__)))

print('Done')