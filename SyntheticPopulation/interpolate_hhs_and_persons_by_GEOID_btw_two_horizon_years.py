import pandas as pd
import h5py
import os
import utility

'''
This tool is to create an interpolated number of households and persons by blockgroup between two horizon years. It takes synthetic population in h5 format
as input files, and parcel lookup table and ofm_estimate_template_file as well. 

The output file is an input to generate_COB_local_hhs_estimate.py.
'''

### Configurations ######
## input files
future_year_synpop_file = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\PSRC\2035_PSRC_hh_and_persons_bkr.h5"
base_year_synpop_file = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\PSRC\2014_PSRC_hh_and_persons.h5"
parcel_filename = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'
ofm_estimate_template_file = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\OFM_estimate_template.csv"
target_year = 2030
future_year =2035
base_year = 2014

## Output files
interploated_ofm_estimate_by_GEOID = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2018TFPSensitivity\2030_ofm_estimate.csv"

### end of configuration
print 'Loading synthetic populations...'
future_hdf_file = h5py.File(future_year_synpop_file, "r")
base_hdf_file = h5py.File(base_year_synpop_file, "r")

future_hh_df = utility.h5_to_df(future_hdf_file, 'Household')
base_hh_df = utility.h5_to_df(base_hdf_file, 'Household')

future_hh_df['future_total_persons'] = future_hh_df['hhexpfac'] * future_hh_df['hhsize']
future_hh_df['future_total_hhs'] = future_hh_df['hhexpfac']

base_hh_df['base_total_persons'] = base_hh_df['hhexpfac'] * base_hh_df['hhsize']
base_hh_df['base_total_hhs'] = base_hh_df['hhexpfac']

parcel_df = pd.read_csv(parcel_filename, low_memory=False) 
future_hh_df = future_hh_df.merge(parcel_df, how = 'left', left_on = 'hhparcel', right_on = 'PSRC_ID')
future_hhs_by_geoid10 = future_hh_df.groupby('GEOID10')['future_total_hhs', 'future_total_persons'].sum()
base_hh_df = base_hh_df.merge(parcel_df, how = 'left', left_on = 'hhparcel', right_on = 'PSRC_ID')
base_hhs_by_geoid10 = base_hh_df.groupby('GEOID10')['base_total_hhs', 'base_total_persons'].sum()

print 'Future total hhs: ' + str(future_hh_df['future_total_hhs'].sum())
print 'Future total persons: '  + str(future_hh_df['future_total_persons'].sum())
print 'Base total hhs: ' + str(base_hh_df['base_total_hhs'].sum())
print 'Base total persons: ' + str(base_hh_df['base_total_persons'].sum())

ofm_df = pd.read_csv(ofm_estimate_template_file)
ofm_df = ofm_df.merge(future_hhs_by_geoid10, how = 'left', left_on = 'GEOID10', right_index = True)
ofm_df = ofm_df.merge(base_hhs_by_geoid10, how = 'left', left_on = 'GEOID10', right_index = True)

if target_year <= future_year and target_year >= base_year:
    # right between the bookends.
    print 'interpolating...'
else:
    print 'extropolating...'
    
ofm_df.fillna(0, inplace = True)
ratio = (target_year - base_year) * 1.0 / (future_year - base_year)
ofm_df['OFM_groupquarters'] = 0
ofm_df['OFM_hhs'] = ((ofm_df['future_total_hhs'] - ofm_df['base_total_hhs']) * ratio + ofm_df['base_total_hhs']).round(0)
ofm_df['OFM_persons'] = ((ofm_df['future_total_persons'] - ofm_df['base_total_persons']) * ratio + ofm_df['base_total_persons']).round(0)

print 'Interpolated total hhs: ' + str(ofm_df['OFM_hhs'].sum())
print 'Interpolated total persons: ' + str(ofm_df['OFM_persons'].sum())
ofm_df[['GEOID10', 'OFM_groupquarters', 'OFM_hhs', 'OFM_persons']].to_csv(interploated_ofm_estimate_by_GEOID, index = False)
utility.backupScripts(__file__, os.path.join(os.path.dirname(interploated_ofm_estimate_by_GEOID), os.path.basename(__file__)))

print 'Done'