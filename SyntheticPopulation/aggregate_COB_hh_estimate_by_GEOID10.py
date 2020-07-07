import os, sys
sys.path.append(os.getcwd())
import pandas as pd
import numpy as np
import utility

sf_occupancy_rate = 0.952  # from Gwen
mf_occupancy_rate = 0.895  # from Gwen
avg_persons_per_sfhh =  2.82 # from Gwen
avg_persons_per_mfhh =  2.03 # from Gwen

file1 = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2019\2019_COB_hhs_estimate.csv'
lookup_file = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'

file1_df = pd.read_csv(file1, sep =',')
lookup_df = pd.read_csv(lookup_file, sep = ',', low_memory = False)

file1_df = file1_df.merge(lookup_df[['PSRC_ID', 'GEOID10']], how = 'left', left_on = 'PSRC_ID', right_on = 'PSRC_ID')
file1_df.fillna(0, inplace = True)
hhs_by_GEOID10 = file1_df.groupby('GEOID10')[['SFUnits', 'MFUnits']].sum()
hhs_by_GEOID10['SF'] = hhs_by_GEOID10['SFUnits'] * sf_occupancy_rate
hhs_by_GEOID10['MF'] = hhs_by_GEOID10['MFUnits'] * mf_occupancy_rate
hhs_by_GEOID10['Tot_New_hhs'] = hhs_by_GEOID10['SF'] + hhs_by_GEOID10['MF']
hhs_by_GEOID10['Tot_New_Persons'] = hhs_by_GEOID10['SF'] * avg_persons_per_sfhh + hhs_by_GEOID10['MF'] * avg_persons_per_mfhh

hhs_by_GEOID10.to_csv(r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2019\2019_COB_hh_estimate_by_GEOID10.csv', sep = ',')
utility.backupScripts(__file__, os.path.join(os.path.dirname(file1), os.path.basename(__file__)))

print 'Done'



