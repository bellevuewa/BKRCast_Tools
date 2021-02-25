import pandas as pd
import os
import utility

########### configuration #########################
## input ##
working_folder = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2020'   
parcel_filename = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'
kc_housingUnits_file = '2020_BKR_housingunits.csv'
ofm_hhs_estimate_file = '2020_OFM_estimate.csv'

## output ##
cob_hhs_estimate_by_blkgp_file = '2020_COB_hh_estimate_by_GEOID10_BKR.csv'
cob_hhs_estimate_by_parcel_file = '2020_COB_hhs_estimate_BKR.csv'


sf_occupancy_rate = 0.952  # from Gwen
mf_occupancy_rate = 0.895  # from Gwen
avg_persons_per_sfhh =  2.82 # from Gwen
avg_persons_per_mfhh =  2.03 # from Gwen

######################################################

parcel_df = pd.read_csv(parcel_filename, low_memory=False) 
kc_housingunits_df = pd.read_csv(os.path.join(working_folder,kc_housingUnits_file)) 
ofm_estimate_df = pd.read_csv(os.path.join(working_folder, ofm_hhs_estimate_file))

bel_parcels_df = parcel_df[parcel_df['Jurisdiction'] == 'BELLEVUE']
bel_parcels_df = bel_parcels_df.merge(kc_housingunits_df, how = 'left', on = 'PSRC_ID').fillna(0)
bel_parcels_df[['PSRC_ID', 'SFUnits', 'MFUnits']].to_csv(os.path.join(working_folder, cob_hhs_estimate_by_parcel_file), sep = ',', index = False)

cob_hhs_estimate_df = bel_parcels_df.groupby('GEOID10')['SFUnits', 'MFUnits'].sum()
cob_hhs_estimate_df['SF'] = cob_hhs_estimate_df['SFUnits']  * sf_occupancy_rate
cob_hhs_estimate_df['MF'] = cob_hhs_estimate_df['MFUnits']  * mf_occupancy_rate
cob_hhs_estimate_df['Tot_New_hhs'] = cob_hhs_estimate_df['SF'] + cob_hhs_estimate_df['MF']
cob_hhs_estimate_df['Tot_New_Persons'] = cob_hhs_estimate_df['SF'] * avg_persons_per_sfhh + cob_hhs_estimate_df['MF'] * avg_persons_per_mfhh
cob_hhs_estimate_df = cob_hhs_estimate_df.merge(ofm_estimate_df[['GEOID10', 'OFM_hhs', 'OFM_persons']], how = 'left', left_index = True, right_on = 'GEOID10')
cob_hhs_estimate_df.to_csv(os.path.join(working_folder, cob_hhs_estimate_by_blkgp_file), sep = ',',  index = False)

utility.backupScripts(__file__, os.path.join(working_folder, os.path.basename(__file__)))

print 'Done'
