import os, sys
sys.path.append(os.getcwd())
import pandas as pd

# 2/23/2021
# this script is used to link parcel data from Community Development to BKRCastTAZ and subarea
# # via PSRC_ID. Sometimes BKRCastTAZ and subarea column in the data from CD are little mismatched
# # so it is always good to reattach parcel data to lookup file to ensure we always
# # summarize land use  on the same base data.

### input files
kingcsqft = r'Z:\Modeling Group\BKRCast\LandUse\2019baseyear\2019_king_county_lu_sqft_by_PSRC_ID-tempuse.csv'
lookup_file = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'
subarea_file = r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\TAZ_subarea.csv"
###


lookup_df = pd.read_csv(lookup_file, sep = ',', low_memory = False)
kc_df = pd.read_csv(kingcsqft, sep = ',', low_memory = False)
subarea_df = pd.read_csv(subarea_file, sep = ',')

updated_kc = kc_df.merge(lookup_df[['PSRC_ID', 'Jurisdiction', 'BKRCastTAZ']], left_on = 'PSRC_ID', right_on = 'PSRC_ID', how = 'left')
updated_kc = updated_kc.merge(subarea_df[['BKRCastTAZ', 'Subarea', 'SubareaName']], left_on = 'BKRCastTAZ', right_on = 'BKRCastTAZ', how = 'left')
updated_kc.to_csv(r'Z:\Modeling Group\BKRCast\LandUse\2019baseyear\temporaryfile.csv', sep = ',')

print 'Done'



