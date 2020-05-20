import pandas as pd


psrcid_mapping_file_name = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'
input_file_name = r'Z:\Modeling Group\BKRCast\2020concurrency\2018_2019_permits.csv'
output_file_name = r'Z:\Modeling Group\BKRCast\2020concurrency\2018_2019_permits_updated.csv' 

maping_df = pd.read_csv(psrcid_mapping_file_name, low_memory = False, sep = ',')
input_df = pd.read_csv(input_file_name, low_memory = False, sep = ',')

#pin_psrcid_df = maping_df[['PIN', 'PSRC_ID']]
#output_df = input_df.merge(pin_psrcid_df, how = 'left', left_on = 'PIN', right_on = 'PIN')

output_df = input_df.groupby('PIN').sum()
output_df.reset_index(inplace = True)
output_df = output_df.merge(input_df[['PIN', 'Action']].drop_duplicates(), how = 'left', left_on = 'PIN', right_on = 'PIN')

output_df.to_csv(output_file_name, sep = ',')

print 'Done'
