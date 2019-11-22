import pandas as pd
import os
import utility

'''
Seed households and persons downloaded from census website are groupped by state. It includes too many data that are not 
within our modeling area. So this tool is used to retrieve data only in four counties: King, Snohomish, Pierce, and Kitsap.
For some reason, the original household serialno (presumed to be household id) does not work well so an extra column 
'hhnum' is added and numbered as household id.
'''
## confirguration
working_folder = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2018'
original_seed_households_file = 'wa2017_pums_hhs.csv'
original_seed_population_file = 'wa2017_pums_persons.csv'
puma_county_lookup_file = '2010_county_puma_lookup.csv'
output_hhs = '2017_seed_households.csv'
output_persons = '2017_seed_persons.csv'

## end of configuration

original_hhs_pd = pd.read_csv(os.path.join(working_folder, original_seed_households_file), sep = ',')
original_persons_pd = pd.read_csv(os.path.join(working_folder, original_seed_population_file), sep = ',')
puma_county_df = pd.read_csv(os.path.join(working_folder, puma_county_lookup_file), sep = ',')

hhs = original_hhs_pd.merge(puma_county_df, how = 'right', left_on = 'PUMA', right_on = 'PUMACE10')
hhs.drop(puma_county_df.columns, axis = 1, inplace = True)
hhs['hhnum'] = hhs.index + 1
hhs = hhs.fillna(0)
hhs.to_csv(os.path.join(working_folder, output_hhs), sep = ',')

persons = original_persons_pd.merge(puma_county_df, how = 'right', left_on = 'PUMA', right_on = 'PUMACE10')
persons.drop(puma_county_df.columns, axis = 1, inplace = True)
persons = persons.merge(hhs[['SERIALNO', 'hhnum']], how = 'left', left_on = 'SERIALNO', right_on = 'SERIALNO')
persons = persons.fillna(0)
persons.to_csv(os.path.join(working_folder, output_persons), sep = ',')

print 'done'

