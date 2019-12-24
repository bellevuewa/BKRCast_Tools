import os, sys
sys.path.append(os.getcwd())
import pandas as pd
import numpy as np

file1 = r'Z:\Modeling Group\BKRCast\2020concurrencyPretest\2018_2019_permits_updated_pscrid+19-118270LP.csv'
lookup_file = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\BKRCast_Parcels_BKRTAZ.csv'

file1_df = pd.read_csv(file1, sep =',')
lookup_df = pd.read_csv(lookup_file, sep = ',', low_memory = False)

file1_df = file1_df.merge(lookup_df[['PSRC_ID', 'TAZNUM']], how = 'left', left_on = 'PSRC_ID', right_on = 'PSRC_ID')
file1_df.fillna(0, inplace = True)
hhs_by_oldtaz = file1_df.groupby('TAZNUM')[['SFUnits', 'MFUnits']].sum()

hhs_by_oldtaz.to_csv(r'Z:\Modeling Group\BKRCast\2020concurrencyPretest\hhs_growth_by_oldtaz.csv', sep = ',')

print 'Done'



