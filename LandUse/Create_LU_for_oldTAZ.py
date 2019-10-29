import pandas as pd
import os

parcel_file = r'Z:\Modeling Group\BKRCast\2018LU\parcels_urbansim.txt'
lookup_file = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\BKRCast_Parcels_BKRTAZ.csv'
output_file = r'Z:\Modeling Group\BKRCast\2018LU\2018_LU_by_oldTAZ.csv'

parcel_df = pd.read_csv(parcel_file, sep = ' ', low_memory = False)
lookup_df = pd.read_csv(lookup_file, sep = ',', low_memory = False)

JOB_CATEGORY = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPOTH_P', 'EMPRET_P', 'EMPSVC_P', 'EMPTOT_P', 'HH_P']

parcel_df = parcel_df.merge(lookup_df, how = 'left', left_on = 'PARCELID', right_on = 'PSRC_ID')

lu_by_oldtaz_df = parcel_df.groupby('TAZNUM')[JOB_CATEGORY].sum()

lu_by_oldtaz_df['Office'] = lu_by_oldtaz_df['EMPGOV_P'] + lu_by_oldtaz_df['EMPOFC_P'] + lu_by_oldtaz_df['EMPSVC_P'] + lu_by_oldtaz_df['EMPMED_P']
lu_by_oldtaz_df['Retail'] = lu_by_oldtaz_df['EMPRET_P'] + lu_by_oldtaz_df['EMPFOO_P'] + lu_by_oldtaz_df['EMPOTH_P']
lu_by_oldtaz_df['Edu'] = lu_by_oldtaz_df['EMPEDU_P']
lu_by_oldtaz_df['Ind'] = lu_by_oldtaz_df['EMPIND_P']
lu_by_oldtaz_df['HHs'] = lu_by_oldtaz_df['HH_P']

lu_by_oldtaz_df.drop(JOB_CATEGORY, axis = 1, inplace = True)
lu_by_oldtaz_df.to_csv(output_file, sep = ',')



print 'Done'

