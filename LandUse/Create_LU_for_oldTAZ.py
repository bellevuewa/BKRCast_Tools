import pandas as pd
import os

'''
   this program is used to aggregate number of jobs and households from parcel file to the old BKR TAZ system. 
'''

### configuration
parcel_file = r'Z:\Modeling Group\BKRCast\2020concurrencyPretest\parcels_urbansim_2020concurrency_pretest.txt'
lookup_file = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\BKRCast_Parcels_BKRTAZ.csv'
output_file = r'Z:\Modeling Group\BKRCast\2020concurrencyPretest\2020_concurrency_Pretest_LU_by_oldTAZ.csv'
permit_output_file = r'Z:\Modeling Group\BKRCast\2020concurrencyPretest\2018_2019_permits_by_oldtaz.csv'
### end of configuration


parcel_df = pd.read_csv(parcel_file, sep = ' ', low_memory = False)
lookup_df = pd.read_csv(lookup_file, sep = ',', low_memory = False)

JOB_CATEGORY = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPOTH_P', 'EMPRET_P', 'EMPSVC_P', 'EMPTOT_P', 'HH_P']

parcel_df = parcel_df.merge(lookup_df, how = 'left', left_on = 'PARCELID', right_on = 'PSRC_ID')

lu_by_oldtaz_df = parcel_df.groupby('TAZNUM')[JOB_CATEGORY].sum()

# convention of aggregation
lu_by_oldtaz_df['FIRES'] = lu_by_oldtaz_df['EMPOFC_P'] + lu_by_oldtaz_df['EMPSVC_P'] 
lu_by_oldtaz_df['Inst'] =  lu_by_oldtaz_df['EMPGOV_P'] + lu_by_oldtaz_df['EMPEDU_P'] + lu_by_oldtaz_df['EMPMED_P']
lu_by_oldtaz_df['Retail'] = lu_by_oldtaz_df['EMPRET_P'] + lu_by_oldtaz_df['EMPFOO_P'] + lu_by_oldtaz_df['EMPOTH_P']
lu_by_oldtaz_df['Ind'] = lu_by_oldtaz_df['EMPIND_P']
lu_by_oldtaz_df['HHs'] = lu_by_oldtaz_df['HH_P']

lu_by_oldtaz_df.drop(JOB_CATEGORY, axis = 1, inplace = True)
lu_by_oldtaz_df.to_csv(output_file, sep = ',')


permit_file = r"Z:\Modeling Group\BKRCast\2020concurrencyPretest\2018_2019_permits_updated_pscrid+19-118270LP-for_trip_model.csv"
permit_df = pd.read_csv(permit_file, sep = ',')
permit_df = permit_df.merge(lookup_df[['PSRC_ID', 'TAZNUM']], how = 'left', on = 'PSRC_ID')

emp_cats = ['Edu', 'Foo', 'Gov', 'Ind', 'Med', 'Ofc', 'Oth', 'Ret', 'Svc']
density_levels = ['VeryLow', 'Low', 'Med', 'High', 'VeryHigh']

sqft_by_old_taz = permit_df.groupby('TAZNUM').sum()
sqft_by_old_taz.fillna(0, inplace = True)
sqft_by_old_taz['temp'] = 0
columns = sqft_by_old_taz.columns
for emp in emp_cats:
    sqft_by_old_taz['temp'] = 0
    for den in density_levels:
        col = emp + '_' +den
        sqft_by_old_taz['temp'] = sqft_by_old_taz['temp'] + sqft_by_old_taz[col]
    sqft_by_old_taz[emp] = sqft_by_old_taz['temp']

sqft_by_old_taz['FIRES'] = sqft_by_old_taz['Ofc'] + sqft_by_old_taz['Svc']
sqft_by_old_taz['Inst'] = sqft_by_old_taz['Gov']  + sqft_by_old_taz['Edu'] + sqft_by_old_taz['Med']
sqft_by_old_taz['Retail'] = sqft_by_old_taz['Ret'] + sqft_by_old_taz['Foo'] + sqft_by_old_taz['Oth']
sqft_by_old_taz['Industrial'] = sqft_by_old_taz['Ind']
sqft_by_old_taz['SFU'] = sqft_by_old_taz['SFUnits']
sqft_by_old_taz['MFU'] = sqft_by_old_taz['MFUnits']
sqft_by_old_taz['Hotel'] = sqft_by_old_taz['Hotel_Rooms']
sqft_by_old_taz.drop(columns, axis = 1, inplace = True)

sqft_by_old_taz.to_csv(permit_output_file, sep = ',')


print 'Done'

