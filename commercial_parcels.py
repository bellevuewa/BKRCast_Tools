import os
import pandas as pd

parcels_list = r'Z:\Modeling Group\BKRCast\2014ESD\Bel_commercial_parcels.csv'
urbansim_parcel_filename = r'Z:\Modeling Group\BKRCast\2014ESD\parcels_urbansim.txt'
Output_Field = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPOTH_P', 'EMPRET_P', 'EMPSVC_P', 'EMPTOT_P', 'HH_P']

print "Loading input files ..."
parcels_df = pd.DataFrame.from_csv(urbansim_parcel_filename, sep = " ")
parcels_df.reset_index(inplace = True)
selected_parcels_list_df = pd.DataFrame.from_csv(parcels_list)
selected_parcels_list_df.reset_index(inplace = True)

selected_parcels_df = selected_parcels_list_df.merge(parcels_df, left_on = 'PSRC_ID', right_on = 'PARCELID') 
summary_by_taz = selected_parcels_df.groupby('TAZ_P')[Output_Field].sum()
summary_by_taz.to_csv(r'Z:\Modeling Group\BKRCast\2014ESD\Bel_commercial_parcels_jobs.csv')

print 'done' 
