'''
Very often PSRC will not have a parcel file consistent with our horizon year. Interpolation bewteen two different horizon years is unavoidable. We use
interpolated parcel file for outside of King County or outside of BKR area. Inside BKR area, we always have our own local estimates of jobs.
Create a new parcel file by interpolating employment bewteen two parcel files. The newly created parcel file has other non-job values
from parcel_file_name_latter.
'''
import pandas as pd
import numpy as np
import os


### Inputs
parcel_file_name_ealier = r'Z:\Modeling Group\BKRCast\CommonData\original_2014_parcels_urbansim.txt'
parcel_file_name_latter = r'Z:\Modeling Group\BKRCast\2035Parcel_fromPSRC\LUV2_2035SCinputs\LUV2_Refined_2035_SCInputs\parcels_bkr.txt'
working_folder = r'Z:\Modeling Group\BKRCast\2018LU'
new_parcel_file_name = 'interpolated_parcel_file.txt'
earlier_year = 2014
latter_year = 2035
horizon_year = 2018
### Enf of inputs

print 'Loading...'
parcel_earlier_df = pd.read_csv(parcel_file_name_ealier, sep = ' ')
parcel_earlier_df.columns = [i.upper() for i in parcel_earlier_df.columns]
parcel_latter_df = pd.read_csv(parcel_file_name_latter, sep = ' ')
parcel_latter_df.columns = [i.upper() for i in parcel_latter_df.columns]

job_cat = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPOTH_P', 'EMPRET_P', 'EMPRSC_P', 'EMPSVC_P']

columns = list(job_cat)
columns.append('PARCELID')

parcels_from_earlier_df = parcel_earlier_df.loc[:,columns]
parcels_from_earlier_df.set_index('PARCELID', inplace = True)
parcels_from_earlier_df.columns = [i + '_E' for i in parcels_from_earlier_df.columns]
parcels_from_earlier_df['EMPTOT_E'] = 0
for cat in job_cat:
    parcels_from_earlier_df['EMPTOT_E'] = parcels_from_earlier_df[cat + '_E'] + parcels_from_earlier_df['EMPTOT_E']

print 'Total jobs in year ', earlier_year, ' are ', parcels_from_earlier_df['EMPTOT_E'].sum()
parcel_horizon_df = parcel_latter_df.merge(parcels_from_earlier_df.reset_index(), how = 'inner', left_on = 'PARCELID', right_on = 'PARCELID')

parcel_horizon_df['EMPTOT_L'] = 0
for cat in job_cat:
    parcel_horizon_df['EMPTOT_L'] = parcel_horizon_df['EMPTOT_L'] + parcel_horizon_df[cat]
parcel_horizon_df['EMPTOT_P'] = parcel_horizon_df['EMPTOT_L']
print 'Total jobs in year ', latter_year, ' are ', parcel_horizon_df['EMPTOT_P'].sum()

# interpolate number of jobs, and round to integer.
for cat in job_cat:
    parcel_horizon_df[cat] = parcel_horizon_df[cat + '_E'] + ((horizon_year - earlier_year) * 1.0 / (latter_year - earlier_year) * (parcel_horizon_df[cat] - parcel_horizon_df[cat + '_E'])) 
    parcel_horizon_df[cat] = parcel_horizon_df[cat].round(0).astype(int)

parcel_horizon_df['EMPTOT_P'] = 0
for cat in job_cat:
    parcel_horizon_df['EMPTOT_P'] = parcel_horizon_df['EMPTOT_P'] + parcel_horizon_df[cat]

parcel_horizon_df = parcel_horizon_df.drop([i + '_E' for i in job_cat], axis = 1)
parcel_horizon_df = parcel_horizon_df.drop(['EMPTOT_L', 'EMPTOT_E'], axis = 1)
parcel_horizon_df.to_csv(os.path.join(working_folder, new_parcel_file_name), sep = ' ')
print 'After interpolation, total jobs are ', parcel_horizon_df['EMPTOT_P'].sum()
print 'done'