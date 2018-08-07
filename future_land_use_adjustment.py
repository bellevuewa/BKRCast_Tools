import pandas as pd
import h5py
import numpy as np
import random 
import os

PSRC_2014_ESD_Parcel_File = r'Z:\Modeling Group\BKRCast\2014ESD\parcels_urbansim.txt'
PSRC_Future_Year_Parcel_File = r'Z:\Modeling Group\BKRCast\2035Parcel_fromPSRC\LUV2_2035SCinputs\LUV2_Refined_2035_SCInputs\parcels_urbansim.txt'
BKRCast_2014_SQFT_based_Parcel_File = r'Z:\Modeling Group\BKRCast\Job Conversion Test\parcel_level\test11\parcels_urbansim.txt'
Output_Parcel_Folder = r'Z:\Modeling Group\BKRCast\2035Parcel_Sqft_based'

JOB_Category = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPOTH_P', 'EMPRET_P', 'EMPRSC_P', 'EMPSVC_P', 'EMPTOT_P', 'HH_P']

print 'Loading ... '     
PSRC_2014_ESD_Parcel_DF = pd.DataFrame.from_csv(PSRC_2014_ESD_Parcel_File, sep = " ")
PSRC_Future_Year_Parcel_DF = pd.DataFrame.from_csv(PSRC_Future_Year_Parcel_File, sep = ' ')
BKRCast_2014_SQFT_based_Parcel_DF = pd.DataFrame.from_csv(BKRCast_2014_SQFT_based_Parcel_File, sep = ' ')
PSRC_2014_ESD_Parcel_DF.reset_index(inplace = True)
PSRC_Future_Year_Parcel_DF.reset_index(inplace = True)
BKRCast_2014_SQFT_based_Parcel_DF.reset_index(inplace = True)

print '2014 PSRC ESD Parcel file: ' + str(PSRC_2014_ESD_Parcel_DF.shape)
print 'PSRC future year parcel file: ' + str(PSRC_Future_Year_Parcel_DF.shape)
print 'BKRCast 2014 SQFT based parcel file ' + str(BKRCast_2014_SQFT_based_Parcel_DF.shape)

print 'Export error files ...'
parcel_in_2014_not_in_future_DF = PSRC_2014_ESD_Parcel_DF[~PSRC_2014_ESD_Parcel_DF['PARCELID'].isin(PSRC_Future_Year_Parcel_DF['PARCELID'])]
parcel_in_future_not_in_2014_DF = PSRC_Future_Year_Parcel_DF[~PSRC_Future_Year_Parcel_DF['PARCELID'].isin(PSRC_2014_ESD_Parcel_DF['PARCELID'])]
parcel_in_2014_not_in_future_DF.to_csv(os.path.join(Output_Parcel_Folder, 'parcel_in_2014_not_in_future.csv'))
parcel_in_future_not_in_2014_DF.to_csv(os.path.join(Output_Parcel_Folder, 'parcel_in_future_not_in_2014.csv')) 

print 'Calculating growth from PSRC parcel data ...'
PSRC_2014_ESD_Parcel_DF.set_index('PARCELID', inplace = True)
temp = list(JOB_Category)
temp.append('TAZ_P')
PSRC_2014_ESD_Parcel_DF_Jobs = PSRC_2014_ESD_Parcel_DF[temp]
PSRC_2014_ESD_Parcel_DF_Jobs.rename(columns = lambda x : x + '_2014', inplace = True)

PSRC_Future_Year_Parcel_DF.set_index('PARCELID', inplace = True)
PSRC_Future_Year_Parcel_DF_Jobs = PSRC_Future_Year_Parcel_DF[JOB_Category]
PSRC_Future_Year_Parcel_DF_Jobs.rename(columns = lambda x : x + '_future', inplace = True)
parcels_growth_df = PSRC_2014_ESD_Parcel_DF_Jobs.join(PSRC_Future_Year_Parcel_DF_Jobs)

for job in JOB_Category:
    parcels_growth_df[job + '_delta'] = PSRC_Future_Year_Parcel_DF_Jobs[job + '_future'] - PSRC_2014_ESD_Parcel_DF_Jobs[job + '_2014']

print 'Exporting PSRC growth parcel data ...'
parcels_growth_df.to_csv(os.path.join(Output_Parcel_Folder, 'parcel_growth.csv'))

print 'Applying growth to the BKRCast base year (sqft based) jobs ... '
Job_category_delta = [x + '_delta' for x in JOB_Category]
Job_category_2014 = [x + '_2014' for x in JOB_Category]
Job_category_future = [x + '_future' for x in JOB_Category]

BKRCast_2014_SQFT_based_Parcel_DF.set_index('PARCELID', inplace = True)
BKRCast_Future_Adjusted_Jobs_DF = BKRCast_2014_SQFT_based_Parcel_DF[JOB_Category].join(parcels_growth_df[Job_category_delta])

for job in JOB_Category:
    BKRCast_Future_Adjusted_Jobs_DF[job] = BKRCast_Future_Adjusted_Jobs_DF[job] + BKRCast_Future_Adjusted_Jobs_DF[job + '_delta']
BKRCast_Future_Adjusted_Jobs_DF.drop(Job_category_delta, axis = 1, inplace = True)

print 'Generating BKRCast future year parcel file ...'
BKRCast_Future_Parcels_DF = PSRC_Future_Year_Parcel_DF.drop(JOB_Category, axis = 1)
BKRCast_Future_Parcels_DF = BKRCast_Future_Parcels_DF.join(BKRCast_Future_Adjusted_Jobs_DF)
BKRCast_Future_Parcels_DF.to_csv(os.path.join(Output_Parcel_Folder, 'BKRCastFuture_parcels_urbansim.txt'), sep = ' ')
print 'Done.'