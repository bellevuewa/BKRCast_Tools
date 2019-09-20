import pandas as pd
import h5py
import numpy as np
import random 
import os

PSRC_2014_ESD_Parcel_File = r'Z:\Modeling Group\BKRCast\2014ESD\parcels_urbansim.txt'
PSRC_Future_Year_Parcel_File = r'Z:\Modeling Group\BKRCast\2035Parcel_fromPSRC\LUV2_2035SCinputs\LUV2_Refined_2035_SCInputs\parcels_bkr.txt'
BKRCast_2014_SQFT_based_Parcel_File = r'Z:\Modeling Group\BKRCast\Job Conversion Test\parcel_level\test11\parcels_urbansim.txt'
#BKRCast_2014_SQFT_based_Parcel_File = r'Z:\Modeling Group\BKRCast\2014_ParkingCost\Half_parking_Cost_Bellevue\parcels_urbansim.txt'
Output_Parcel_Folder = r'Z:\Modeling Group\BKRCast\2035Parcel_Sqft_based'
Hh_and_person_file = r'D:\2035BKRCastBaseline\2035BKRCastBaseline\inputs\hh_and_persons.h5'
JOB_Category = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPOTH_P', 'EMPRET_P', 'EMPRSC_P', 'EMPSVC_P', 'EMPTOT_P']

def h5_to_df(h5_file, group_name):
    """
    Converts the arrays in a H5 store to a Pandas DataFrame. 
    """
    col_dict = {}
    h5_set = hdf_file[group_name]
    for col in h5_set.keys():
        my_array = np.asarray(h5_set[col])
        col_dict[col] = my_array
    df = pd.DataFrame(col_dict)
    return df

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

parcels_growth_df.reset_index(inplace = True)
parcels_growth_df.sort_values(by = ['TAZ_P_2014', 'PARCELID'], inplace = True)
print 'Exporting PSRC growth parcel data ...'
parcels_growth_df.to_csv(os.path.join(Output_Parcel_Folder, 'parcel_growth.csv'))

print 'Applying growth to the BKRCast base year (sqft based) jobs ... '
Job_category_delta = [x + '_delta' for x in JOB_Category]
Job_category_2014 = [x + '_2014' for x in JOB_Category]
Job_category_future = [x + '_future' for x in JOB_Category]

BKRCast_2014_SQFT_based_Parcel_DF.set_index('PARCELID', inplace = True)
parcels_growth_df.set_index('PARCELID', inplace = True)
BKRCast_Future_Adjusted_Jobs_DF = BKRCast_2014_SQFT_based_Parcel_DF[JOB_Category].join(parcels_growth_df[Job_category_delta])

for job in JOB_Category:
    BKRCast_Future_Adjusted_Jobs_DF[job] = BKRCast_Future_Adjusted_Jobs_DF[job] + BKRCast_Future_Adjusted_Jobs_DF[job + '_delta']
BKRCast_Future_Adjusted_Jobs_DF.drop(Job_category_delta, axis = 1, inplace = True)

print 'Generating BKRCast future year parcel file ...'
BKRCast_Future_Parcels_DF = PSRC_Future_Year_Parcel_DF.drop(JOB_Category, axis = 1)
BKRCast_Future_Parcels_DF = BKRCast_Future_Parcels_DF.join(BKRCast_Future_Adjusted_Jobs_DF)
BKRCast_Future_Parcels_DF.reset_index(inplace = True)
BKRCast_Future_Parcels_DF.sort_values(by = ['TAZ_P', 'PARCELID'], inplace = True)
BKRCast_Future_Parcels_DF.set_index('PARCELID', inplace = True)

# export rows with negative job number
print 'Exporting rows with negative job numbers...'
BKRCast_Negative_Parcels_DF = BKRCast_Future_Parcels_DF[(BKRCast_Future_Parcels_DF['EMPEDU_P'] < 0) | (BKRCast_Future_Parcels_DF['EMPFOO_P'] < 0) | (BKRCast_Future_Parcels_DF['EMPGOV_P'] < 0) | (BKRCast_Future_Parcels_DF['EMPIND_P'] < 0) | (BKRCast_Future_Parcels_DF['EMPMED_P'] < 0) | (BKRCast_Future_Parcels_DF['EMPOFC_P'] < 0)  | (BKRCast_Future_Parcels_DF['EMPOTH_P'] < 0) | (BKRCast_Future_Parcels_DF['EMPRET_P'] < 0) | (BKRCast_Future_Parcels_DF['EMPRSC_P'] < 0) | (BKRCast_Future_Parcels_DF['EMPSVC_P'] < 0)]
BKRCast_Negative_Parcels_DF.to_csv(os.path.join(Output_Parcel_Folder, 'BKRCastFuture_parcels_negative.txt'), sep = ' ')    
BKRCast_Negative_Parcels_DF.rename(columns = {'EMPTOT_P': 'EMPTOT_P_FUTURE'}, inplace = True)                              
BKRCast_Negative_Fixed_DF =  BKRCast_2014_SQFT_based_Parcel_DF.join(BKRCast_Negative_Parcels_DF['EMPTOT_P_FUTURE'], how = 'right')

print 'Fixing the negative job numbers by redistributing jobs based on base year job distribution...'
for job in JOB_Category:
    BKRCast_Negative_Fixed_DF[job] = BKRCast_Negative_Fixed_DF[job] * (BKRCast_Negative_Fixed_DF['EMPTOT_P_FUTURE'] / BKRCast_Negative_Fixed_DF['EMPTOT_P'])

# if base year total jobs = 0 but future year has jobs, use future year PSRC number.
tempindex = BKRCast_Negative_Fixed_DF.loc[(BKRCast_Negative_Fixed_DF['EMPTOT_P'] == 0) & (BKRCast_Negative_Fixed_DF['EMPTOT_P_FUTURE'] > 0)].index
BKRCast_Negative_Fixed_DF = BKRCast_Negative_Fixed_DF.join(PSRC_Future_Year_Parcel_DF_Jobs, how = 'left')
for job in JOB_Category:
    BKRCast_Negative_Fixed_DF.loc[tempindex, job] = BKRCast_Negative_Fixed_DF[job + '_future']

# BKRCast_Negative_Fixed_DF.replace([np.inf, -np.inf], 0)
# Future year cannot have negative total jobs. If it has, zero all job numbers.
BKRCast_Negative_Fixed_DF.loc[BKRCast_Negative_Fixed_DF['EMPTOT_P_FUTURE']<0, JOB_Category] = 0
# copy back the adjusted jobs because of prior negative numbers

# Update hh based on othe hh_and_persons.h5
BKRCast_Future_Parcels_DF.loc[BKRCast_Future_Parcels_DF.index.isin(BKRCast_Negative_Fixed_DF.index), JOB_Category] =  BKRCast_Negative_Fixed_DF[JOB_Category]

print 'Loading hh_and_persons.h5...'
hdf_file = h5py.File(Hh_and_person_file, "r")
hh_df = h5_to_df(hdf_file, 'Household')
hh_df.set_index('hhparcel', inplace = True)

print 'Updating number of households...'
hhs = hh_df.groupby('hhparcel')['hhexpfac', 'hhsize'].sum()
BKRCast_Future_Parcels_DF = BKRCast_Future_Parcels_DF.join(hhs, how = 'left')
BKRCast_Future_Parcels_DF['HH_P']  = BKRCast_Future_Parcels_DF['hhexpfac']
BKRCast_Future_Parcels_DF.fillna(0, inplace = True)
BKRCast_Future_Parcels_DF.drop(['hhexpfac', 'hhsize'], axis = 1, inplace = True)


print 'Exporting future parcel file...'
BKRCast_Future_Parcels_DF.to_csv(os.path.join(Output_Parcel_Folder, 'BKRCastFuture_parcels_urbansim_test.txt'), sep = ' ')


print 'Done.'