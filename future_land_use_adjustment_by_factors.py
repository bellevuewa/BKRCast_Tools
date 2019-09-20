import pandas as pd
import h5py
import numpy as np
import random 
import os

###
### This program is used to adjust number of jobs in a parcel file with a subarea level adjustment factors.
Input_parcel_file = r'Z:\Modeling Group\BKRCast\2035Parcel_Sqft_based\BKRCastFuture_parcels_urbansim_test.txt'
Subarea_adjustment_factor_file = r'Z:\Modeling Group\BKRCast\2035Parcel_Sqft_based\2035COBJobAdjustment.csv'
Subarea_taz_file = r'Z:\Modeling Group\BKRCast\Job Conversion Test\parcel_level\test11\TAZ_subarea.csv'
Output_parcel_file = r'Z:\Modeling Group\BKRCast\2035Parcel_Sqft_based\BKRCastFuture_adjusted_parcels_urbansim_test.txt'

print 'Loading ... '     
Input_parcel_df = pd.DataFrame.from_csv(Input_parcel_file, sep = ' ')
Subarea_taz_df = pd.DataFrame.from_csv(Subarea_taz_file)
Subarea_adjustment_factors_df = pd.DataFrame.from_csv(Subarea_adjustment_factor_file)
Input_parcel_df.reset_index(inplace = True)
Subarea_taz_df.reset_index(inplace = True)
Subarea_adjustment_factors_df.reset_index(inplace = True)
Subarea_taz_df = Subarea_taz_df[['Subarea', 'TAZNUM']]
Subarea_adjustment_factors_df = Subarea_adjustment_factors_df[['Subarea', 'Ofc_fac', 'Ret_fac', 'Ind_fac', 'Tot_fac']]
Factors_by_parcels_df = Subarea_taz_df.merge(Subarea_adjustment_factors_df, left_on = 'Subarea', right_on = 'Subarea', how = 'left')
parcels = Input_parcel_df[['PARCELID', 'TAZ_P']]
Factors_by_parcels_df = parcels.merge(Factors_by_parcels_df, left_on = 'TAZ_P', right_on = 'TAZNUM', how = 'left')
Factors_by_parcels_df.fillna(1, inplace = True)
Factors_by_parcels_df.drop(['Subarea', 'TAZ_P', 'TAZNUM'], axis = 1, inplace = True)
OutputParcels = Input_parcel_df.merge(Factors_by_parcels_df, left_on = 'PARCELID', right_on = 'PARCELID', how = 'left')
OutputParcels.set_index('PARCELID', inplace = True)
Factors_by_parcels_df.set_index('PARCELID', inplace = True)

OutputParcels['EMPEDU_P'] =  OutputParcels['EMPEDU_P'] * OutputParcels['Tot_fac']
OutputParcels['EMPFOO_P'] =  OutputParcels['EMPFOO_P'] * OutputParcels['Ret_fac']
OutputParcels['EMPGOV_P'] =  OutputParcels['EMPGOV_P'] * OutputParcels['Ofc_fac']
OutputParcels['EMPIND_P'] =  OutputParcels['EMPIND_P'] * OutputParcels['Ind_fac']
OutputParcels['EMPMED_P'] =  OutputParcels['EMPMED_P'] * OutputParcels['Ofc_fac']
OutputParcels['EMPOFC_P'] =  OutputParcels['EMPOFC_P'] * OutputParcels['Ofc_fac']
OutputParcels['EMPOTH_P'] =  OutputParcels['EMPOTH_P'] * OutputParcels['Tot_fac']
OutputParcels['EMPRET_P'] =  OutputParcels['EMPRET_P'] * OutputParcels['Ret_fac']
OutputParcels['EMPRSC_P'] =  OutputParcels['EMPRSC_P'] * OutputParcels['Ret_fac']
OutputParcels['EMPSVC_P'] =  OutputParcels['EMPSVC_P'] * OutputParcels['Tot_fac']
OutputParcels['EMPTOT_P'] = OutputParcels['EMPEDU_P'] + OutputParcels['EMPFOO_P'] + OutputParcels['EMPGOV_P'] + OutputParcels['EMPIND_P'] +  \
    OutputParcels['EMPMED_P'] + OutputParcels['EMPOFC_P'] + OutputParcels['EMPOTH_P'] + OutputParcels['EMPRET_P'] + OutputParcels['EMPRSC_P'] + \
    OutputParcels['EMPSVC_P']

print 'Exporting ...'
OutputParcels.drop(Factors_by_parcels_df.columns, axis = 1, inplace = True)
OutputParcels.to_csv(Output_parcel_file, sep = ' ')

print 'Done'