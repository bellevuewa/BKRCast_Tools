import pandas as pd
import os, copy
import numpy as np
import utility

''' 
09/02/2021
   This program is used to allocate TAZ level jobs to each parcel. Sometimes it is too difficult to estimate jobs by parcel 
but is relatively easy to go by TAZ. In general, it proportionally allocates TAZ level jobs based on base year job distribution.
Algorithm:
    foreach TAZ and each job category:
        when base year and future year jobs are all > 0, allocate future TAZ jobs to parcels based on base year's job proportion;
        if future year TAZ jobs are zero, set parcel jobs to zero;
        if base year TAZ job is zero  but future year TAZ jobs > 0
            if total_eligible_parcels > total_future_jobs
                 random select total_future_jobs parcels and assign 1 job to each selected parcel
            else
                evenly distribute jobs over all eligible parcels

Very likely fractional jobs are assigned to a parcel. To make all job numbers integer, first round to integer, then address the
difference between the total jobs and the sum of rounded jobs. The algorithm to address this difference:
    if the difference > 0:
        randomly select one parcel fromt eh eligbile parcels and assign the difference to this selected parcel
    else the difference < 0
        Reduce jobs evenly across all eligible parcels. If too many jobs are removed, put them back to a randomly selected parcel.
        
     
'''

### input files
working_folder = r'Z:\Modeling Group\BKRCast\LandUse\test\2021Concurrency_TAZjobs'
lookup_file = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'
subarea_file = r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\TAZ_subarea.csv"
taz_job_files = r"Z:\Modeling Group\BKRCast\LandUse\test\2021Concurrency_TAZjobs\test_taz_jobs.csv"
base_year_parcel_file =  r"Z:\Modeling Group\BKRCast\LandUse\2020baseyear-BKR\parcels_urbansim.txt"

# output files
parcelized_parcel_file_name = 'reallocated_taz_jobs_to_parcels.txt'

parcel_jobs_columns_List = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPRET_P', 'EMPRSC_P', 'EMPSVC_P', 'EMPOTH_P']
parcel_jobs_columns_List_full = copy.deepcopy(parcel_jobs_columns_List)
parcel_jobs_columns_List_full.append('EMPTOT_P')

def address_fractional_jobs(lostjobs, taz, cat, parcel_lu_df, selected_parcels):
    if lostjobs > 0:
        # positive lostjobs means they need to be added back to that TAZ.
        # to make it simple, add the lostjobs to a randomly selected parcel.
        sampled_ID = parcel_lu_df[selected_parcels].sample( n = 1)['PARCELID']
        parcel_lu_df.loc[parcel_lu_df['PARCELID'].isin(sampled_ID), cat] += lostjobs
    elif lostjobs < 0:
        while (lostjobs < 0):
            # too many jobs are allocated to that TAZ, need to remove them.
            # evely remove jobs from eligible parcels, one job per eligible parcel each iteration 
            parcels_w_jobs = (parcel_lu_df['BKRCastTAZ'] == taz) & (parcel_lu_df[cat] >= 1)
            counts = parcel_lu_df.loc[parcels_w_jobs].shape[0]
            if abs(lostjobs) > counts:
                sampled_ID = parcel_lu_df[parcels_w_jobs].sample( n = counts)['PARCELID']
                parcel_lu_df.loc[parcel_lu_df['PARCELID'].isin(sampled_ID), cat] -= 1
                lostjobs += counts
            else:
                sampled_ID = parcel_lu_df[parcels_w_jobs].sample(n = int(abs(lostjobs)))['PARCELID']
                parcel_lu_df.loc[parcel_lu_df['PARCELID'].isin(sampled_ID), cat] -= 1
                lostjobs += abs(lostjobs)
        if lostjobs > 0:
            # too many jobs are removed. need to put them back
            sampled_ID = parcel_lu_df[parcels_w_jobs].sample( n =1)['PARCELID']
            parcel_lu_df.loc[parcel_lu_df['PARCELID'].isin(sampled_ID), cat] += lostjobs

print 'Loading....'
lookup_df = pd.read_csv(lookup_file, sep = ',', low_memory = False)
tazjobs_df = pd.read_csv(os.path.join(working_folder,taz_job_files), sep = ',', low_memory = False)
subarea_df = pd.read_csv(subarea_file, sep = ',')
baseyear_lu_df = pd.read_csv(base_year_parcel_file, sep = ' ', low_memory = False)

# aggregate base year jobs to TAZ level, rename attribute names and join with base year.
baseyear_tazjobs_df = baseyear_lu_df.groupby('TAZ_P').sum()[parcel_jobs_columns_List_full]
baseyear_tazjobs_df.rename(columns = lambda x: x.replace('_P', '_T_B'), inplace = True)
baseyear_lu_df = pd.merge(baseyear_lu_df, baseyear_tazjobs_df, left_on = 'TAZ_P', right_index = True, how = 'inner' )
updated_parcel_lu_df = pd.merge(baseyear_lu_df, tazjobs_df, left_on = 'TAZ_P', right_on = 'BKRCastTAZ', how = 'inner' )

# rename the attribute with an '_B' attached B is for base year.
for cat in parcel_jobs_columns_List_full:
    updated_parcel_lu_df[cat.replace('_P', '_P_B')] = updated_parcel_lu_df[cat]
                                                        
# retrieve the taz list from parcels. It guarantees a TAZ includes at least one parcel. 
tazlist = sorted(updated_parcel_lu_df['BKRCastTAZ'].unique())

# proportionally allocate future jobs based on existing condition job distribution and TAZ level sum
# the allocated jobs are rounded to integer. After adding up to, they probably do not equal TAZ control total. The difference will 
# be addressed later.
for cat in parcel_jobs_columns_List:
    base_taz_jobcat_name = cat.replace('_P', '_T_B')
    future_taz_jobcat_name = cat.replace('_P', '_T')
    
    # base year TAZ jobs > 0 && future year TAZ jobs >=0
    updated_parcel_lu_df.loc[updated_parcel_lu_df[base_taz_jobcat_name]>0, cat] = updated_parcel_lu_df.loc[updated_parcel_lu_df[base_taz_jobcat_name]>0, cat] * (updated_parcel_lu_df.loc[updated_parcel_lu_df[base_taz_jobcat_name]>0, future_taz_jobcat_name] * 1.0 / updated_parcel_lu_df.loc[updated_parcel_lu_df[base_taz_jobcat_name]>0, base_taz_jobcat_name])
    updated_parcel_lu_df.loc[updated_parcel_lu_df[base_taz_jobcat_name]>0, cat] = updated_parcel_lu_df.loc[updated_parcel_lu_df[base_taz_jobcat_name]>0, cat].round(0)
    # base year TAZ jobs == 0 && future year TAZ jobs == 0
    b0f0 = (updated_parcel_lu_df[base_taz_jobcat_name] == 0) & (updated_parcel_lu_df[future_taz_jobcat_name] == 0) 
    updated_parcel_lu_df.loc[b0f0, cat] = 0

# base year TAZ jobs == 0 && future year TAZ jobs >0. this is a little complicated case..Will need to loop over each TAZ. 
# to make the program more efficient, make the taz loop the outside loop.
# if a job category has zero jobs in a taz in base year but it has jobs in future year, allocation by proportion will not work
# for this scenario. In this case, we evenly distribute jobs over all eligible parcels (base year total jobs > 0)
for taz in tazlist: 
    for cat in parcel_jobs_columns_List:
        base_taz_jobcat_name = cat.replace('_P', '_T_B')
        future_taz_jobcat_name = cat.replace('_P', '_T')
        selected_parcels= (updated_parcel_lu_df['BKRCastTAZ'] == taz) & (updated_parcel_lu_df[base_taz_jobcat_name] == 0) & (updated_parcel_lu_df[future_taz_jobcat_name] > 0)
        if updated_parcel_lu_df.loc[selected_parcels, future_taz_jobcat_name].count() > 0:
            future_taz_cat_jobs = updated_parcel_lu_df.loc[selected_parcels, future_taz_jobcat_name].iloc[0]
        else:
            future_taz_cat_jobs = 0

        if updated_parcel_lu_df.loc[selected_parcels, base_taz_jobcat_name].count() > 0:
            base_taz_cat_jobs = updated_parcel_lu_df.loc[selected_parcels, base_taz_jobcat_name].iloc[0]
        else:
            base_taz_cat_jobs = 0

        if (base_taz_cat_jobs == 0 and future_taz_cat_jobs > 0):
            print 'TAZ ' + str(taz) + ' '+ cat +' has '+ str(future_taz_cat_jobs) +' in future year but zero jobs in base year'
            numparcels = updated_parcel_lu_df[selected_parcels].shape[0] 
            if numparcels > future_taz_cat_jobs:
                sampled_ID = updated_parcel_lu_df[selected_parcels].sample( n = int(future_taz_cat_jobs))['PARCELID']
                updated_parcel_lu_df.loc[updated_parcel_lu_df['PARCELID'].isin(sampled_ID), cat] = 1
                print 'TAZ ' + str(taz) + ': randomly draw ' + str(int(future_taz_cat_jobs)) + ' parcels for ' + cat
            else:
                updated_parcel_lu_df.loc[selected_parcels,cat] = updated_parcel_lu_df.loc[selected_parcels,future_taz_jobcat_name] / numparcels
                updated_parcel_lu_df.loc[selected_parcels,cat] = updated_parcel_lu_df.loc[selected_parcels,cat].round(0)
                # some jobs are lost due to rounding down. add the missing jobs back to one parcel.
                lostjobs = future_taz_cat_jobs - updated_parcel_lu_df.loc[selected_parcels,cat].sum()
                address_fractional_jobs(lostjobs, taz, cat, updated_parcel_lu_df, selected_parcels)
                selected_parcels_by_cat = (updated_parcel_lu_df['BKRCastTAZ'] == taz) & (updated_parcel_lu_df[cat] > 0)
                print str(lostjobs) + ' are lost in ' + cat + '. Need to add them back. Future taz jobs ' + str(future_taz_cat_jobs) + ', assigned ' + str(updated_parcel_lu_df.loc[selected_parcels_by_cat,cat].sum())

        # below are addressing the fractional jobs in base_taz_jobs > 0 and future_taz_jobs > 0.
        selected_parcels= (updated_parcel_lu_df['BKRCastTAZ'] == taz) & (updated_parcel_lu_df[base_taz_jobcat_name] > 0) & (updated_parcel_lu_df[future_taz_jobcat_name] > 0)
        if updated_parcel_lu_df.loc[selected_parcels, future_taz_jobcat_name].count() > 0:
            future_taz_cat_jobs = updated_parcel_lu_df.loc[selected_parcels, future_taz_jobcat_name].iloc[0]
        else:
            future_taz_cat_jobs = 0

        if updated_parcel_lu_df.loc[selected_parcels, base_taz_jobcat_name].count() > 0:
            base_taz_cat_jobs = updated_parcel_lu_df.loc[selected_parcels, base_taz_jobcat_name].iloc[0]
        else:
            base_taz_cat_jobs = 0

        if (future_taz_cat_jobs > 0) and (base_taz_cat_jobs > 0):
            print 'TAZ ' + str(taz) + ": " + cat + " have jobs in both base and future year. Addressing fractional jobs. "
            lostjobs = future_taz_cat_jobs - updated_parcel_lu_df.loc[selected_parcels, cat].sum()
            address_fractional_jobs(lostjobs, taz, cat, updated_parcel_lu_df, selected_parcels)

print 'Exporting...'
updated_parcel_lu_df['EMPTOT_P']  =  updated_parcel_lu_df[parcel_jobs_columns_List].sum(axis = 1)
updated_parcel_lu_df.drop(columns = (x.replace('_P', '_T') for x in parcel_jobs_columns_List_full), inplace = True)
updated_parcel_lu_df.drop(columns = (x.replace('_P', '_T_B') for x in parcel_jobs_columns_List_full), inplace = True)
updated_parcel_lu_df.drop(columns = (x.replace('_P', '_P_B') for x in parcel_jobs_columns_List_full), inplace = True)
updated_parcel_lu_df.drop(columns = ['BKRCastTAZ'], inplace = True)
updated_parcel_lu_df.to_csv(os.path.join(working_folder, parcelized_parcel_file_name), sep = ' ', index = False)
print 'Done.'


