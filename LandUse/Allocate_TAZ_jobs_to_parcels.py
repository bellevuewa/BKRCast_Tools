import pandas as pd
import os, copy
import numpy as np
import utility


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

print 'Loading....'
lookup_df = pd.read_csv(lookup_file, sep = ',', low_memory = False)
tazjobs_df = pd.read_csv(os.path.join(working_folder,taz_job_files), sep = ',', low_memory = False)
subarea_df = pd.read_csv(subarea_file, sep = ',')
baseyear_lu_df = pd.read_csv(base_year_parcel_file, sep = ' ', low_memory = False)

# aggregate jobs to TAZ level, rename attribute names and join with base year.
baseyear_tazjobs_df = baseyear_lu_df.groupby('TAZ_P').sum()[parcel_jobs_columns_List_full]
baseyear_tazjobs_df.rename(columns = lambda x: x.replace('_P', '_T_B'), inplace = True)
baseyear_lu_df = pd.merge(baseyear_lu_df, baseyear_tazjobs_df, left_on = 'TAZ_P', right_index = True, how = 'inner' )
updated_parcel_lu_df = pd.merge(baseyear_lu_df, tazjobs_df, left_on = 'TAZ_P', right_on = 'BKRCastTAZ', how = 'inner' )

for cat in parcel_jobs_columns_List_full:
    updated_parcel_lu_df[cat.replace('_P', '_P_B')] = updated_parcel_lu_df[cat]

# retrieve the taz list from parcels. It guarantees a TAZ includes at least one parcel. 
tazlist = sorted(updated_parcel_lu_df['BKRCastTAZ'].unique())

# proportionally allocate future jobs based on existing condition job distribution and TAZ level sum
for cat in parcel_jobs_columns_List:
    base_taz_jobcat_name = cat.replace('_P', '_T_B')
    future_taz_jobcat_name = cat.replace('_P', '_T')
    
    # base year TAZ jobs > 0 && future year TAZ jobs >=0
    updated_parcel_lu_df.loc[updated_parcel_lu_df[base_taz_jobcat_name]>0, cat] = updated_parcel_lu_df.loc[updated_parcel_lu_df[base_taz_jobcat_name]>0, cat] * (updated_parcel_lu_df.loc[updated_parcel_lu_df[base_taz_jobcat_name]>0, future_taz_jobcat_name] * 1.0 / updated_parcel_lu_df.loc[updated_parcel_lu_df[base_taz_jobcat_name]>0, base_taz_jobcat_name])
    updated_parcel_lu_df.loc[updated_parcel_lu_df[base_taz_jobcat_name]>0, cat] = updated_parcel_lu_df.loc[updated_parcel_lu_df[base_taz_jobcat_name]>0, cat].round(0)
    # base year TAZ jobs == 0 && future year TAZ jobs == 0
    b0f0 = (updated_parcel_lu_df[base_taz_jobcat_name] == 0) & (updated_parcel_lu_df[future_taz_jobcat_name] == 0) 
    updated_parcel_lu_df.loc[b0f0, cat] = 0

# base year TAZ jobs == 0 && future year TAZ jobs >0. this is a little complicated case..Will need to go over each TAZ
# to make the program more efficient, put the taz loop the outside loop.
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
                print 'taz ' + str(taz) + ': randomly draw ' + str(int(future_taz_cat_jobs)) + ' parcels for ' + cat
            else:
                updated_parcel_lu_df.loc[selected_parcels,cat] = updated_parcel_lu_df.loc[selected_parcels,future_taz_jobcat_name] / numparcels
                updated_parcel_lu_df.loc[selected_parcels,cat] = updated_parcel_lu_df.loc[selected_parcels,cat].round(0)
                # some jobs are lost due to rounding down. add the missing jobs back to one parcel.
                lostjobs = future_taz_cat_jobs - updated_parcel_lu_df.loc[selected_parcels,cat].sum()
                if lostjobs > 0:
                    # put the last jobs back
                    sampled_ID = updated_parcel_lu_df[selected_parcels].sample( n =1)['PARCELID']
                    updated_parcel_lu_df.loc[updated_parcel_lu_df['PARCELID'].isin(sampled_ID), cat] += lostjobs
                elif lostjobs < 0:  
                    while (lostjobs < 0):  # more jobs are allocated, need to trim it back.
                        parcels_w_jobs = (updated_parcel_lu_df['BKRCastTAZ'] == taz) & (updated_parcel_lu_df[cat] >= 1)
                        counts = updated_parcel_lu_df.loc[parcels_w_jobs].shape[0]
                        updated_parcel_lu_df.loc[parcels_w_jobs,cat] -= 1
                        lostjobs += counts
                    if lostjobs > 0:
                        # too many jobs are trimed. put them back
                        sampled_ID = updated_parcel_lu_df[parcels_w_jobs].sample( n =1)['PARCELID']
                        updated_parcel_lu_df.loc[updated_parcel_lu_df['PARCELID'].isin(sampled_ID), cat] += lostjobs
                selected_parcels_by_cat = (updated_parcel_lu_df['BKRCastTAZ'] == taz) & (updated_parcel_lu_df[cat] > 0)
                print str(lostjobs) + ' are lost in ' + cat + '. Need to add them back. Future taz jobs ' + str(future_taz_cat_jobs) + ', assigned ' + str(updated_parcel_lu_df.loc[selected_parcels_by_cat,cat].sum())

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
            lostjobs = future_taz_cat_jobs - updated_parcel_lu_df.loc[selected_parcels, cat].sum()
            if lostjobs > 0:
                sampled_ID = updated_parcel_lu_df[selected_parcels].sample( n = 1)['PARCELID']
                updated_parcel_lu_df.loc[updated_parcel_lu_df['PARCELID'].isin(sampled_ID), cat] += lostjobs
            elif lostjobs < 0:
                while (lostjobs < 0):
                    parcels_w_jobs = (updated_parcel_lu_df['BKRCastTAZ'] == taz) & (updated_parcel_lu_df[cat] >= 1)
                    counts = updated_parcel_lu_df.loc[parcels_w_jobs].shape[0]
                    if abs(lostjobs) > counts:
                        sampled_ID = updated_parcel_lu_df[parcels_w_jobs].sample( n = counts)['PARCELID']
                        updated_parcel_lu_df.loc[updated_parcel_lu_df['PARCELID'].isin(sampled_ID), cat] -= 1
                        lostjobs += counts
                    else:
                        sampled_ID = updated_parcel_lu_df[parcels_w_jobs].sample(n = int(abs(lostjobs)))['PARCELID']
                        updated_parcel_lu_df.loc[updated_parcel_lu_df['PARCELID'].isin(sampled_ID), cat] -= 1
                        lostjobs += abs(lostjobs)

#for taz in tazlist:
#    if taz == 26:
#        print 'here you go.'
#    selected_parcels = (updated_parcel_lu_df['BKRCastTAZ'] == taz) & (updated_parcel_lu_df['EMPTOT_T_B'] > 0)
#    for cat in parcel_jobs_columns_List:
#        base_taz_jobcat_name = cat.replace('_P', '_T_B')
#        future_taz_jobcat_name = cat.replace('_P', '_T')
#        if updated_parcel_lu_df.loc[selected_parcels, future_taz_jobcat_name].count() > 0:
#            future_taz_cat_jobs = updated_parcel_lu_df.loc[selected_parcels, future_taz_jobcat_name].iloc[0]
#        else:
#            future_taz_cat_jobs = 0

#        if updated_parcel_lu_df.loc[selected_parcels, base_taz_jobcat_name].count() > 0:
#            base_taz_cat_jobs = updated_parcel_lu_df.loc[selected_parcels, base_taz_jobcat_name].iloc[0]
#        else:
#            base_taz_cat_jobs = 0

#        if future_taz_cat_jobs > 0 and base_taz_cat_jobs > 0:
#            lostjobs = future_taz_cat_jobs - updated_parcel_lu_df.loc[selected_parcels, cat].sum()
#            if lostjobs > 0:
#                sampled_ID = updated_parcel_lu_df[selected_parcels].sample( n = 1)['PARCELID']
#                updated_parcel_lu_df.loc[updated_parcel_lu_df['PARCELID'].isin(sampled_ID), cat] += lostjobs
#            elif lostjobs < 0:
#                while (lostjobs < 0):
#                    parcels_w_jobs = (updated_parcel_lu_df['BKRCastTAZ'] == taz) & (updated_parcel_lu_df[cat] >= 1)
#                    counts = updated_parcel_lu_df.loc[parcels_w_jobs].shape[0]
#                    if abs(lostjobs) > counts:
#                        sampled_ID = updated_parcel_lu_df[parcels_w_jobs].sample( n = counts)['PARCELID']
#                        updated_parcel_lu_df.loc[updated_parcel_lu_df['PARCELID'].isin(sampled_ID), cat] -= 1
#                        lostjobs += counts
#                    else:
#                        sampled_ID = updated_parcel_lu_df[parcels_w_jobs].sample(n = int(abs(lostjobs)))['PARCELID']
#                        updated_parcel_lu_df.loc[updated_parcel_lu_df['PARCELID'].isin(sampled_ID), cat] -= 1
#                        lostjobs += abs(lostjobs)

#    # if future total jobs in taz is zero, set all job cat to zero.
#    if updated_parcel_lu_df.loc[(updated_parcel_lu_df['BKRCastTAZ'] == taz),'EMPTOT_T'].iloc[0] == 0:
#        updated_parcel_lu_df.loc[(updated_parcel_lu_df['BKRCastTAZ'] == taz),parcel_jobs_columns_List_full]  == 0
#        print 'taz ' + str(taz) + ': all jobs are set to zero.'
#    else:
#        selected_parcels = (updated_parcel_lu_df['BKRCastTAZ'] == taz) & (updated_parcel_lu_df['EMPTOT_T'] > 0)
#        # evenly distribute jobs across eligible parcels.
#        if updated_parcel_lu_df.loc[selected_parcels,'EMPTOT_T_B'].iloc[0] == 0:
#            print 'taz ' + str(taz) + ': has ' + str(updated_parcel_lu_df.loc[selected_parcels,'EMPTOT_T'].iloc[0]) + ' jobs in the future year but zero in the base year.'
#            numparcels = updated_parcel_lu_df[selected_parcels].shape[0] 

#            for cat in parcel_jobs_columns_List:
#                base_taz_jobcat_name = cat.replace('_P', '_T_B')
#                future_taz_jobcat_name = cat.replace('_P', '_T')
#                future_taz_cat_jobs = updated_parcel_lu_df.loc[selected_parcels, future_taz_jobcat_name].iloc[0]
#                if future_taz_cat_jobs > 0:
#                    # when future total taz jobs is less than number of eligible parcels (each parcel would have less than one job in average)
#                    # random draw parcels from the eligible parcels such that one job is assigned to one drawn parcel.
#                    if numparcels > future_taz_cat_jobs:
#                        sampled_ID = updated_parcel_lu_df[selected_parcels].sample( n = int(future_taz_cat_jobs))['PARCELID']
#                        updated_parcel_lu_df.loc[updated_parcel_lu_df['PARCELID'].isin(sampled_ID), cat] = 1
#                        print 'taz ' + str(taz) + ': randomly draw ' + str(int(future_taz_cat_jobs)) + ' parcels for ' + cat
#                    else:
#                        updated_parcel_lu_df.loc[selected_parcels,cat] = updated_parcel_lu_df.loc[selected_parcels,future_taz_jobcat_name] / numparcels
#                        updated_parcel_lu_df.loc[selected_parcels,cat] = updated_parcel_lu_df.loc[selected_parcels,cat].round(0)
#                        # some jobs are lost due to rounding down. add the missing jobs back to one parcel.
#                        lostjobs = future_taz_cat_jobs - updated_parcel_lu_df.loc[selected_parcels,cat].sum()
#                        if lostjobs > 0:
#                            # put the last jobs back
#                            sampled_ID = updated_parcel_lu_df[selected_parcels].sample( n =1)['PARCELID']
#                            updated_parcel_lu_df.loc[updated_parcel_lu_df['PARCELID'].isin(sampled_ID), cat] += lostjobs
#                        elif lostjobs < 0:  
#                            while (lostjobs < 0):  # more jobs are allocated, need to trim it back.
#                                parcels_w_jobs = (updated_parcel_lu_df['BKRCastTAZ'] == taz) & (updated_parcel_lu_df[cat] >= 1)
#                                counts = updated_parcel_lu_df.loc[parcels_w_jobs].shape[0]
#                                updated_parcel_lu_df.loc[parcels_w_jobs,cat] -= 1
#                                lostjobs += counts
#                            if lostjobs > 0:
#                                # too many jobs are trimed. put them back
#                                sampled_ID = updated_parcel_lu_df[parcels_w_jobs].sample( n =1)['PARCELID']
#                                updated_parcel_lu_df.loc[updated_parcel_lu_df['PARCELID'].isin(sampled_ID), cat] += lostjobs
#                        selected_parcels_by_cat = (updated_parcel_lu_df['BKRCastTAZ'] == taz) & (updated_parcel_lu_df[cat] > 0)
#                        print str(lostjobs) + ' are lost in ' + cat + '. Need to add them back. Future taz jobs ' + str(future_taz_cat_jobs) + ', assigned ' + str(updated_parcel_lu_df.loc[selected_parcels_by_cat,cat].sum())

updated_parcel_lu_df['EMPTOT_P']  =  updated_parcel_lu_df[parcel_jobs_columns_List].sum(axis = 1)
                    

updated_parcel_lu_df.drop(columns = (x.replace('_P', '_T') for x in parcel_jobs_columns_List_full), inplace = True)
updated_parcel_lu_df.drop(columns = (x.replace('_P', '_T_B') for x in parcel_jobs_columns_List_full), inplace = True)
updated_parcel_lu_df.drop(columns = (x.replace('_P', '_P_B') for x in parcel_jobs_columns_List_full), inplace = True)
updated_parcel_lu_df.drop(columns = ['BKRCastTAZ'], inplace = True)
updated_parcel_lu_df.to_csv(os.path.join(working_folder, parcelized_parcel_file_name), sep = ' ', index = False)
print 'Done.'


