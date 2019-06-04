'''
   Adjust HH and persons proportionally by control total of ***HHs*** in TAZ level and export a new set of HH and 
   persons. 
   This program is used to generate a set of new sythetic hhs based on an existing synthetic population and a new set of total
   households (control totals) by TAZ.
   It splits TAZs into two lists, one with number of hhs higher than control total and the other lower than control
   total. For each taz in the former list, move randomly picked households from a TAZ to the 
   pool_for_selection until reaching the control total.
   for the latter list, add households randomly selected from pool_for_selection to a TAZ until reaching the control total. 
   If the pool is depleted, randomly draw samples from the household list to reach the control total. Once reached, household members in person list are
   duplicated with new household id.
   5/31/19.
'''

import pandas as pd
import h5py
import numpy as np
import random 
import os
import sys

# new files go here
output_dir = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\SytheticPopulationTest'   
# input hh and persons file:
hdf_file = h5py.File(r'D:\2035BKRCastBaseline\2035BKRCastBaseline\inputs\hh_and_persons.h5', "r")
TAZ_Subarea_File_Name = r'Z:\Modeling Group\BKRCast\Job Conversion Test\TAZ_subarea.csv'
control_total_file_name = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\SytheticPopulationTest\household_controls_by_taz.csv'

# output hh and persons file- don't overwrite existing one!
out_h5_file = h5py.File(os.path.join(output_dir, 'hh_and_persons2.h5'), 'w')

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

def df_to_h5(df, h5_store, group_name):
    """
    Stores DataFrame series as indivdual to arrays in an h5 container. 
    """
    # delete store store if exists   
    if group_name in h5_store:
        del h5_store[group_name]
        my_group = h5_store.create_group(group_name)
        print "Group " + group_name + " Exists. Group deleted then created"
        #If not there, create the group
    else:
        my_group = h5_store.create_group(group_name)
        print "Group " + group_name + " Created"
    for col in df.columns:
        h5_store[group_name].create_dataset(col, data=df[col].values.astype('int32'))

def generateHHs(hhdf, popdf, taz, number):
    '''
        This function generates number of households in a taz, by randomly sampling hhdf. Once households are generated,
        their household members are duplicated with new household id.
        if more households than a taz has to offer is requested, it would sample hhdf with replacement. Otherwise sampled households
        are not replaced.
    '''
    hhs_taz = hhdf[hh_df['hhtaz'] == taz]    
    totalHhs = hhs_taz['hhexpfac'].sum()
    replacement = False
    if totalHhs < number:
        replacement = True
    newhhs = hhs_taz.sample(n = number, replace = replacement)
    oldHHsID = newhhs['hhno'].copy()
    maxhhno = max(hhdf['hhno'])
    nextID = maxhhno + 1

    newpop = pd.DataFrame()
    if replacement == True:
        newhhs = pd.DataFrame()

    for hhid in oldHHsID:
        pop = popdf[popdf['hhno'] == hhid]
        pop.loc[:, 'hhno'] = nextID
        newpop = newpop.append(pop)
        if replacement == False:
            newhhs.loc[newhhs['hhno'] == hhid, 'hhno'] = nextID
        else:
            # when sampling with replacement, newhhs would include duplicated hhs. Each duplicated hhs would have to 
            # convert to a new households with different hhid.
            hhs = hhdf[hhdf['hhno'] == hhid]
            hhs.loc[hhs['hhno'] == hhid, 'hhno'] = nextID
            newhhs = newhhs.append(hhs)
        nextID = nextID + 1

    return newhhs, newpop


person_df = h5_to_df(hdf_file, 'Person')
hh_df = h5_to_df(hdf_file, 'Household')
print 'Original total {0:5.0f} households, {1:5.0f} persons {2:5.0f}'.format(hh_df['hhexpfac'].sum(), person_df['psexpfac'].sum(), hh_df['hhsize'].sum())

existing_hh_df = hh_df.groupby('hhtaz')['hhexpfac', 'hhsize'].sum()
control_total_df = pd.DataFrame.from_csv(control_total_file_name, sep = ',', index_col = 'TAZ') 
joined_df = existing_hh_df.join(control_total_df, how = 'left' )
# sort the hh growth by ascending order. 
joined_df['hhincrease'] = joined_df['Sum_Households'] - joined_df['hhexpfac']
joined_df.sort_values(by = ['hhincrease'], inplace = True)
taz_list = joined_df.index

total_hhs_taz = hh_df.groupby('hhtaz')['hhexpfac'].sum()

taz_list_1 = [] # list of tazs that need increase hhs (move in hhs from the pool, when the pool is depleted, randomly duplicate households from the available hhs list)
taz_list_2 = [] # list of tazs that has surplus households. Need to remove some of them to the pool.

for taz in taz_list:
    ### random pick person records to match control total in this taz
    control_total_taz = control_total_df['Sum_Households'].loc[taz]
    if control_total_taz <= total_hhs_taz[taz]:
        taz_list_2.append(taz)
    else:
        taz_list_1.append(taz)

pool_for_selection = pd.DataFrame()

# if household target in a taz is less than what it currently has, randomly 
# pick and remove some households to a pool to reach its target.
for taz in taz_list_2:
    control_total_taz = control_total_df['Sum_Households'].loc[taz]       
    hhs_have = total_hhs_taz[taz]
    #randomly select hh and remove it from hhs and persons
    if (control_total_taz > hhs_have):
        print 'Error. TAZ {0:d} should not have less households than control total.'.format(taz)
        sys.exit(-1)

    surplus_hhs = hh_df[hh_df['hhtaz'] == taz].sample(n = hhs_have - control_total_taz)
    pool_for_selection = pool_for_selection.append(surplus_hhs)
    hh_df = hh_df[~hh_df['hhno'].isin(surplus_hhs['hhno'])]
    print "Move {0:5.0f} households from TAZ {1:d} to the pool.".format(surplus_hhs.shape[0], taz)
    

print 'Total {0:5.0f} households, {1:5.0f} persons {2:5.0f}'.format(hh_df['hhexpfac'].sum(), person_df['psexpfac'].sum(), hh_df['hhsize'].sum())

tot_reloc_hhs = 0
tot_newadded_hhs = 0
tot_newadded_persons = 0

for taz in taz_list_1:
    control_total_taz = control_total_df['Sum_Households'].loc[taz]
    hhs_have = total_hhs_taz[taz]
    # relocate hhs from other taz
    hhs_needed = control_total_taz - hhs_have
    if hhs_needed < 0:
        print 'Error. TAZ {0:d} should not have more households than control total.'.format(taz)
        sys.exit(-1)

    if len(pool_for_selection.index) >= hhs_needed:
        ## move hhs from the pool to this taz
        hhs_got = pool_for_selection.sample(n = hhs_needed)
        pool_for_selection = pool_for_selection[~pool_for_selection['hhno'].isin(hhs_got['hhno'])]
        hhs_got.loc[:, 'hhtaz'] = taz
        hh_df = hh_df.append(hhs_got)
        newadded_hhs = hhs_got['hhexpfac'].sum()
        tot_reloc_hhs = tot_reloc_hhs + newadded_hhs
        print 'Move {0:5.0f} households from the pool to TAZ {1:d}.'.format(newadded_hhs, taz)
    elif (len(pool_for_selection.index) > 0) and (len(pool_for_selection.index) < hhs_needed):
        hhs_got = pool_for_selection.sample(n = len(pool_for_selection.index))
        pool_for_selection = pool_for_selection[~pool_for_selection['hhno'].isin(hhs_got['hhno'])]
        hhs_got.loc[:, 'hhtaz'] = taz
        hh_df = hh_df.append(hhs_got)  
        hhs_short = hhs_needed - len(hhs_got.index)
        newhhs, newpop = generateHHs(hh_df, person_df, taz, hhs_short)
        hh_df = hh_df.append(newhhs)
        person_df = person_df.append(newpop)
        tot_reloc_hhs = tot_reloc_hhs + hhs_got['hhexpfac'].sum()
        tot_newadded_hhs = tot_newadded_hhs + newhhs['hhexpfac'].sum()
        tot_newadded_persons = tot_newadded_persons + newpop['psexpfac'].sum()
        print 'Move {0:5.0f} households from the pool to TAZ {1:d}.'.format(hhs_got.shape[0], taz)
        print 'Generate {0:5.0f} households for the TAZ {1:d}.'.format(hhs_short, taz)
    else:
        # pool is depleted
        newhhs, newpop = generateHHs(hh_df, person_df, taz, hhs_needed)
        hh_df = hh_df.append(newhhs)
        person_df = person_df.append(newpop)
        newadded_hhs = newhhs['hhexpfac'].sum()
        newadded_persons = newpop['psexpfac'].sum()
        tot_newadded_hhs = tot_newadded_hhs + newadded_hhs
        tot_newadded_persons = tot_newadded_persons + newadded_persons
        if (person_df[~person_df['hhno'].isin(hh_df['hhno'])].empty == False):
            test = 0
            print 'TAZ ', taz 
        print 'pool is depleted. Generate {0:5.0f} households {1:5.0f} persons for the TAZ {2:d} {3:5.0f}.'.format(newadded_hhs, newpop['psexpfac'].sum(), taz, newhhs['hhsize'].sum())
            
print '{0:5.0f} households were relocated. {1:5.0f} households, {2:5.0f} persons were generated.'.format(tot_reloc_hhs, tot_newadded_hhs, tot_newadded_persons)     

df_to_h5(hh_df, out_h5_file, 'Household')
df_to_h5(person_df, out_h5_file, 'Person')
out_h5_file.close()