import pandas as pd
import numpy as np
import os
import math
import h5py
import utility

'''
This program takes synthetic households and synthetic persons( from PopulationSim),
and allocates them to parcels. It also reformats household and person data columns to match
BKRCast input requirement. The output, h5_file_name, can be directly loaded into BKRCast.

Number of households per parcel in synthetic population should be consistent with the parcel file. It can be done by calling 
sync_population_parcel.py. 

Two algorithms are available for allocating households to parcels. If no housing units estimate from local jurisdiction, 
use assign_hhs_to_parcels_by_blkgrp(), otherwise use assign_hhs_parcels_by_local_estimate().

Date: 7/1/2019

9/12/2019
fixed a minor bug that could drop some households under certain occasions. 
Will export a list of blockgroups if their new generated number of households do not match control totals.
Always check the error file to make sure all households are allocated.

3/9/2022
upgrade to python 3.7
'''


###############Start of configuration
working_folder = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\TFP\2033_horizon_year_old_approach'
synthetic_households_file_name = '2033TFP_synthetic_households.csv'
synthetic_population_file_name = '2033TFP_synthetic_persons.csv'
parcel_filename = 'I:/psrcpopsim/popsimproject/parcelize/parcel_TAZ_2014_lookup.csv'

# dwelling units per parcel
new_local_estimated_file_name = r'2033TFP_COB_du.csv'
block_group_list_for_local_estimate_name = r'Local_estimate_choice.csv' 
# number of hhs per parcel
parcels_for_allocation_filename = r'2033_horizon_TFP_parcels_for_allocation_local_estimate.csv'

## output
updated_hhs_file_name = 'updated_2033TFP_synthetic_households.csv'
updated_persons_file_name = 'updated_2033TFP_synthetic_persons.csv'
h5_file_name = '2033TFP_hh_and_persons.h5'
error_file_name = 'error.txt'


sf_occupancy_rate = 0.952  # from Gwen
mf_occupancy_rate = 0.895  # from Gwen
avg_persons_per_sfhh =  2.82 # from Gwen
avg_persons_per_mfhh =  2.03 # from Gwen

############## End of configuration

def assign_hhs_to_parcels_by_blkgrp(hhs_control, hhs_blkgrp_df, parcel_df, blkgrpid):
    '''
        Households by blockgroup (from populationsim) are derived from source other than local estimate. The source are: 
            * OFM SAEP program
            * PSRC's synthetic population hhs_and_persons.h5
        In general, households are assigned to parcels in proportion to 2014 housing units.

        Household allocation method (by priority):
        1. one household goes to each single family parcel. 
        2. the remainning households go to multiple family parcel following the proportion given in parcel data.
        3. the remaining hhs go to mixed use parcels and vacant parcels.
        4. Make SF parcels to MF 
        5. evenly assign remaining to all parcels.

        hhs_control: the forecast number of households in a block group 
        hhs_blkgrp_df: forecast households dataframe (in a blockgroup)
        parcel_df: parcel dataframe. (all parcels) 
        blkgrp: block group id
    '''
    parcels = parcel_df.loc[parcel_df['GEOID10'] == blcgrpid]
    if parcels.shape[0] == 0:
        print('blockgroup ', blcgrpid, ' has no parcels')
        return None
    
    # assign SF first
    sfparcelids = parcels.loc[parcels['LUTYPE_P'] == 24]
    numSFparcels = sfparcelids.shape[0]
    
    updatedHHs = pd.DataFrame()
    sfHhs = pd.DataFrame()
    mfparcels = pd.DataFrame()

    if hhs_control <= numSFparcels: 
        # all households are assigned to SF parcels       
        sfHhs = hhs_blkgrp_df.sample(n = int(hhs_control))
        sfparcels = sfparcelids.sample(n = int(hhs_control))
        for i in range(int(hhs_control)):
            sfHhs['hhparcel'].iat[i] = sfparcels['PSRC_ID'].iat[i]
        updatedHHs = updatedHHs.append(sfHhs)
        return updatedHHs
    
    # if hhs_control > numSFparcels. assign one hh to each SF parcel   
    sfHhs = hhs_blkgrp_df.sample(n = int(numSFparcels))
    for i in range(int(numSFparcels)):
        sfHhs['hhparcel'].iat[i] = sfparcelids['PSRC_ID'].iat[i]
    updatedHHs = updatedHHs.append(sfHhs)  
    
    # leftover hhs and number of leftover  
    remainHhs_df = hhs_blkgrp_df[~hhs_blkgrp_df['household_id'].isin(sfHhs['household_id'])]
    remainHhs = remainHhs_df['hhexpfac'].sum()

    ### below is to decide how many hhs  should be assigned to what parcels. save the planned number of hhs
    ### to forecastedHhs attribute. The numbers are rough because they are fractions. 
    # existing (has mf in 2014) multi-family parcels have higher priority to receive mf hhs.
    # eligible parcels is named mfparcels.
    sum = parcels.loc[parcels['HousingUnits2014'] >= 2, 'HousingUnits2014'].sum()
    if sum > 0: # has existing mf parcels
        mfparcels = parcels.loc[parcels['HousingUnits2014'] >= 2] 
        mfparcels.loc[:, 'forecastedHhs'] = remainHhs * 1.0 * mfparcels['HousingUnits2014'] / sum
    else:
        # no existing mf parcels. use mixed land use (LUTYPE 30) or vacant land (LUTYPE 26)
        mfparcels = parcels.loc[(parcels['LUTYPE_P'] == 26) | (parcels['LUTYPE_P'] == 30)]
        if mfparcels.shape[0] > 0:
            # has mixed use or vacant land, evenly distribute leftover to these parcels.
            mfparcels.loc[:, 'forecastedHhs'] = remainHhs * 1.0 / mfparcels.shape[0]
        else:
            # neither mixed use nor vacant land is available. 
            # evenly distribute to SF parcels (make them MF)
            if numSFparcels > 0:
                mfparcels = sfparcelids
                mfparcels.loc[:, 'forecastedHhs'] = remainHhs * 1.0 / numSFparcels
            else:
                # otherwise, evely distribute hhs to all parcels.
                mfparcels = parcels
                n = parcels.shape[0]
                mfparcels.loc[:, 'forecastedHhs'] = remainHhs / (n * 1.0) 
                

    # decide in each mf parcel, how many hhs (in integer) should be allocated  Eventually planned (forecasedHhs) should equal leftover(remainHhs)
    if (remainHhs >= mfparcels['forecastedHhs'].apply(np.floor).sum()):
        #
        # Round forecastedHhs to integer. Arbitrarily put the difference to the first parcel of the elligible parcel dataframe 
        mfparcels.loc[:, 'forecastedHhs'] = mfparcels['forecastedHhs'].apply(np.floor)
        diff = remainHhs - mfparcels['forecastedHhs'].sum()
        mfparcels.loc[:, 'forecastedHhs'].iat[0] = mfparcels['forecastedHhs'].iat[0] + diff  
    else:
        # total planned-hhs is more than leftover. Evenly distribute leftover to eligible parcels.
        tot_mfparcels = mfparcels.shape[0]
        iat = 0
        while (remainHhs > 0):
            index = iat % tot_mfparcels
            mfparcels.loc[:, 'forecastedHhs'].iat[index] = mfparcels['forecastedHhs'].iat[index] + 1 
            remainHhs = remainHhs - 1
            iat = iat + 1
   
    # now planned hhs == leftover, random draw planned number of hhs from the pool and assign them to the parcel. 
    mfhhs = pd.DataFrame()    
    for row in mfparcels.itertuples():
        if remainHhs_df.shape[0] == 0:
            break
        if row.forecastedHhs <= remainHhs_df.shape[0]:
            mfhhs = remainHhs_df.sample(n = int(row.forecastedHhs))
        else:
            mfhhs = remainHhs_df.sample(n = remainHhs_df.shape[0])
        remainHhs_df = remainHhs_df[~remainHhs_df['household_id'].isin(mfhhs['household_id'])]   
        mfhhs.loc[:, 'hhparcel'] = row.PSRC_ID
        updatedHHs = updatedHHs.append(mfhhs)
    return updatedHHs   

def assign_hhs_parcels_by_local_estimate(hhs_control, hhs_blkgrp_df, parcel_df, blcgrpid, new_local_estimate_df, parcels_available_for_alloc):
    '''
        assign households to parcels by matching local estimate. Housing units by block group are replaced by local estimate.  
        Local estimate knows exactly where the SFUnits and MFunits are located and how many. Therefore the algorithm is different from assign_hhs_to_parcels_by_blkgrp()           
            1. Fill all SFUnits with single families (from synthetic households)
                if necesssary, use multi-families to fill SFUnits.
            2. Fill MFunits with multi-families (from synthetic households). 
        
        hhs_control: the forecast number of households in a block group
        hhs_blkgrp_df: forecast households dataframe (in a blockgroup)
        parcel_df: parcel dataframe (all parcels)
        blcgrpid: block group id
        new_local_estimate_df:
        parcels_available_for_alloc:
    '''

    parcels = parcel_df.loc[parcel_df['GEOID10'] == blcgrpid]
    if parcels.shape[0] == 0:
        print('blockgroup ', blcgrpid, ' has no parcels')
        return None
    
    # assign SF first
    # local estimate decides that which parcel has SF units. So all SFUnits will be filled, to the max extent.
    sfparcels_df = new_local_estimate_df.loc[new_local_estimate_df['SFUnits'] == 1]
    numSFparcels = sfparcels_df.shape[0]
    mfparcels_df = parcels_available_for_alloc.loc[~parcels_available_for_alloc['PSRC_ID'].isin(sfparcels_df['PSRC_ID'])]    
    
    updatedHHs = pd.DataFrame()
    sfHhs = pd.DataFrame()
    sfhhs_blkgrp_df = hhs_blkgrp_df.loc[(hhs_blkgrp_df['hrestype'] == 2) | (hhs_blkgrp_df['hrestype'] == 3)]
    sfhhs_sum = sfhhs_blkgrp_df['hhexpfac'].sum()
    mfhhs_blkgrp_df = hhs_blkgrp_df.loc[~((hhs_blkgrp_df['hrestype'] == 2) | (hhs_blkgrp_df['hrestype'] == 3))]
    mfhhs_sum = mfhhs_blkgrp_df['hhexpfac'].sum()

    #always to fill SF parcels first
    if (sfhhs_sum > 0 and (sfhhs_sum >= numSFparcels)):        
        # assign SF to SF parcels.
        sfHhs = sfhhs_blkgrp_df.sample(n = int(numSFparcels))
        for i in range(int(numSFparcels)):
            sfHhs['hhparcel'].iat[i] = sfparcels_df['PSRC_ID'].iat[i]
        updatedHHs = updatedHHs.append(sfHhs)
        # leftover of SF are moved to mf dataframe 
        sfhhs_blkgrp_df = sfhhs_blkgrp_df.loc[~sfhhs_blkgrp_df['household_id'].isin(sfHhs['household_id'])]
        mfhhs_blkgrp_df = mfhhs_blkgrp_df.append(sfhhs_blkgrp_df)
    else:
        # sfHhs_sum < numSFparcels
        # try to assign hhs to SFUnits as many as possible. If SFUnits (SF parcels) are more than SF (from the synthetic hhs), 
        # MF will be used to fill SFUnits.
        diff = numSFparcels - sfhhs_sum
        if (mfhhs_sum >= diff):
            mfhhs = mfhhs_blkgrp_df.sample(n = int(diff))
            combined = pd.concat([sfhhs_blkgrp_df, mfhhs])
            for i in range(int(numSFparcels)):
                combined['hhparcel'].iat[i] = sfparcels_df['PSRC_ID'].iat[i]
        else:
            if mfhhs_blkgrp_df.shape[0] != 0:
                # if MF hhs are still available but not enough to fill SFUnits, use all of them.
                mfhhs = mfhhs_blkgrp_df.sample(n = mfhhs_sum)
                combined = pd.concat([sfhhs_blkgrp_df, mfhhs])
            else:
                # if MF hhs are not available
                combined = pd.concat([sfhhs_blkgrp_df])
            total = int(combined['hhexpfac'].sum())
            for i in range(total):
                combined['hhparcel'].iat[i] = sfparcels_df['PSRC_ID'].iat[i]
        updatedHHs = updatedHHs.append(combined)
        if mfhhs_blkgrp_df.shape[0] != 0:
            mfhhs_blkgrp_df = mfhhs_blkgrp_df[~mfhhs_blkgrp_df['household_id'].isin(mfhhs['household_id'])]

    if mfhhs_blkgrp_df.shape[0] != 0:
        mfhhs_sum = mfhhs_blkgrp_df['hhexpfac'].sum() 
    else:
        mfhhs_sum = 0

    if mfhhs_sum == 0:
        return updatedHHs

    ## Now all SFUnits are occupied by single families. Need to assign mf to eligible parcels.
    mfhhs = pd.DataFrame()
    tot_sum = mfparcels_df['total_hhs'].sum()
    allocated = 0
    mfparcels_df['total_hhs'] = mfparcels_df['total_hhs'].fillna(0)
    for parcel in mfparcels_df.itertuples():
        total_mfhhs = mfhhs_blkgrp_df.shape[0]
        if total_mfhhs <= 0:
            break
        numHhs = parcel.total_hhs
        if numHhs == 0:
            continue
        pid = parcel.PSRC_ID
        proportion = numHhs / (tot_sum * 1.0)
        selected = int(total_mfhhs * proportion)
        allocated = allocated + selected
        mfhhs = mfhhs_blkgrp_df.sample(n = selected)
        mfhhs.loc[:, 'hhparcel'] = pid
        mfhhs_blkgrp_df = mfhhs_blkgrp_df[~mfhhs_blkgrp_df['household_id'].isin(mfhhs['household_id'])]
        updatedHHs = updatedHHs.append(mfhhs)       
    
    if (mfhhs_blkgrp_df.shape[0] > 0):
        mfhhs_blkgrp_df.loc[:, 'hhparcel'] = parcel.PSRC_ID
        updatedHHs = updatedHHs.append(mfhhs_blkgrp_df)   

    return updatedHHs
    
try:
    error_f = open(os.path.join(working_folder, error_file_name), 'w')
except:
    print('Error open cannot be opened. Close it and try it again.')
    exit(-1)

###
parcel_df = pd.read_csv(parcel_filename, low_memory=False) 
GEOID10_with_local_estimate_df = pd.read_csv(os.path.join(working_folder, block_group_list_for_local_estimate_name), low_memory = False)
GEOID10_with_local_estimate = GEOID10_with_local_estimate_df.loc[GEOID10_with_local_estimate_df['Use_Local'] == 'Y', 'GEOID10'].tolist()
hhs_df = pd.read_csv(os.path.join(working_folder, synthetic_households_file_name))
hhs_df['hhparcel'] = 0

# read local estimate 
new_local_estimate_df = pd.read_csv(os.path.join(working_folder, new_local_estimated_file_name), sep = ',')
new_local_estimate_df['SF'] = new_local_estimate_df['SFUnits'] * sf_occupancy_rate
new_local_estimate_df['MF'] = new_local_estimate_df['MFUnits'] * mf_occupancy_rate
new_local_estimate_df['total_hhs'] = new_local_estimate_df['SF'] + new_local_estimate_df['MF']
new_local_estimate_df['total_persons'] =  new_local_estimate_df['SF'] * avg_persons_per_sfhh + new_local_estimate_df['MF'] * avg_persons_per_mfhh
# Some parcels may have same PSRC_ID but different PINs. Stacked parcels. So need to groupby PSRC_ID first.
new_local_estimate_df = new_local_estimate_df.groupby('PSRC_ID')['SF', 'MF', 'SFUnits', 'MFUnits', 'total_hhs', 'total_persons'].sum()
new_local_estimate_df.reset_index(inplace = True)
new_local_estimate_df = new_local_estimate_df.merge(parcel_df[['PSRC_ID', 'GEOID10']], how = 'left', left_on = 'PSRC_ID', right_on = 'PSRC_ID')

parcels_for_allocation = pd.read_csv(os.path.join(working_folder, parcels_for_allocation_filename), sep = ',')

# remove any blockgroup ID is Nan.
all_blcgrp_ids = hhs_df['block_group_id'].unique()
mask = np.isnan(all_blcgrp_ids)
all_blcgrp_ids = all_blcgrp_ids[~mask]

hhs_by_blkgrp = hhs_df.groupby('block_group_id')['hhexpfac', 'hhsize'].sum()

final_hhs_df = pd.DataFrame()
updatedHhs = pd.DataFrame()

id = 0
for blcgrpid in all_blcgrp_ids:
    hhs_new = hhs_by_blkgrp.loc[hhs_by_blkgrp.index == blcgrpid].iloc[0]['hhexpfac']
    if hhs_new > 0:
        #if blcgrpid == 530330238043:
        #    a = 0
        #    continue
            
        hhs_blkgrp_df = hhs_df.loc[hhs_df['block_group_id'] == blcgrpid] 
        if blcgrpid in GEOID10_with_local_estimate:
            local_estimate_df = new_local_estimate_df.loc[new_local_estimate_df['GEOID10'] == blcgrpid]
            parcels_available_for_alloc_df = parcels_for_allocation.loc[parcels_for_allocation['GEOID10'] == blcgrpid, ['PSRC_ID', 'GEOID10', 'total_hhs']]
            updatedHhs = assign_hhs_parcels_by_local_estimate(hhs_new, hhs_blkgrp_df, parcel_df, blcgrpid, local_estimate_df, parcels_available_for_alloc_df)
            if updatedHhs is not None:
                msg = '{4:d}: blocgroup {0:d}: allocated by local estimate. total hhs: {1:.0f},  allocated: {2:d}, local estimate: {3:d}'.format(blcgrpid, hhs_new, updatedHhs['hhexpfac'].sum(), int(local_estimate_df['SF'].sum() + local_estimate_df['MF'].sum()), id) 
                print(msg)
            else: 
                id += 1
                continue
        else:
            updatedHhs = assign_hhs_to_parcels_by_blkgrp(hhs_new, hhs_blkgrp_df, parcel_df, blcgrpid)
            if updatedHhs is not None:
                msg = '{3:d}: blocgroup {0:d}: total hhs: {1:.0f},  allocated: {2:d}'.format(blcgrpid, hhs_new, updatedHhs['hhexpfac'].sum(), id) 
                print(msg)
            else:
                id += 1
                continue
        if (hhs_new != updatedHhs['hhexpfac'].sum()):
            print('******************** blockgroup ', blcgrpid, ': total hhs does not match control total.')           
            error_f.write(msg+'\n')
        final_hhs_df = final_hhs_df.append(updatedHhs) 
        id = id + 1  
        if len(final_hhs_df.loc[final_hhs_df['household_id'].duplicated()]) > 0:
            print('duplicated households found in block group ', blcgrpid)
    else:
        print('no household in block_group ', blcgrpid) 
    
final_hhs_df.rename(columns = {'household_id':'hhno'}, inplace = True)
final_hhs_df = final_hhs_df.merge(parcel_df[['PSRC_ID', 'BKRCastTAZ']], how = 'left', left_on = 'hhparcel', right_on = 'PSRC_ID')
final_hhs_df.rename(columns = {'BKRCastTAZ': 'hhtaz'}, inplace = True)
del final_hhs_df['PSRC_ID']

### process other attributes to match required columns
pop_df = pd.read_csv(os.path.join(working_folder, synthetic_population_file_name)) 
pop_df.rename(columns={'household_id':'hhno'}, inplace = True)
pop_df.rename(columns={'SEX':'pgend'}, inplace = True)
ages=pop_df.pagey
pop_df.sort_values(by = 'hhno', inplace = True)

# -1 pdairy ppaidprk pspcl,pstaz ptpass,puwarrp,puwdepp,puwmode,pwpcl,pwtaz 
# pstyp is covered by pptyp and pwtyp, misssing: puwmode -1 puwdepp -1 puwarrp -1 pwpcl -1 pwtaz -1 ptpass -1  pspcl,pstaz 
# 1 psexpfac 
morecols=pd.DataFrame({'pdairy': [-1]*pop_df.shape[0],'pno': [-1]*pop_df.shape[0],'ppaidprk': [-1]*pop_df.shape[0],'psexpfac': [1]*pop_df.shape[0],
                       'pspcl': [-1]*pop_df.shape[0], 'pstaz': [-1]*pop_df.shape[0],'pptyp': [-1]*pop_df.shape[0],'ptpass': [-1]*pop_df.shape[0],
                       'puwarrp': [-1]*pop_df.shape[0], 'puwdepp': [-1]*pop_df.shape[0],'puwmode': [-1]*pop_df.shape[0],
                       'pwpcl': [-1]*pop_df.shape[0], 'pwtaz': [-1]*pop_df.shape[0]})
pop_df=pop_df.join(morecols) #1493219

####here assign household size in household size and person numbers in person file
hhsize_df = pop_df.groupby('hhno')['psexpfac'].sum()
hhsize_df = hhsize_df.reset_index()
final_hhs_df = final_hhs_df.merge(hhsize_df, how = 'inner', left_on = 'hhno', right_on = 'hhno')
final_hhs_df['hhsize'] = final_hhs_df['psexpfac']
final_hhs_df.drop(['psexpfac'], axis = 1, inplace = True)

#=========================================
pwtype=pop_df.WKW.fillna(-1)
pop_df.WKW=pwtype
set(pwtype)
fullworkers=[1, 2]
partworkers=[3.0, 4.0, 5.0, 6.0]
noworker=[-1]

pstype=pop_df.pstyp.fillna(-1)
pop_df.pstyp=pstype
fullstudents=[3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0]
nostudents=[-1, 0, 1.0, 2.0]
pp5=[15, 16]
pp6=[13.0, 14.0]
pp7=[2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0]
pp8=[1]

lenpersons=pop_df.shape[0] #3726050
#pptyp Person type (1=full time worker, 2=part time worker, 3=non-worker age 65+, 4=other non-working adult, 
#5=university student, 6=grade school student/child age 16+, 7=child age 5-15, 8=child age 0-4); 
#this could be made optional and computed within DaySim for synthetic populations based on ACS PUMS; for other survey data, the coding and rules may be more variable and better done outside DaySim

lastHhno = -1
personid = 0
for i in range(lenpersons):
    if i % 100000 == 0:
        print(i, ' persons processed.')

    # assign pno
    curHhno = pop_df['hhno'].iat[i]
    if curHhno != lastHhno:
        personid = 1
    else:
        personid = personid + 1
    pop_df['pno'].iat[i] = personid
    lastHhno = curHhno

    tmpw=pwtype[i]
    tmps=pstype[i]
    tmpage=ages[i]

    if tmps in nostudents:
        pop_df['pstyp'].iat[i] = 0
    elif tmps in fullstudents:
        pop_df['pstyp'].iat[i] = 1

    if tmpw in noworker:
        pop_df['pwtyp'].iat[i] = 0
        if tmpage >= 65:
            pop_df['pptyp'].iat[i] = 3
        elif tmpage > 15:
            pop_df['pptyp'].iat[i] = 4
        elif 5 <= tmpage <= 15:
            pop_df['pptyp'].iat[i] = 7
        elif 0 <= tmpage < 5:
            pop_df['pptyp'].iat[i] = 8
    elif tmpw in fullworkers:
        pop_df['pwtyp'].iat[i] = 1
        pop_df['pptyp'].iat[i] = 1
    elif tmpw in partworkers:
        pop_df['pptyp'].iat[i] = 2
        pop_df['pwtyp'].iat[i] = 2
        if tmps in fullstudents:
            pop_df['pstyp'].iat[i] = 2

    if tmps in pp5:
        pop_df['pptyp'].iat[i] = 5
    elif tmps in pp6:
        pop_df['pptyp'].iat[i] = 6
    elif tmps in pp7:
        pop_df['pptyp'].iat[i] = 7
    elif tmps in pp8:
        pop_df['pptyp'].iat[i] = 8

#del pop_df['block_group_id']
#del pop_df['hh_id']
#del pop_df['PUMA']
#del pop_df['WKW']
pop_df.drop(['block_group_id', 'hh_id', 'PUMA', 'WKW'], axis = 1, inplace = True)

morecols=pd.DataFrame({'hownrent': [-1]*final_hhs_df.shape[0]})
#del final_hhs_df[u'hownrent']
final_hhs_df.drop([u'hownrent'], axis = 1, inplace = True)
final_hhs_df=final_hhs_df.join(morecols) 

pop_df = pop_df.loc[pop_df['hhno'].isin(final_hhs_df['hhno'])]
output_h5_file = h5py.File(os.path.join(working_folder, h5_file_name), 'w')
utility.df_to_h5(final_hhs_df, output_h5_file, 'Household')
utility.df_to_h5(pop_df, output_h5_file, 'Person')
output_h5_file.close()
print('H5 exported.')

pop_df.to_csv(os.path.join(working_folder, updated_persons_file_name), sep = ',')  
final_hhs_df.to_csv(os.path.join(working_folder, updated_hhs_file_name), sep = ',')
error_f.close()

utility.backupScripts(__file__, os.path.join(working_folder, os.path.basename(__file__)))

print('Total census block groups: ', len(all_blcgrp_ids))
print('Final number of households: ', final_hhs_df.shape[0])
print('Final number of persons: ', pop_df.shape[0])
