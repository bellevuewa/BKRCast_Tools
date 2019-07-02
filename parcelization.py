import pandas as pd
import numpy as np
import os
import math
import h5py
import utility

'''
This program takes output files, synthetic households and synthetic persons, from PopulationSim,
and allocate them to parcels. It also reformat household and person data columns to match
BKRCast input requirement. The output, h5_file_name, can be directly loaded into BKRCast.

Number of households per parcel should be consistent with parcel file. It can be done by calling 
sync_population_parcel.py. 

Date: 7/1/2019
'''

#configuration
working_folder = r'D:\PopulationSim\PSRCrun0423\output'
synthetic_households_file_name = 'synthetic_households.csv'
synthetic_population_file_name = 'synthetic_persons.csv'
parcel_filename = 'I:/psrcpopsim/popsimproject/parcelize/parcel_TAZ_2014_lookup.csv'
h5_file_name = 'popsim_hh_and_persons.h5'

def assign_hhs_to_parcels_by_blkgrp(hhs_control, hhs_blkgrp_df, parcel_df, blkgrpid):
    '''
        Household allocation method (by priority):
        1. one household goes to each single family parcel. 
        2. the remainning households go to multiple family parcel following the proportion given in parcel data.
        hhs_control: the forecast number of households in a block group 
        hhs_blkgrp_df: forecast households dataframe (in a blockgroup)
        parcel_df: parcel dataframe. (all parcels) 
        blkgrp: block group id
    '''
    parcels = parcel_df.loc[parcel_df['GEOID10'] == blcgrpid]
    
    # assign SF first
    sfparcelids = parcels.loc[parcels['LUTYPE_P'] == 24]
    numSFparcels = sfparcelids.shape[0]
    
    updatedHHs = pd.DataFrame()
    sfHhs = pd.DataFrame()
    mfparcels = pd.DataFrame()

    if hhs_control <= numSFparcels:        
        sfHhs = hhs_blkgrp_df.sample(n = int(hhs_control))
        sfparcels = sfparcelids.sample(n = int(hhs_control))
        for i in range(int(hhs_control)):
            sfHhs['hhparcel'].iat[i] = sfparcels['PSRC_ID'].iat[i]
        updatedHHs = updatedHHs.append(sfHhs)
        return updatedHHs
    
    # hhs_control > numSFparcels. assign one hh to each SF parcel   
    sfHhs = hhs_blkgrp_df.sample(n = int(numSFparcels))
    for i in range(int(numSFparcels)):
        sfHhs['hhparcel'].iat[i] = sfparcelids['PSRC_ID'].iat[i]
    remainHhs_df = hhs_blkgrp_df[~hhs_blkgrp_df['household_id'].isin(sfHhs['household_id'])]
    remainHhs = remainHhs_df['hhexpfac'].sum()

    sum = parcels.loc[parcels['HousingUnits2014'] >= 2, 'HousingUnits2014'].sum()
    if sum > 0: 
        mfparcels = parcels.loc[parcels['HousingUnits2014'] >= 2] 
        mfparcels.loc[:, 'forecastedHhs'] = remainHhs * 1.0 * mfparcels['HousingUnits2014'] / sum
        updatedHHs = updatedHHs.append(sfHhs)        
    else:
        mfparcels = parcels.loc[(parcels['LUTYPE_P'] == 26) | (parcels['LUTYPE_P'] == 30)]
        if mfparcels.shape[0] > 0:
            mfparcels.loc[:, 'forecastedHhs'] = (hhs_control - numSFparcels) * 1.0 / mfparcels.shape[0]
            updatedHHs = updatedHHs.append(sfHhs)        
        else:
            mfparcels = sfparcelids
            mfparcels.loc[:, 'forecastedHhs'] = hhs_control * 1.0 / numSFparcels

    # decide in each mf parcel, how many hhs should be allocated
    mfparcels.loc[:, 'forecastedHhs'] = mfparcels['forecastedHhs'].apply(np.floor)
    diff = remainHhs - mfparcels['forecastedHhs'].sum()
    mfparcels.loc[:, 'forecastedHhs'].iat[0] = mfparcels['forecastedHhs'].iat[0] + diff    
    
    mfhhs = pd.DataFrame()    
    for row in mfparcels.itertuples():
        mfhhs = remainHhs_df.sample(n = int(row.forecastedHhs))
        remainHhs_df = remainHhs_df[~remainHhs_df['household_id'].isin(mfhhs['household_id'])]   
        mfhhs.loc[:, 'hhparcel'] = row.PSRC_ID
        updatedHHs = updatedHHs.append(mfhhs)
        if remainHhs_df.shape[0] == 0:
            break
    return updatedHHs   


###
parcel_df = pd.read_csv(parcel_filename, low_memory=False) 
hhs_df = pd.read_csv(os.path.join(working_folder, synthetic_households_file_name))
hhs_df['hhparcel'] = 0
# remove any blockgroup ID is Nan.
all_blcgrp_ids = hhs_df['block_group_id'].unique()
mask = np.isnan(all_blcgrp_ids)
all_blcgrp_ids = all_blcgrp_ids[~mask]

hhs_by_blkgrp = hhs_df.groupby('block_group_id')['hhexpfac', 'hhsize'].sum()

final_hhs_df = pd.DataFrame()
updatedHhs = pd.DataFrame()

for blcgrpid in all_blcgrp_ids:
    hhs_new = hhs_by_blkgrp.loc[hhs_by_blkgrp.index == blcgrpid].iloc[0]['hhexpfac']
    print 'Blockgroup ', blcgrpid , ' ', '  new hhs: ', hhs_new
    if hhs_new > 0:
        hhs_blkgrp_df = hhs_df.loc[hhs_df['block_group_id'] == blcgrpid] 
        if blcgrpid == 530330229025:
            a = 0
        updatedHhs = assign_hhs_to_parcels_by_blkgrp(hhs_new, hhs_blkgrp_df, parcel_df, blcgrpid)            
        final_hhs_df = final_hhs_df.append(updatedHhs)   
        if len(final_hhs_df.loc[final_hhs_df['household_id'].duplicated()]) > 0:
            print 'duplicated households found in block group ', blcgrpid
            break
    else:
        print 'no household in block_group ', blcgrpid 
    
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
#del final_hhs_df['psexpfac']

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
    if i % 10000 == 0:
        print i

    # assign pno
    curHhno = pop_df['hhno'].iat[i]
    if curHhno <> lastHhno:
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

output_h5_file = h5py.File(os.path.join(working_folder, h5_file_name), 'w')
utility.df_to_h5(final_hhs_df, output_h5_file, 'Household')
utility.df_to_h5(pop_df, output_h5_file, 'Person')
output_h5_file.close()
print 'H5 exported.'

pop_df.to_csv(os.path.join(working_folder, 'updated_persons.csv'))  
final_hhs_df.to_csv(os.path.join(working_folder, 'updated_synthetic_households.csv'))

print 'Total census block groups: ', len(all_blcgrp_ids)
print 'Final number of households: ', final_hhs_df.shape[0]
print 'Final number of persons: ', pop_df.shape[0]
