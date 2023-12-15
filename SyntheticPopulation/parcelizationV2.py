from xml.dom import HIERARCHY_REQUEST_ERR
import pandas as pd
import numpy as np
import os
import math
import utility
import h5py

'''
This program takes synthetic households and synthetic persons( from PopulationSim) as inputs,
and allocates them to parcels, under the guidance of parcels_for_allocation_filename which is an output file from 
Prepare_Hhs_for_future_using_KR_oldTAZ_COB_parcel_forecast.py or prepare_hhs_for_baseyear_using_ofm.py. It also reformats household and person
data columns to match BKRCast input requirement. The output, h5_file_name, can be directly loaded into BKRCast.

Number of households per parcel in synthetic population should be consistent with the parcel file. It can be done by calling 
sync_population_parcel.py. 

3/9/2022
upgrade to python 3.7
'''


###############Start of configuration
working_folder = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\KirklandSupport\Kirkland2044Complan\baseline2044 - Copy'
synthetic_households_file_name = '2044_kirk_complan_baseline_synthetic_households.csv'
synthetic_population_file_name = '2044_kirk_complan_baseline_synthetic_persons.csv'

# number of hhs per parcel
parcels_for_allocation_filename = r"2044_kirkcomplan_baseline_parcels_for_allocation_local_estimate.csv"

## output
updated_hhs_file_name = 'updated_2044_kirk_complan_baseline_synthetic_households.csv'
updated_persons_file_name = 'updated_2044_kirk_complan_baseline_synthetic_persons.csv'
h5_file_name = '2044_kirk_complan_baseline_hh_and_persons.h5'

############## End of configuration
   
###
print('Loading....')
hhs_df = pd.read_csv(os.path.join(working_folder, synthetic_households_file_name))
hhs_df['hhparcel'] = 0
hhs_by_GEOID10 = hhs_df[['block_group_id', 'hhexpfac']].groupby('block_group_id').sum()

parcels_for_allocation_df = pd.read_csv(os.path.join(working_folder, parcels_for_allocation_filename))
# remove any blockgroup ID is Nan.
all_blcgrp_ids = hhs_df['block_group_id'].unique()
mask = np.isnan(all_blcgrp_ids)
all_blcgrp_ids = all_blcgrp_ids[~mask]

# special treatment on GEOID10 530619900020. Since in 2016 ACS no hhs lived in this census blockgroup, when creating popsim control file
# we move all hhs in this blockgroup to 530610521042. We need to do the same thing when we allocate hhs to parcels.
parcels_for_allocation_df.loc[(parcels_for_allocation_df['GEOID10'] == 530619900020) & (parcels_for_allocation_df['total_hhs'] > 0), 'GEOID10'] = 530610521042

hhs_by_blkgrp_popsim = hhs_df.groupby('block_group_id')[['hhexpfac', 'hhsize']].sum()
hhs_by_blkgrp_parcel = parcels_for_allocation_df.groupby('GEOID10')[['total_hhs']].sum()
final_hhs_df = pd.DataFrame()
for blcgrpid in all_blcgrp_ids:
    # if (hhs_by_GEOID10.loc[blcgrpid, 'hhexpfac'] != hhs_by_blkgrp_parcel.loc[blcgrpid, 'total_hhs']):
    #     print(f"GEOID10 {blcgrpid}:  popsim: {hhs_by_GEOID10.loc[blcgrpid, 'hhexpfac']}, parcel: {hhs_by_blkgrp_parcel.loc[blcgrpid, 'total_hhs']}")
    #     print('popsim should equal parcel. You need to fix this issue before moving forward.')
    #     exit(-1)
    num_parcels = 0 
    num_hhs = 0
    parcels_in_GEOID10_df = parcels_for_allocation_df.loc[(parcels_for_allocation_df['GEOID10'] == blcgrpid) & (parcels_for_allocation_df['total_hhs'] > 0)]
    subtotal_parcels = parcels_in_GEOID10_df.shape[0]
    selected_hhs_df = hhs_df.loc[(hhs_df['block_group_id'] == blcgrpid) & (hhs_df['hhparcel'] == 0)]
    j_start_index = 0
    index_hhparcel = selected_hhs_df.columns.get_loc('hhparcel')
    for i in range(subtotal_parcels):
        numHhs = parcels_in_GEOID10_df['total_hhs'].iat[i]
        parcelid = parcels_in_GEOID10_df['PSRC_ID'].iat[i]
        for j in range(int(numHhs)):
            selected_hhs_df.iat[j + j_start_index, index_hhparcel] = parcelid 
            num_hhs += 1          
        num_parcels += 1
        j_start_index += int(numHhs)

    final_hhs_df = pd.concat([final_hhs_df, selected_hhs_df])
    #hhs_df = hhs_df.loc[~hhs_df['household_id'].isin(selected_hhs_df['household_id'])]
 
    ## process sf parcel in batch, to save some computer time
    #sf_parcels_df = parcels_for_allocation_df.loc[(parcels_for_allocation_df['GEOID10'] == blcgrpid) & (parcels_for_allocation_df['total_hhs'] == 1)]
    #sf_parcels_count = sf_parcels_df.shape[0]
    #if sf_parcels_count > 0:
    #    selected_hhids = hhs_df.loc[(hhs_df['block_group_id'] == blcgrpid) & (hhs_df['hhparcel'] == 0)].sample(n = sf_parcels_count)['household_id']   
    #    hhs_df.loc[hhs_df['household_id'].isin(selected_hhids), 'hhparcel'] = sf_parcels_df['PSRC_ID']
    #    num_parcels += sf_parcels_count
    #parcels_GEOID10_df = parcels_for_allocation_df.loc[(parcels_for_allocation_df['GEOID10'] == blcgrpid) & (parcels_for_allocation_df['total_hhs'] > 1)]
    #for parcel in parcels_GEOID10_df.itertuples():
    #    num_parcels += 1
    #    selected_hhids = hhs_df.loc[(hhs_df['block_group_id'] == blcgrpid) & (hhs_df['hhparcel'] == 0)].sample(n = int(parcel.total_hhs))['household_id']   
    #    hhs_df.loc[hhs_df['household_id'].isin(selected_hhids), 'hhparcel'] = parcel.PSRC_ID
    print(f"{hhs_by_GEOID10.loc[blcgrpid, 'hhexpfac']} (actual {num_hhs}) hhs allocated to GEOID10 {blcgrpid}, {num_parcels} parcels are processed")


final_hhs_df = final_hhs_df.merge(parcels_for_allocation_df[['PSRC_ID', 'BKRCastTAZ']], how = 'left', left_on = 'hhparcel', right_on = 'PSRC_ID')
final_hhs_df.rename(columns = {'BKRCastTAZ': 'hhtaz'}, inplace = True)
final_hhs_df.drop(columns = ['PSRC_ID'], axis = 1, inplace = True)

### process other attributes to match required columns
pop_df = pd.read_csv(os.path.join(working_folder, synthetic_population_file_name)) 
pop_df.rename(columns={'household_id':'hhno', 'SEX':'pgend'}, inplace = True)
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
hhsize_df = pop_df.groupby('hhno')[['psexpfac']].sum().reset_index()
final_hhs_df.rename(columns = {'household_id':'hhno'}, inplace = True)
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

pop_df.drop(['block_group_id', 'hh_id', 'PUMA', 'WKW'], axis = 1, inplace = True)

morecols=pd.DataFrame({'hownrent': [-1]*final_hhs_df.shape[0]})
final_hhs_df.drop(['hownrent'], axis = 1, inplace = True)
final_hhs_df=final_hhs_df.join(morecols) 

pop_df = pop_df.loc[pop_df['hhno'].isin(final_hhs_df['hhno'])]
output_h5_file = h5py.File(os.path.join(working_folder, h5_file_name), 'w')
utility.df_to_h5(final_hhs_df, output_h5_file, 'Household')
utility.df_to_h5(pop_df, output_h5_file, 'Person')
output_h5_file.close()
print('H5 exported.')

pop_df.to_csv(os.path.join(working_folder, updated_persons_file_name), sep = ',')  
final_hhs_df.to_csv(os.path.join(working_folder, updated_hhs_file_name), sep = ',')

utility.backupScripts(__file__, os.path.join(working_folder, os.path.basename(__file__)))

print('Total census block groups: ', len(all_blcgrp_ids))
print('Final number of households: ', final_hhs_df.shape[0])
print('Final number of persons: ', pop_df.shape[0])
print('Done')
