###
###  Adjust HH and persons proportionally by control total of persons in TAZ level and export a new set of HH and 
###  persons. This is originally used in backcast.
###  4/9/2018.

import pandas as pd
import h5py
import numpy as np
import random 
import os

# new files go here
output_dir = r'D:\Hu\tests'   
# input hh and persons file:
hdf_file = h5py.File(r'D:\BKRCast_KirklandTest\BKRCast_Kirkland_Test3\inputs\hh_and_persons.h5', "r")
TAZ_Subarea_File_Name = r"Z:\Modeling Group\BKRCast\Job Conversion Test\TAZ_subarea.csv"
control_total_file_name = r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\backcast\1990Pop_BKRCastTAZ.csv"

# output hh and persons file- don't overwrite existing one!
out_h5_file = h5py.File(os.path.join(output_dir, 'hh_and_persons.h5'), 'w')

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


person_df = h5_to_df(hdf_file, 'Person')
hh_df = h5_to_df(hdf_file, 'Household')

total_population = hh_df['hhsize'].sum()
control_total_df = pd.DataFrame.from_csv(control_total_file_name, sep = ',', index_col = 'TAZNUM') 
control_total_persons = control_total_df['Sum_Population'].sum()
ratio = float(control_total_persons) / total_population

taz_list = hh_df['hhtaz'].drop_duplicates()
taz_list.sort_values(inplace = True)
taz_list.dropna(inplace = True)

total_person_taz = hh_df.groupby('hhtaz')['hhsize'].sum()

taz_list_1 = [] # list of tazs that need increase population (move persons from other tazs)
taz_list_2 = [] # list of tazs that need to reduce population from p

for taz in taz_list:
    ### random pick person records to match control total in this taz
    control_total_taz = control_total_df['Sum_Population'].loc[taz]
    if control_total_taz <= total_person_taz[taz]:
        taz_list_2.append(taz)
        ### random pick number of control_total_taz person records 
        #persons_taz = person_df[person_df['hhtaz'] == 2]
        #selected = np.random.choice(persons_taz.index, control_total_taz)
        #person_taz = persons_taz[persons_taz.index.isin(selected)]
        #new_person_df.append(persons_taz, ignore_index = True)
        
    else:
        ### get all person records, and duplicate more persons to match control total
        #print taz, control_total_taz, total_person_taz[taz]
        taz_list_1.append(taz)

pool_for_selection = pd.DataFrame()

hh_df = pd.read_csv(os.path.join(output_dir, 'hh_df.csv'), sep = ',')
pool_for_selection = pd.read_csv(os.path.join(output_dir, 'pool_for_selection.csv'), sep = ',')
## reduce population for taz_list_2
#cnt = 0
#for taz in taz_list_2:
#    control_total_taz = control_total_df['Sum_Population'].loc[taz]       
#    target_pop = total_person_taz[taz]
#    #randomly select hh and remove it from hhs and persons
#    while (control_total_taz < target_pop):
#        hhs_taz = hh_df[hh_df['hhtaz'] == taz]
#        hhno = np.random.choice(hhs_taz['hhno'], 1)[0]
#        number_persons = hh_df[hh_df['hhno'] == hhno]['hhsize']
#        pool_for_selection = pool_for_selection.append(hh_df[hh_df['hhno'] == hhno])
#        hh_df = hh_df[(hh_df['hhno'] != hhno)]
#        target_pop = target_pop - number_persons.item()
    
#    cnt = cnt + 1
#    print "taz {0:5.0f} {1:d} to be processed".format(taz, len(taz_list_2) - cnt)
    
    #assert target_pop == hh_df[hh_df['hhtaz'] == taz]['hhsize'].sum()

# relocate persons and hhs for taz_list_1
for taz in taz_list_1:
    control_total_taz = control_total_df['Sum_Population'].loc[taz]
    target_pop = total_person_taz[taz]
    # relocate hhs from other taz
    while (control_total_taz > target_pop):
        hhno = np.random.choice(pool_for_selection['hhno'], 1)[0]
        number_persons = pool_for_selection[pool_for_selection['hhno'] == hhno]['hhsize']
        pool_for_selection.loc[pool_for_selection['hhno'] == hhno, 'hhtaz'] = taz
        #hh_df.loc[hh_df['hhno'] == hhno, 'hhtaz'] = taz
        hh_df = hh_df.append(pool_for_selection[pool_for_selection['hhno'] == hhno])
        target_pop = target_pop + number_persons.item()
    print 'taz {0:5f} control {1:5f} before {2:5f}  after {3:5f}'.format(taz, control_total_taz, total_person_taz[taz], hh_df[hh_df['hhtaz'] == taz]['hhsize'].sum())
     
person_df = person_df[person_df['hhno'].isin(hh_df['hhno'])]

df_to_h5(hh_df, out_h5_file, 'Household')
df_to_h5(person_df, out_h5_file, 'Person')
out_h5_file.close()


        

                
   


