import pandas as pd
import os
import h5py
import numpy as np
from collections import Counter
import utility


# input configuration
working_folder = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\KirklandSupport\Kirkland2044Complan\target2044_popsim_from_baseline"
baseline_popsim = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\KirklandSupport\Kirkland2044Complan\baseline2044\final_COK_renumbered_2044_kirk_complan_baseline_hh_and_persons.h5"
lookup_file = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'
parcel_allocation_guide_file = '2044_kirkcomplan_target_parcels_for_allocation_local_estimate.csv'

# output configuration
output_popsim = '2044_kirk_complan_target_hh_and_persons_reallocated_from_baseline.h5'
taz_hhs_growth_file = '2044_Kirk_Complan_target_hhs_growth_from_baseline.csv'

Jurisdiction_selector = ['KIRKLAND']
# enf of configuration


lookup_df = pd.read_csv(lookup_file, low_memory = False)


def check_list_duplicated(datalist):
    counter = Counter(datalist)
    duplicates = [item for item, count in counter.items() if count > 1]
    return duplicates

print('Loading hh and person file...')
with h5py.File(baseline_popsim, "r") as hdf_file: 
    hh_df = utility.h5_to_df(hdf_file, 'Household')
    person_df = utility.h5_to_df(hdf_file, 'Person')

parcel_allocation_guide_df = pd.read_csv(os.path.join(working_folder, parcel_allocation_guide_file))
parcel_allocation_guide_df['total_hhs'] = parcel_allocation_guide_df['total_hhs'].round(0).astype(int)
parcel_allocation_guide_df = parcel_allocation_guide_df.merge(lookup_df[['PSRC_ID', 'Jurisdiction']], on = 'PSRC_ID', how = 'left')
sel_parcel_allocation_guide_df = parcel_allocation_guide_df.loc[parcel_allocation_guide_df['Jurisdiction'].isin(Jurisdiction_selector)].copy()
baseline_hhs_by_parcel_df = hh_df[['hhparcel', 'hhexpfac']].groupby('hhparcel').sum().reset_index()

sel_parcel_allocation_guide_df = sel_parcel_allocation_guide_df.merge(baseline_hhs_by_parcel_df, left_on = 'PSRC_ID', right_on = 'hhparcel', how = 'left')
sel_parcel_allocation_guide_df.fillna(0, inplace = True)
sel_parcel_allocation_guide_df['total_hhs'] = sel_parcel_allocation_guide_df['total_hhs'].round(0).astype(int)
sel_parcel_allocation_guide_df.drop(columns = ['hhparcel'], inplace = True)
sel_parcel_allocation_guide_df.rename(columns={'hhexpfac': 'baseline_hhs', 'total_hhs':'control_hhs'}, inplace = True)
sel_parcel_allocation_guide_df['Growth'] = sel_parcel_allocation_guide_df['control_hhs'] - sel_parcel_allocation_guide_df['baseline_hhs']

taz_growth_df = sel_parcel_allocation_guide_df[['BKRCastTAZ', 'control_hhs', 'baseline_hhs', 'Growth']].groupby('BKRCastTAZ').sum()
taz_growth_df.to_csv(os.path.join(working_folder, taz_hhs_growth_file))
sel_parcel_allocation_guide_df.to_csv(os.path.join(working_folder, 'kirk_control.csv'))


parcel_hhs_growth_df = sel_parcel_allocation_guide_df.loc[sel_parcel_allocation_guide_df['Growth'] != 0].sort_values(by = 'Growth', ascending = True)
neg_growth_df = parcel_hhs_growth_df.loc[parcel_hhs_growth_df['Growth'] < 0].sort_values(by = 'Growth', ascending = True)
pos_growth_df = parcel_hhs_growth_df.loc[parcel_hhs_growth_df['Growth'] > 0].sort_values(by = 'Growth', ascending = False)
neg_growth_df.to_csv(os.path.join(working_folder, 'neg_control.csv'))
pos_growth_df.to_csv(os.path.join(working_folder, 'pos_control.csv'))

for_reallocation_hhno_list = []
# total hhs in baseline is higher than target
# only need to randomly select growth number of hhs and set their hhtaz = -1
total_selected = 0
for parcel, growth in zip(neg_growth_df['PSRC_ID'], neg_growth_df['Growth']):
        selected = np.random.choice(hh_df.loc[hh_df['hhparcel'] == parcel, 'hhno'], abs(int(growth)), replace = False).tolist() 
        dup = check_list_duplicated(selected)
        if (len(dup) > 0):
            print(f'parcel {parcel} growth {growth}: duplicated hhs are selected. {dup}')  
        for_reallocation_hhno_list.extend(selected)
        total_selected += len(selected)
        hh_df.loc[hh_df['hhno'].isin(selected), ['hhtaz', 'hhparcel']] = -1

print(f'{total_selected} households are removed from baseline parcels.')
counter = Counter(for_reallocation_hhno_list)
duplicates = [item for item, count in counter.items() if count > 1]

# total hhs in target is higher than baseline
# reallocate hhs from for_reallocation_hhs_list to relevant parcels
# so far, it can only handle total negative growth is bigger than pos growth (total hhs in new scenario is less than baseline)
# do not work if total hhs in new scenario is higher. 
total_selected = 0
for parcel, taz, growth in zip(pos_growth_df['PSRC_ID'], pos_growth_df['BKRCastTAZ'], pos_growth_df['Growth']):
    selected = np.random.choice(for_reallocation_hhno_list, int(growth), replace = False).tolist()
    mask = hh_df['hhno'].isin(selected)
    hh_df.loc[mask, 'hhtaz'] = taz
    hh_df.loc[mask, 'hhparcel'] = parcel
    for_reallocation_hhno_list = [item for item in for_reallocation_hhno_list if item not in selected]
    total_selected += len(selected)

print(f'{total_selected} households are reallocated to baseline parcels.')

print(f'baseline has {hh_df.shape[0]} hhs.')
updated_hhs_df = hh_df.loc[hh_df['hhtaz'] > 0]
hhs_to_be_removed = hh_df.loc[hh_df['hhtaz'] == -1]
updated_persons_df = person_df.loc[~person_df['hhno'].isin(hhs_to_be_removed['hhno'])]
print(f'new popsim has {updated_hhs_df.shape[0]} hhs.')

with h5py.File(os.path.join(working_folder, output_popsim), 'w') as new_popsim:
    utility.df_to_h5(updated_hhs_df, new_popsim, 'Household')
    utility.df_to_h5(updated_persons_df, new_popsim, 'Person')

utility.backupScripts(__file__, os.path.join(working_folder, os.path.basename(__file__)))
print('Done')