from msilib import Control
import pandas as pd
import numpy as np
import os
import math
import h5py
import utility

# input configuration
working_folder = r'Z:\Modeling Group\BKRCast\KirklandSupport\Kirkland2044Complan\preferred_2044'
kirkland_land_use_file = 'parcel_fixed_Kirkland_Complan_2044_target_Landuse.csv'
lookup_file = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'
psrc_2044_parcel_file_name = r"interpolated_parcel_file_2044_from_PSRC_2014_2050_w_psrc_hhs.txt"


LU_category_2044 = {'EMPCOM_2044':'Commercial', 'EMPIND_2044':'Industrial', 'EMPOFF_2044':'Office', 'EMPINST_2044':'Institutional', 'EMPTOT_2044': 'Total_jobs',  'HHSF_2044':'Sfhh', 'HHMF_2044':'Mfhh'}
Columns_List = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPRET_P', 'EMPRSC_P', 'EMPSVC_P', 'EMPOTH_P']

# output configuration
updated_land_use_file = '2044_kirkcomplan_target_parcels_urbansim.txt'
control_total_file_name = '2044_Kirkland_target_control_total_by_BKRCastTAZ.csv'
lu_parcel_comparison_file_name = '2044_kirkland_target_landuse_parcel_comparison.csv'
kirk_parcel_file_name = 'Adjusted_2044_Kirkland_target_parcels.csv'

sf_occupancy_rate = 0.952  # from Gwen
mf_occupancy_rate = 0.895  # from Gwen
avg_persons_per_sfhh =  2.82 # from Gwen
avg_persons_per_mfhh =  2.03 # from Gwen


def controlled_rounding(data_df, attr_name, control_total):
    # find residential parcels within taz     
    updated_data_df = data_df.copy()
    total_rows = updated_data_df.shape[0]
    already_assigned = updated_data_df[attr_name].sum()
    
    # how many need to be assigned or removed to match the control total
    diff = int(control_total - already_assigned)
    if diff == 0:
        return updated_data_df

    if total_rows >= abs(diff):
        selected_indices = np.random.choice(updated_data_df.index, size = abs(diff), replace = False) 
    else:
        selected_indices = np.random.choice(updated_data_df.index, size = abs(diff), replace = True) 
    
    unique_indices, counts = np.unique(selected_indices, return_counts=True)
    sorted_zipped = sorted(zip(unique_indices, counts), key=lambda x: x[1], reverse=True)

    index_for_2nd_round = []
    if control_total >= already_assigned: # need to add more hhs to match the control total, 
        for index, count in sorted_zipped:
            updated_data_df.loc[index, attr_name] += count   
    else:  # need to remove some hhs to match the control total. more complicated.
        remaining = 0
        for index, count in sorted_zipped:
            count += remaining
            # need to ensure no negative households
            if updated_data_df.loc[index, attr_name] >= count:
                updated_data_df.loc[index, attr_name] -= count
                remaining = 0
                index_for_2nd_round.append(index)
            else:
                remaining = count - updated_data_df.loc[index, attr_name]
                updated_data_df.loc[index, attr_name] = 0

        if (remaining > 0):
            for index in index_for_2nd_round:
                curValue = updated_data_df.loc[index, attr_name]
                if curValue >= remaining:
                    updated_data_df.loc[index, attr_name] = curValue - remaining
                    remaining = 0
                    break
                else:
                    remaining -= updated_data_df.loc[index, attr_name]
                    updated_data_df.loc[index, attr_name] = 0
    return updated_data_df



# this function is used to integerize household fractions in a parcel file, if the control total in TAZ level is known. 
# The number of households in the parcel file will be held to the maximum extent by rounding to its nearliest integer. 
# After that household adjustment (increase or decrease by 1) will be made on randomly selected residential parcels so that number of hhs
# will match the control total.
# parcel_df: parcel file with hhs attribute
# control_in_taz_df: control total in TAZ
# parcel_hhs_attr_name: hhs attribute name in parcel file
# taz_control_total_attr_name: control total attribute name in control_in_taz_df
def integerize_households(parcels_df, control_in_taz_df, parcel_hhs_attr_name,  taz_control_total_attr_name):
    updated_parcels_df = parcels_df.copy()
    for taz in control_in_taz_df['BKRCastTAZ']:
        # find residential parcels within taz     
        parcels_in_taz_df = updated_parcels_df.loc[(updated_parcels_df['TAZ_P'] == taz) & (updated_parcels_df[parcel_hhs_attr_name] > 0)] 
        if parcels_in_taz_df.shape[0] == 0: # if no residential parcels, use all parcels
            parcels_in_taz_df = updated_parcels_df.loc[updated_parcels_df['TAZ_P'] == taz]

        control_total = control_in_taz_df.loc[control_in_taz_df['BKRCastTAZ'] == taz, taz_control_total_attr_name].iloc[0]
        parcels_in_taz_df = controlled_rounding(parcels_in_taz_df, parcel_hhs_attr_name, control_total)

        updated_parcels_df.update(parcels_in_taz_df)
    
    return updated_parcels_df

print('loading...')
kirk_ctrl_input_lu_df = pd.read_csv(os.path.join(working_folder, kirkland_land_use_file))
lookup_df = pd.read_csv(lookup_file)
kirk_ctrl_input_lu_df = kirk_ctrl_input_lu_df.merge(lookup_df[['PSRC_ID', 'Jurisdiction']], on = 'PSRC_ID', how = 'left')
psrc_parcels_df = pd.read_csv(os.path.join(working_folder, psrc_2044_parcel_file_name), sep = ' ')
psrc_parcels_df = psrc_parcels_df.merge(lookup_df[['PSRC_ID', 'Jurisdiction']], left_on = 'PARCELID', right_on = 'PSRC_ID', how = 'left')
# make sure total is sum of all categories
psrc_parcels_df['EMPTOT_P'] = psrc_parcels_df[Columns_List].sum(axis=1)

kirk_input_lu_df = kirk_ctrl_input_lu_df.copy()
# calculate households
kirk_input_lu_df['SF_2044'] = kirk_input_lu_df['SFU_2044'] * sf_occupancy_rate
kirk_input_lu_df['MF_2044'] = kirk_input_lu_df['MFU_2044'] * mf_occupancy_rate

# calculate number of jobs and hhs by BKRCastTAZ
kirk_control_by_TAZ_df = kirk_input_lu_df.groupby('BKRCastTAZ').sum().reset_index()
kirk_control_by_TAZ_df.drop(columns = ['PSRC_ID'], inplace = True)
total_SF = int(round(kirk_control_by_TAZ_df['SF_2044'].sum(), 0))
total_MF = int(round(kirk_control_by_TAZ_df['MF_2044'].sum(), 0))
kirk_control_by_TAZ_df = kirk_control_by_TAZ_df.round(0).astype(int)
kirk_control_by_TAZ_df['EMPTOT_2044'] = kirk_control_by_TAZ_df['EMPCOM_2044'] + kirk_control_by_TAZ_df['EMPIND_2044'] + kirk_control_by_TAZ_df['EMPOFF_2044'] + kirk_control_by_TAZ_df['EMPINST_2044']
kirk_control_by_TAZ_df['DU_2044'] = kirk_control_by_TAZ_df['SFU_2044'] + kirk_control_by_TAZ_df['MFU_2044']
kirk_control_by_TAZ_df['SF_2044'] = kirk_control_by_TAZ_df['SF_2044'].round(0).astype(int)
kirk_control_by_TAZ_df['MF_2044'] = kirk_control_by_TAZ_df['MF_2044'].round(0).astype(int)


kirk_control_by_TAZ_df = controlled_rounding(kirk_control_by_TAZ_df, 'SF_2044', total_SF)
kirk_control_by_TAZ_df = controlled_rounding(kirk_control_by_TAZ_df, 'MF_2044', total_MF)

kirk_control_by_TAZ_df.rename(columns={'SF_2044':'SF_ctrl_2044', 'MF_2044':'MF_ctrl_2044'}, inplace = True)
kirk_control_by_TAZ_df['TotHhs_ctrl_2044'] = kirk_control_by_TAZ_df['SF_ctrl_2044'] + kirk_control_by_TAZ_df['MF_ctrl_2044']
kirk_control_by_TAZ_df.to_csv(os.path.join(working_folder, control_total_file_name))

print('Kirkland (control) land use summary by BKRCastTAZ is exported.')
print(f'Control total {kirk_control_by_TAZ_df["EMPTOT_2044"].sum()}')

job_col_list = Columns_List.copy()
job_col_list.append('EMPTOT_P')

lu_col_list = job_col_list.copy()
lu_col_list.extend(['TAZ_P', 'HH_P'])

# calculate scale factor
kirk_parcels_df = psrc_parcels_df.loc[psrc_parcels_df['Jurisdiction'] == 'KIRKLAND'].copy()
kirk_psrc_sum_by_BKRCastTAZ = kirk_parcels_df[lu_col_list].groupby('TAZ_P').sum().reset_index()
kirk_psrc_sum_by_BKRCastTAZ = kirk_psrc_sum_by_BKRCastTAZ.merge(kirk_control_by_TAZ_df[['BKRCastTAZ', 'EMPTOT_2044', 'SF_ctrl_2044', 'MF_ctrl_2044', 'TotHhs_ctrl_2044']], left_on = 'TAZ_P', right_on = 'BKRCastTAZ')
kirk_psrc_sum_by_BKRCastTAZ.loc[kirk_psrc_sum_by_BKRCastTAZ['EMPTOT_P'] != 0 , 'scale'] = kirk_psrc_sum_by_BKRCastTAZ['EMPTOT_2044'] / kirk_psrc_sum_by_BKRCastTAZ['EMPTOT_P']
kirk_psrc_sum_by_BKRCastTAZ.loc[kirk_psrc_sum_by_BKRCastTAZ['EMPTOT_P'] == 0 , 'scale'] = 1

# these tazs cannot be scaled because denominators are zeros. 
special_taz = kirk_psrc_sum_by_BKRCastTAZ.loc[(kirk_psrc_sum_by_BKRCastTAZ['EMPTOT_P']==0) & (kirk_psrc_sum_by_BKRCastTAZ['EMPTOT_2044']>0), 'TAZ_P']
special_parcels = kirk_input_lu_df.loc[kirk_input_lu_df['BKRCastTAZ'].isin(special_taz)]

# calculate adjusted jobs based on PSRC's job distribution. Kirkland citywide total jobs match the control total. 
kirk_parcels_df = kirk_parcels_df.merge(kirk_psrc_sum_by_BKRCastTAZ[['TAZ_P', 'scale']], on = 'TAZ_P')

kirkland_local_job_category = {'EMPCOM_2044':'EMPRET_P', 'EMPIND_2044':'EMPIND_P', 'EMPOFF_2044':'EMPOFC_P', 'EMPINST_2044':'EMPGOV_P'}
merged_df = kirk_parcels_df.merge(special_parcels[['PSRC_ID'] + list(kirkland_local_job_category.keys())], on = 'PSRC_ID', how = 'right').reset_index()


for key, val in kirkland_local_job_category.items():
    merged_df[val] = merged_df[key].round(0).astype(int)
merged_df.drop(columns = list(kirkland_local_job_category.keys()), inplace = True)

kirk_parcels_df = kirk_parcels_df.loc[~kirk_parcels_df['PSRC_ID'].isin(merged_df['PSRC_ID'])]
kirk_parcels_df = pd.concat([kirk_parcels_df, merged_df])


kirk_parcels_df[Columns_List] = kirk_parcels_df[Columns_List].multiply(kirk_parcels_df['scale'], axis = 0)

kirk_parcels_df[Columns_List] = kirk_parcels_df[Columns_List].round(0).astype(int)

# controlled rounding for jobs.
# find the citywide total job difference, assign the difference proportionally to each job category. Then controlled balance each category.
total_jobs_ctrl = kirk_control_by_TAZ_df['EMPTOT_2044'].sum()
diff = total_jobs_ctrl - kirk_parcels_df[Columns_List].sum().sum()
total_assigned_jobs = kirk_parcels_df[Columns_List].sum(axis = 1).sum()
for job_cat in Columns_List:
    assigned_by_job_cat = kirk_parcels_df[job_cat].sum()
    job_cat_ctrl = int(round((assigned_by_job_cat / total_assigned_jobs) * diff + assigned_by_job_cat, 0))
    kirk_parcels_df = controlled_rounding(kirk_parcels_df, job_cat, job_cat_ctrl)

kirk_parcels_df['EMPTOT_P'] = kirk_parcels_df[Columns_List].sum(axis = 1)
print(f'After scaling, total jobs in Kirkland is {kirk_parcels_df["EMPTOT_P"].sum()}')

# adjust number of hhs to whole number.
kirk_parcels_df = kirk_parcels_df.merge(kirk_input_lu_df[['PSRC_ID', 'SF_2044', 'MF_2044']], on = 'PSRC_ID', how = 'left')
kirk_parcels_df['SF_2044'] = kirk_parcels_df['SF_2044'].fillna(0)
kirk_parcels_df['MF_2044'] = kirk_parcels_df['MF_2044'].fillna(0)
kirk_parcels_df['SF_2044'] = kirk_parcels_df['SF_2044'].round(0).astype(int)
kirk_parcels_df['MF_2044'] = kirk_parcels_df['MF_2044'].round(0).astype(int)
kirk_parcels_df = integerize_households(kirk_parcels_df, kirk_psrc_sum_by_BKRCastTAZ, 'SF_2044', 'SF_ctrl_2044')
kirk_parcels_df = integerize_households(kirk_parcels_df, kirk_psrc_sum_by_BKRCastTAZ, 'MF_2044', 'MF_ctrl_2044')
kirk_parcels_df['HH_P'] = kirk_parcels_df['SF_2044'] + kirk_parcels_df['MF_2044']


print(f'Household control total: {kirk_psrc_sum_by_BKRCastTAZ["TotHhs_ctrl_2044"].sum()}')
print(f'Total households after integerization: {kirk_parcels_df["SF_2044"].sum() + kirk_parcels_df["MF_2044"].sum()}')
kirk_parcels_df.to_csv(os.path.join(working_folder, kirk_parcel_file_name), index = False)

lu_col_list.extend(['PSRC_ID', 'SF_2044', 'MF_2044'])
kirk_compare_lu_df = kirk_ctrl_input_lu_df.merge(kirk_parcels_df[lu_col_list], on = 'PSRC_ID', how = 'left')
kirk_compare_lu_df.to_csv(os.path.join(working_folder, lu_parcel_comparison_file_name))

kirk_rounded_by_TAZ = kirk_parcels_df[['SF_2044', 'MF_2044', 'TAZ_P']].groupby('TAZ_P').sum().reset_index()
kirk_rounded_by_TAZ['TotHhs_2044'] = kirk_rounded_by_TAZ['SF_2044'] + kirk_rounded_by_TAZ['MF_2044']
kirk_rounded_comparison_TAZ = kirk_rounded_by_TAZ.merge(kirk_psrc_sum_by_BKRCastTAZ[['SF_ctrl_2044', 'MF_ctrl_2044', 'TotHhs_ctrl_2044', 'BKRCastTAZ']], left_on = 'TAZ_P', right_on = 'BKRCastTAZ', how = 'outer')
kirk_rounded_comparison_TAZ['Diff'] = kirk_rounded_comparison_TAZ['TotHhs_ctrl_2044'] - kirk_rounded_comparison_TAZ['TotHhs_2044']

kirk_rounded_comparison_TAZ.to_csv(os.path.join(working_folder, 'Rounding_Comparison_by_TAZ.csv'), index = False)


updated_parcels_df = psrc_parcels_df.copy()
# clear all jobs in Kirkland. get ready to receive job forecast from Kirkland staff
updated_parcels_df.loc[updated_parcels_df['Jurisdiction'] == 'KIRKLAND', ['HH_P'] + job_col_list] = 0
updated_parcels_df.drop(columns = ['PSRC_ID', 'Jurisdiction'], inplace = True)
updated_parcels_df.set_index('PARCELID', inplace = True)
selected_cols = job_col_list.copy()
selected_cols.extend(['PARCELID'])
kirk_parcels_df_sel_cols = kirk_parcels_df[selected_cols]
kirk_parcels_df_sel_cols.set_index('PARCELID', inplace = True)
updated_parcels_df.update(kirk_parcels_df_sel_cols)
updated_parcels_df[job_col_list] = updated_parcels_df[job_col_list].round(0).astype(int)

updated_parcels_df.to_csv(os.path.join(working_folder, updated_land_use_file), sep = ' ', index = True)

utility.backupScripts(__file__, os.path.join(working_folder, os.path.basename(__file__)))
print('Done')

