from configparser import ConverterMapping
from pickle import TRUE
from re import T
from this import d
import pandas as pd
import numpy as np
import datetime
import os
from sqlalchemy import create_engine

'''
10/14/2022  
   Soundcast starts to use light sql DATABASE (datafile: soundcast_inputs.db in inputs/db folder) to manage a number of input files. BKRCast does not do that. 
    In order to keep consistent with Soundcast in supplemental module, we need to read each individual input file.

    All tables inside the db are exported to csv file, each table in one csv. 

    The following tables are also converted to meet BKRCast input format.
        enlisted_personnel
        external_trip_distribution
        psrc_zones.csv
        auto_externals.csv
        group_quarters.csv
        heavy_trucks.csv
        seatac.csv
        special_generators.csv
        external_unadjusted.csv
        jblm_trips.csv
        time_of_day_factors.csv
        truck_time_of_day_factors.csv

 

'''

input_db_file = r"D:\Soundcast\SC2050_input_only\soundcast_inputs_01262024.db"
parcel_lookup_file = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'
tazSharesFileName = r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\psrc_to_bkr.txt"

output_folder = r'D:\Soundcast\SC_input_db_export_01262024'
to_export_tables  = True


def export_tables_from_SC_input_db(db_file, output_folder):
    '''
       export all tables from db_file to output_folder. exported files are named as table_name + '.csv' 
    '''
    conn = create_engine('sqlite:///' + db_file)
    table_name_list = conn.table_names()
    num = 0
    for name in table_name_list:
        query = f'SELECT * FROM {name}'
        current_table = pd.read_sql(query, con=conn)
        current_table.to_csv(os.path.join(output_folder, name) + '.csv', index = False)
        num += 1

    with open(os.path.join(output_folder, 'read_me.txt'), 'w') as f:
        f.write(str(datetime.datetime.now()))
        f.write('\n')
        f.write(f'database: {input_db_file}\n')
        f.write(f'total {num} tables are exported.\n')
        for name in table_name_list:
            f.write(f'  {name}\n')

    print(f'Totally {num} tables exported to {output_folder}')

def convert_military_jobs_to_bkr(folder, filename):
    '''
        convert military job file to bkrcast format. Soundcast filename is enlisted_personnel.csv
    '''
    military_df = pd.read_csv(os.path.join(folder, filename))
    parcel_lookup_df = pd.read_csv(parcel_lookup_file, low_memory = False)
    military_df = military_df.merge(parcel_lookup_df[['PSRC_ID', 'BKRCastTAZ']], left_on = 'ParcelID', right_on = 'PSRC_ID', how = 'left')
    military_df.drop(['PSRC_ID', 'Zone'], axis = 1, inplace = True)
    military_df.rename(columns = {'ParcelID': 'PSRC_ID'}, inplace = True)
    military_df.fillna(0, inplace = True)
    fn = os.path.basename(filename).split('.')[0] + '_bkr.csv'
    
    military_df.to_csv(os.path.join(folder, fn), index = False)

def convert_work_trip_ixxi_to_bkr(folder, filename):
    '''
        convert soundcast's external trip distribution file to bkrcast format. Soundcast filename is external_trip_distribution.csv 
        External_Station (PSRC_TAZ): include JBLM Taz list: [3061, 3070, 3346, 3348, 3349, 3350, 3351, 3352, 3353, 3354, 3355, 3356], plus regular external stations
            3733 ~ 3750
'   '''
    work_ixxi_df = pd.read_csv(os.path.join(folder, filename))
    tazshare_df = pd.read_table(tazSharesFileName)
    ixxi_cols = ['Total_IE', 'Total_EI', 'SOV_Veh_IE', 'SOV_Veh_EI','HOV2_Veh_IE','HOV2_Veh_EI','HOV3_Veh_IE','HOV3_Veh_EI']
    work_ixxi_df = work_ixxi_df[['PSRC_TAZ','External_Station'] + ixxi_cols]
    work_ixxi_grp_df = work_ixxi_df.groupby(['PSRC_TAZ', 'External_Station']).sum().reset_index()

    # convert values in External_Station from psrc taz to BKRCast TAZ through taz lookup table.
    work_ixxi_grp_bkr_df = pd.merge(work_ixxi_grp_df, tazshare_df[['psrc_zone_id', 'bkr_zone_id']], left_on = 'External_Station', right_on = 'psrc_zone_id', how = 'left')
    work_ixxi_grp_bkr_df['External_Station'] = work_ixxi_grp_bkr_df['bkr_zone_id']
    work_ixxi_grp_bkr_df.drop(['bkr_zone_id', 'psrc_zone_id'], axis = 1, inplace = True)

    # add corresponding BKRCastTAZ field. recalculate trips based on PSRC_TAZ/BKRCastTAZ lookup table.
    work_ixxi_grp_bkr_df = pd.merge(work_ixxi_grp_bkr_df, tazshare_df, left_on = 'PSRC_TAZ', right_on = 'psrc_zone_id', how = 'left')
    work_ixxi_grp_bkr_df.rename(columns = {'bkr_zone_id':'BKRCastTAZ'}, inplace = True)
    work_ixxi_grp_bkr_df.drop(['psrc_zone_id'], axis = 1, inplace = True)
    for col in ixxi_cols:
        work_ixxi_grp_bkr_df[col] = work_ixxi_grp_bkr_df[col] * work_ixxi_grp_bkr_df['percent']

    work_ixxi_grp_bkr_df = work_ixxi_grp_bkr_df.groupby(['BKRCastTAZ', 'External_Station']).sum()
    fn = os.path.basename(filename).split('.')[0] + '_bkr.csv'

    work_ixxi_grp_bkr_df.to_csv(os.path.join(output_folder, fn), index = True)
 
def convert_PSRC_zones_to_BKR(folder, filename):
    '''
       convert PSRC_zones.csv to BKR_zones.csv. 
    '''
    PSRC_zones_df = pd.read_csv(os.path.join(folder, filename))
    tazshare_df = pd.read_table(tazSharesFileName)
    BKR_zones_df = pd.merge(PSRC_zones_df, tazshare_df, left_on = 'taz', right_on = 'psrc_zone_id', how = 'left')
    BKR_zones_df = BKR_zones_df.groupby('bkr_zone_id').agg({'county':'first', 'jblm':'first', 'external':'first'}).reset_index()
    BKR_zones_df.rename(columns = {'bkr_zone_id':'BKRCastTAZ'}, inplace = True)
    BKR_zones_df.loc[BKR_zones_df['BKRCastTAZ'] == 1319, 'jblm'] = 1
    
    fn = os.path.basename(filename).split('.')[0] + '_bkr.csv'
    BKR_zones_df.to_csv(os.path.join(output_folder, 'bkr_zones.csv'), index = False)

def convert_auto_externals_to_BKR(folder, filename):
    auto_externals_df = pd.read_csv(os.path.join(folder, filename))
    tazshare_df = pd.read_table(tazSharesFileName)
    auto_externals_df = pd.merge(auto_externals_df, tazshare_df, left_on = 'taz', right_on = 'psrc_zone_id', how = 'left')
    auto_externals_df.drop(['percent', 'psrc_zone_id', 'taz'], axis = 1, inplace = True)
    auto_externals_df.rename(columns = {'bkr_zone_id':'BKRCastTAZ'}, inplace = True)
    
    agg_op = {}
    for name in auto_externals_df.columns:
        if name == 'year' or name == 'record':
            agg_op[name] = 'first'
        elif name == 'BKRCastTAZ':
            continue
        else:
            agg_op[name] = 'sum'
            
    auto_externals_df = auto_externals_df.groupby('BKRCastTAZ').agg(agg_op).reset_index()
    fn = os.path.basename(filename).split('.')[0] + '_bkr.csv'
    auto_externals_df.to_csv(os.path.join(output_folder, fn), index = False)
 
def convert_group_quarters_to_bkr(folder, filename):
    group_quarters_df = pd.read_csv(os.path.join(folder, filename))
    tazshare_df = pd.read_table(tazSharesFileName)
    group_quarters_df = pd.merge(group_quarters_df, tazshare_df, left_on = 'taz', right_on = 'psrc_zone_id', how = 'left')
    group_quarters_df['group_quarters'] = group_quarters_df['group_quarters'] * group_quarters_df['percent']
    group_quarters_df.drop(['percent', 'psrc_zone_id', 'taz'], axis = 1, inplace = True)
    group_quarters_df.rename(columns={'bkr_zone_id':'BKRCastTAZ'}, inplace = True)
    fn = os.path.basename(filename).split('.')[0] + '_bkr.csv'
    group_quarters_df.to_csv(os.path.join(output_folder, fn), index = False)


def convert_jblm_trips_to_bkr(folder, filename):
    jblm_trips_df = pd.read_csv(os.path.join(folder, filename))
    tazshare_df = pd.read_table(tazSharesFileName)
    jblm_trips_df = pd.merge(jblm_trips_df, tazshare_df[['psrc_zone_id', 'bkr_zone_id', 'percent']], left_on = 'origin_zone', right_on = 'psrc_zone_id', how = 'left')
    jblm_trips_df['trips'] = jblm_trips_df['trips'] * jblm_trips_df['percent']
    jblm_trips_df.drop(['origin_zone', 'psrc_zone_id', 'percent'], axis = 1, inplace = True)
    jblm_trips_df.rename(columns = {'bkr_zone_id':'origin_zone'}, inplace = True)

    jblm_trips_df = pd.merge(jblm_trips_df, tazshare_df[['psrc_zone_id', 'bkr_zone_id', 'percent']], left_on = 'destination_zone', right_on = 'psrc_zone_id', how = 'left')
    jblm_trips_df['trips'] = jblm_trips_df['trips'] * jblm_trips_df['percent']
    jblm_trips_df.drop(['destination_zone', 'psrc_zone_id', 'percent'], axis = 1, inplace = True)
    jblm_trips_df.rename(columns = {'bkr_zone_id':'destination_zone'}, inplace = True)

    fn = os.path.basename(filename).split('.')[0] + '_bkr.csv'
    jblm_trips_df.to_csv(os.path.join(output_folder, fn), index = False)

def convert_heavy_trucks_to_bkr(folder, filename):
    heavy_trucks_df = pd.read_csv(os.path.join(folder, filename))
    tazshare_df = pd.read_table(tazSharesFileName)
    heavy_trucks_df = pd.merge(heavy_trucks_df, tazshare_df, left_on = 'taz', right_on = 'psrc_zone_id', how = 'left')
    heavy_trucks_df['htkpro'] *= heavy_trucks_df['percent']
    heavy_trucks_df['htkatt'] *= heavy_trucks_df['percent']
    heavy_trucks_df.drop(['atri_zone', 'taz', 'psrc_zone_id', 'percent'], axis = 1, inplace = True)
    heavy_trucks_df.rename(columns = {'bkr_zone_id':'BKRCastTAZ'}, inplace = True)
    fn = os.path.basename(filename).split('.')[0] + '_bkr.csv'
    heavy_trucks_df.to_csv(os.path.join(output_folder, fn), index = False)

def convert_seatac_to_bkr(folder, filename):
    seatac_df = pd.read_csv(os.path.join(folder, filename))
    seatac_df.loc[seatac_df['taz'] == 983, 'taz'] = 1356
    seatac_df.rename(columns = {'taz':'BKRCastTAZ'}, inplace = True)
    fn = os.path.basename(filename).split('.')[0] + '_bkr.csv'
    seatac_df.to_csv(os.path.join(output_folder, fn), index = False)

def convert_special_generator_to_bkr(folder, filename):
    sp_df = pd.read_csv(os.path.join(folder, filename))
    sp_df.loc[sp_df['taz'] == 438, 'taz'] = 1358  # Seattle center
    sp_df.loc[sp_df['taz'] == 631, 'taz'] = 1359  # exibition center
    sp_df.loc[sp_df['taz'] == 3110, 'taz'] = 1357 # Tacoma Dome
    sp_df.rename(columns = {'taz':'BKRCastTAZ'}, inplace = True)
    fn = os.path.basename(filename).split('.')[0] + '_bkr.csv'
    sp_df.to_csv(os.path.join(output_folder, fn), index = False)

def convert_externals_unadjusted_to_bkr(folder, filename):
    external_unadjusted_df = pd.read_csv(os.path.join(folder, filename))
    cols = external_unadjusted_df.columns.drop('taz')
    tazshare_df = pd.read_table(tazSharesFileName)
    external_unadjusted_df = pd.merge(external_unadjusted_df, tazshare_df, left_on = 'taz', right_on = 'psrc_zone_id', how = 'left')
    for col in cols:
        external_unadjusted_df[col] = external_unadjusted_df[col] * external_unadjusted_df['percent']
    external_unadjusted_df.drop(['psrc_zone_id', 'taz', 'percent'], axis = 1, inplace = True)
    external_unadjusted_df.rename(columns = {'bkr_zone_id':'BKRCastTAZ'}, inplace = True)
    external_unadjusted_df = external_unadjusted_df.groupby('BKRCastTAZ').sum().reset_index()
    fn = os.path.basename(filename).split('.')[0] + '_bkr.csv'
    external_unadjusted_df.to_csv(os.path.join(output_folder, fn), index = False)

def convert_tod_factors_to_bkr(folder, filename):
    tod_factors_df = pd.read_csv(os.path.join(folder, filename))
    tod_mapping = {'5to6' : '1830to6', '6to7' : '6to9', '7to8' : '6to9', '8to9' : '6to9', 
                       '9to10' : '9to1530', '10to14' : '9to1530', '14to15' : '9to1530', '15to1530':'9to1530',
                       '1530to16' : '1530to1830', '16to17' : '1530to1830', '17to18' : '1530to1830', '18to1830':'1530to1830',
                       '1830to20' : '1830to6', '20to5' : '1830to6'}

    # create factors to split 15to16.
    temp_df = tod_factors_df.loc[tod_factors_df['time_of_day'] == '15to16'].copy()
    temp_df['time_of_day'] = '15to1530'
    temp_df['value'] *= 0.5
    tod_factors_df.drop(tod_factors_df[tod_factors_df['time_of_day']  == '15to16'].index, inplace = True)
    tod_factors_df = pd.concat([tod_factors_df, temp_df]).reset_index(drop = True)
    temp_df['time_of_day'] = '1530to16'
    tod_factors_df = pd.concat([tod_factors_df, temp_df]).reset_index(drop = True)

    # create factors to split 18to20
    temp_df = tod_factors_df.loc[tod_factors_df['time_of_day'] == '18to20'].copy()
    temp_df['time_of_day'] = '18to1830'
    temp_df['value'] *= 0.25
    tod_factors_df = pd.concat([tod_factors_df, temp_df]).reset_index(drop = True)
    temp_df = tod_factors_df.loc[tod_factors_df['time_of_day'] == '18to20'].copy()
    temp_df['time_of_day'] = '1830to20'
    temp_df['value'] *= 0.75
    tod_factors_df = pd.concat([tod_factors_df, temp_df]).reset_index(drop = True)

    tod_factors_df.drop(tod_factors_df[tod_factors_df['time_of_day'] == '18to20'].index, inplace = True)

    tod_factors_df.replace({'time_of_day':tod_mapping}, inplace = True)
    tod_factors_df = tod_factors_df.groupby(['time_of_day', 'mode']).sum().reset_index()
    fn = os.path.basename(filename).split('.')[0] + '_bkr.csv'
    tod_factors_df.to_csv(os.path.join(output_folder, fn), index = False)

def convert_truck_tod_to_bkr(output_folder, filename):
    truck_tod_df = pd.read_csv(os.path.join(output_folder, filename))
    truck_tod_bkr_df = truck_tod_df.loc[truck_tod_df['time_period'].isin(['am','md', 'pm'])]
    truck_ni_df =  truck_tod_df.loc[truck_tod_df['time_period'].isin(['ev','ni'])].groupby('truck_type').sum().reset_index()
    truck_ni_df['time_period'] = 'ni'
    truck_tod_bkr_df = pd.concat([truck_tod_bkr_df, truck_ni_df]).sort_values('truck_type')
    fn = os.path.basename(filename).split('.')[0] + '_bkr.csv'
    truck_tod_bkr_df.to_csv(os.path.join(output_folder, fn), index = False)

def shorten_running_emission_rates_by_veh_type(output_folder, filename):
    df = pd.read_csv(os.path.join(output_folder, filename))
    df_kingc = df.loc[df['county'] == 'king']    
    df_kingc.drop(columns = ['field1'], inplace = True)    
    fn = os.path.basename(filename).split('.')[0] + '_bkr.csv'    
    df_kingc.to_csv(os.path.join(output_folder, fn), index = False)        

def main():

    if to_export_tables == True:
        export_tables_from_SC_input_db(input_db_file, output_folder)


    convert_military_jobs_to_bkr(output_folder, 'enlisted_personnel.csv')
    convert_work_trip_ixxi_to_bkr(output_folder, 'external_trip_distribution.csv')
    convert_PSRC_zones_to_BKR(output_folder, 'psrc_zones.csv')
    convert_auto_externals_to_BKR(output_folder, 'auto_externals.csv')
    convert_group_quarters_to_bkr(output_folder, 'group_quarters.csv')
    convert_heavy_trucks_to_bkr(output_folder, 'heavy_trucks.csv')
    convert_seatac_to_bkr(output_folder, 'seatac.csv')
    convert_special_generator_to_bkr(output_folder, 'special_generators.csv')
    convert_externals_unadjusted_to_bkr(output_folder, 'externals_unadjusted.csv')
    convert_jblm_trips_to_bkr(output_folder, 'jblm_trips.csv')
    convert_tod_factors_to_bkr(output_folder, 'time_of_day_factors.csv')
    convert_truck_tod_to_bkr(output_folder, 'truck_time_of_day_factors.csv')
    shorten_running_emission_rates_by_veh_type(output_folder, 'running_emission_rates_by_veh_type.csv')    


    print('Done')

if __name__ == '__main__':
    main()
    
