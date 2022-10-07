from configparser import ConverterMapping
from re import T
from this import d
import pandas as pd
import numpy as np
import datetime
import os
from sqlalchemy import create_engine

'''
    Soundcast starts to use light sql DATABASE (datafile: soundcast_inputs.db in inputs/db folder) to manage a number of input files. BKRCast does not do that. 
    In order to keep consistent with Soundcast in supplemental module, we need to read each individual input file.

    All tables inside the db are exported to csv file, each table in one csv. 

    The following tables are also converted to meet BKRCast input format.
        enlisted_personnel
        external_trip_distribution
'''

input_db_file = r"D:\Soundcast\soundcast\inputs\db\soundcast_inputs.db"
parcel_lookup_file = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'
tazSharesFileName = r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\psrc_to_bkr.txt"

input_year = 2014
output_folder = r'D:\Soundcast\SC_input_db_export'
to_export_tables  = False


def export_tables_from_SC_input_db(db_file, input_year, output_folder):
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
    

def main():

    if to_export_tables == True:
        export_tables_from_SC_input_db(input_db_file, input_year, output_folder)


    convert_military_jobs_to_bkr(output_folder, 'enlisted_personnel.csv')
    convert_work_trip_ixxi_to_bkr(output_folder, 'external_trip_distribution.csv')

    
    print('Done')

if __name__ == '__main__':
    main()
    
