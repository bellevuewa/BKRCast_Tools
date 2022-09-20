import os
import pandas as pd
import numpy as np


trips_file1 = r'D:\WFH_test\base_year\BKR3-19\outputs\_trip.tsv'
trips_file2 = r'D:\WFH_test\BKR3-19-WFH-test1a\outputs\_updated_trips_df.tsv'

# enter a TAZ list if mode share for a specific subarea is desired. 
# if the list is empty (with the header 'TAZ only), the mode share for the whole region will be calculated.
subarea_taz_file = r'D:\WFH_test\BKR3-19-WFH-test1a\tazlist.txt'
Output_file = r'D:\WFH_test\BKR3-19-WFH-test1a\2019_TAZ_1193.csv'

# Below is the start and end time you want to query from daysim output. It is number of minutes after 12am. 
# if you want 24hr data, set all to 0.
start_time = 0  # minutes starting from 12am
end_time = 0   # minutes starting from 12am

trips_from_only = True  # if true, trips only from the TAZ list
trips_end_only = False    # if true, trips only to the TAZ list
#################################################################################################################

purp_dict = {-1: 'All_Purpose', 0: 'home', 1: 'work', 2: 'school', 3: 'escort', 4: 'personal_biz', 5: 'shopping', 6: 'meal', 7: 'social', 8: 'rec', 9: 'medical', 10: 'change'}

total_trips1_df = pd.read_csv(trips_file1, sep = '\t')
total_trips2_df = pd.read_csv(trips_file2, sep = '\t')
subarea_taz_df = pd.read_csv(subarea_taz_file)
subarea_taz_df.reset_index(inplace = True)

def trip_filter(start_time, end_time, total_trips_df, subarea_taz_df, trips_from_only, trips_end_only):
    if (start_time == 0 and end_time == 0):
        trips_df = total_trips_df
    else:
        trips_df = total_trips_df.loc[(total_trips_df['deptm'] >= start_time) & (total_trips_df['arrtm'] <= end_time)]

    if subarea_taz_df.empty == False:
        if trips_from_only == True:
            trips_df.set_index('otaz', inplace = True)
            from_subarea_trips_df = trips_df.join(subarea_taz_df.set_index('TAZ'), how = 'right')
        if trips_end_only == True:
            trips_df.set_index('dtaz', inplace = True)
            to_subarea_trips_df = trips_df.join(subarea_taz_df.set_index('TAZ'), how = 'right')
        if ((trips_from_only == True) and (trips_end_only == True)):
            subarea_trips_df = from_subarea_trips_df.merge(subarea_taz_df, left_on = 'dtaz', right_on = 'TAZ')
        elif trips_from_only == True:
            subarea_trips_df = pd.concat([from_subarea_trips_df])
        else:
            subarea_trips_df = pd.concat([to_subarea_trips_df])
    else:
        print('No subarea is defined. Use the whole trip table.')
        subarea_trips_df = trips_df
    return subarea_trips_df

def min_to_hour(input, base): #Converts minutes since a certain time of the day to hour of the day
    timemap = {}
    for i in range(0, 24):
        if i + base < 24:
            for j in range(0, 60):
                if i + base < 9:
                    timemap.update({i * 60 + j: '0' + str(i + base) + ' - 0' + str(i + base + 1)})
                elif i + base == 9:
                    timemap.update({i * 60 + j: '0' + str(i + base) + ' - ' + str(i + base + 1)})
                else:
                    timemap.update({i * 60 + j: str(i + base) + ' - ' + str(i + base + 1)})
        else:
            for j in range(0, 60):
                if i + base - 24 < 9:
                    timemap.update({i * 60 + j: '0' + str(i + base - 24) + ' - 0' + str(i + base - 23)})
                elif i + base - 24 == 9:
                    timemap.update({i * 60 + j: '0' + str(i + base - 24) + ' - ' + str(i + base - 23)})
                else:
                    timemap.update({i * 60 + j:str(i + base - 24) + ' - ' + str(i + base - 23)})
    output = input.map(timemap)
    return output

subarea_trips1_df = trip_filter(start_time, end_time, total_trips1_df, subarea_taz_df, trips_from_only, trips_end_only)
subarea_trips1_df['arrhr'] = min_to_hour(subarea_trips1_df['arrtm'], 0)
subarea_trips1_df['dephr'] = min_to_hour(subarea_trips1_df['deptm'], 0)

TOD1_by_arrival = subarea_trips1_df[['arrhr', 'trexpfac']].groupby('arrhr').sum()
TOD1_by_arrival.reset_index(inplace = True)
TOD1_by_arrival['share'] = TOD1_by_arrival['trexpfac'] / TOD1_by_arrival['trexpfac'].sum()
TOD1_by_arrival.rename(columns = {'trexpfac' : 'trips_case1', 'share':'share_case1'}, inplace = True)

TOD1_by_departure = subarea_trips1_df[['dephr', 'trexpfac']].groupby('dephr').sum()
TOD1_by_departure.reset_index(inplace = True)
TOD1_by_departure['share'] = TOD1_by_departure['trexpfac'] / TOD1_by_departure['trexpfac'].sum()
TOD1_by_departure.rename(columns = {'trexpfac' : 'trips_case1', 'share':'share_case1'}, inplace = True)

subarea_trips2_df = trip_filter(start_time, end_time, total_trips2_df, subarea_taz_df, trips_from_only, trips_end_only) 
subarea_trips2_df['arrhr'] = min_to_hour(subarea_trips2_df['arrtm'], 0)
subarea_trips2_df['dephr'] = min_to_hour(subarea_trips2_df['deptm'], 0)

TOD2_by_arrival = subarea_trips2_df[['arrhr', 'trexpfac']].groupby('arrhr').sum()
TOD2_by_arrival.reset_index(inplace = True)
TOD2_by_arrival['share'] = TOD2_by_arrival['trexpfac'] / TOD2_by_arrival['trexpfac'].sum()
TOD2_by_arrival.rename(columns = {'trexpfac' : 'trips_case2', 'share':'share_case2'}, inplace = True)

TOD2_by_departure = subarea_trips2_df[['dephr', 'trexpfac']].groupby('dephr').sum()
TOD2_by_departure.reset_index(inplace = True)
TOD2_by_departure['share'] = TOD2_by_departure['trexpfac'] / TOD2_by_departure['trexpfac'].sum()
TOD2_by_departure.rename(columns = {'trexpfac' : 'trips_case2', 'share':'share_case2'}, inplace = True)

TOD_by_arrival = pd.merge(TOD1_by_arrival, TOD2_by_arrival, on = 'arrhr', how = 'outer')
TOD_by_departure = pd.merge(TOD1_by_departure, TOD2_by_departure, on = 'dephr', how = 'outer')

trips1_purpose_df = subarea_trips1_df[['dpurp', 'trexpfac']].groupby('dpurp').sum()
trips1_purpose_df.rename(columns = {'trexpfac':'trips_case1'}, inplace = True)
#subarea_trips1_df.replace({'dpurp' : purp_dict}, inplace = True)
trips2_purpose_df = subarea_trips2_df[['dpurp', 'trexpfac']].groupby('dpurp').sum()
trips2_purpose_df.rename(columns = {'trexpfac':'trips_case2'}, inplace = True)
trips_purp_df = pd.merge(trips1_purpose_df, trips2_purpose_df, left_index = True, right_index = True, how = 'outer')
trips_purp_df.reset_index(inplace = True)
trips_purp_df.replace({'dpurp' : purp_dict}, inplace = True)

with open(Output_file, 'w') as f:
    f.write('case1: %s\n' % trips_file1)
    f.write('case2: %s\n' % trips_file2)
    f.write('subarea: %s\n' % subarea_taz_file)
    f.write('start time: %d\n' % start_time)
    f.write('end time: %d\n' % end_time)
    f.write('trips_from_subarea_only: %s\n' % trips_from_only)
    f.write('trips_end_subarea_only: %s\n\n\n' % trips_end_only)

    f.write('%s' % TOD_by_arrival)
    f.write('\n\n')
    f.write('%s' % TOD_by_departure)

    f.write('\n\n')
    f.write('Trips by purpose\n')
    f.write('%s' % trips_purp_df)

print('Done.')