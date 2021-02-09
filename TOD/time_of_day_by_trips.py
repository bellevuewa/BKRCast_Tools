import os
import pandas as pd
import numpy as np


trips_file1 = r'D:\2018baseyear\BKR0V1-02\outputs\_trip.tsv'
trips_file2 = r'D:\2020Concurrency\BKR6V1\outputs\_trip.tsv'

# enter a TAZ list if mode share for a specific subarea is desired. 
# if the list is empty (with the header 'TAZ only), the mode share for the whole region will be calculated.
subarea_taz_file = r'D:\2018baseyear\BKR0V1-02\BellevueDTTAZ.txt'
Output_file = r'D:\2018baseyear\BKR0V1-02\2018_bkr0v1-02_tod.txt'

# Below is the start and end time you want to query from daysim output. It is number of minutes after 12am. 
# if you want 24hr data, set all to 0.
start_time = 0  # minutes starting from 12am
end_time = 0   # minutes starting from 12am

trips_from_only = True  # if true, trips only from the TAZ list
trips_end_only = False    # if true, trips only to the TAZ list
#################################################################################################################

total_trips1_df = pd.DataFrame.from_csv(trips_file1, sep = '\t')
total_trips2_df = pd.DataFrame.from_csv(trips_file2, sep = '\t')
subarea_taz_df = pd.DataFrame.from_csv(subarea_taz_file)
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
        print 'No subarea is defined. Use the whole trip table.'
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

TOD1_by_departure = subarea_trips1_df[['dephr', 'trexpfac']].groupby('dephr').sum()
TOD1_by_departure.reset_index(inplace = True)
TOD1_by_departure['share'] = TOD1_by_departure['trexpfac'] / TOD1_by_departure['trexpfac'].sum()

subarea_trips2_df = trip_filter(start_time, end_time, total_trips2_df, subarea_taz_df, trips_from_only, trips_end_only) 
subarea_trips2_df['arrhr'] = min_to_hour(subarea_trips2_df['arrtm'], 0)
subarea_trips2_df['dephr'] = min_to_hour(subarea_trips2_df['deptm'], 0)

TOD2_by_arrival = subarea_trips2_df[['arrhr', 'trexpfac']].groupby('arrhr').sum()
TOD2_by_arrival.reset_index(inplace = True)
TOD2_by_arrival['share'] = TOD2_by_arrival['trexpfac'] / TOD2_by_arrival['trexpfac'].sum()

TOD2_by_departure = subarea_trips2_df[['dephr', 'trexpfac']].groupby('dephr').sum()
TOD2_by_departure.reset_index(inplace = True)
TOD2_by_departure['share'] = TOD2_by_departure['trexpfac'] / TOD2_by_departure['trexpfac'].sum()

TOD_by_arrival = TOD1_by_arrival.merge(TOD2_by_arrival, on = 'arrhr')

print 'Done.'