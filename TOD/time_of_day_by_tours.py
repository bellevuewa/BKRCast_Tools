import os
import pandas as pd
import numpy as np


tours_file1 = r'D:\2018baseyear\BKR0V1-02\outputs\_tour.tsv'
tours_file2 = r'D:\2020Concurrency\BKR6V1\outputs\_tour.tsv'

# enter a TAZ list if mode share for a specific subarea is desired. 
# if the list is empty (with the header 'TAZ only), the mode share for the whole region will be calculated.
subarea_taz_file = r'D:\2018baseyear\BKR0V1-02\BellevueDTTAZ.txt'
Output_file = r'D:\2018baseyear\BKR0V1-02\2018_bkr0v1-02_tod.txt'

# Below is the start and end time you want to query from daysim output. It is number of minutes after 12am. 
# if you want 24hr data, set all to 0.
start_time = 0  # minutes starting from 12am
end_time = 0   # minutes starting from 12am

tours_from_only = False  # if true, tours only from the TAZ list
tours_end_only = True    # if true, tours only to the TAZ list
#################################################################################################################

total_tours1_df = pd.DataFrame.from_csv(tours_file1, sep = '\t')
total_tours2_df = pd.DataFrame.from_csv(tours_file2, sep = '\t')
subarea_taz_df = pd.DataFrame.from_csv(subarea_taz_file)
subarea_taz_df.reset_index(inplace = True)

def tours_filter(start_time, end_time, total_tours_df, subarea_taz_df, tours_from_only, tours_end_only):
    if (start_time == 0 and end_time == 0):
        tours_df = total_tours_df
    else:
        tours_df = total_tours_df.loc[(total_tours_df['deptm'] >= start_time) & (total_tours_df['arrtm'] <= end_time)]

    if subarea_taz_df.empty == False:
        if tours_from_only == True:
            tours_df.set_index('totaz', inplace = True)
            from_subarea_tours_df = tours_df.join(subarea_taz_df.set_index('TAZ'), how = 'right')
        if tours_end_only == True:
            tours_df.set_index('tdtaz', inplace = True)
            to_subarea_tours_df = tours_df.join(subarea_taz_df.set_index('TAZ'), how = 'right')
        if ((tours_from_only == True) and (tours_end_only == True)):
            subarea_tours_df = from_subarea_tours_df.merge(subarea_taz_df, left_on = 'tdtaz', right_on = 'TAZ')
        elif tours_from_only == True:
            subarea_tours_df = pd.concat([from_subarea_tours_df])
        else:
            subarea_tours_df = pd.concat([to_subarea_tours_df])
    else:
        print('No subarea is defined. Use the whole tours table.')
        subarea_tours_df = tours_df

    # get the work tours only
    subarea_tours_df = subarea_tours_df.loc[(subarea_tours_df['pdpurp'] == 1)]
    return subarea_tours_df

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

subarea_tours1_df = tours_filter(start_time, end_time, total_tours1_df, subarea_taz_df, tours_from_only, tours_end_only)
subarea_tours1_df['arrhr'] = min_to_hour(subarea_tours1_df['tarorig'], 0)
subarea_tours1_df['dephr'] = min_to_hour(subarea_tours1_df['tlvorig'], 0)

TOD1_by_arrival = subarea_tours1_df[['arrhr', 'toexpfac']].groupby('arrhr').sum()
TOD1_by_arrival.reset_index(inplace = True)
TOD1_by_arrival['share'] = TOD1_by_arrival['toexpfac'] / TOD1_by_arrival['toexpfac'].sum()

TOD1_by_departure = subarea_tours1_df[['dephr', 'toexpfac']].groupby('dephr').sum()
TOD1_by_departure.reset_index(inplace = True)
TOD1_by_departure['share'] = TOD1_by_departure['toexpfac'] / TOD1_by_departure['toexpfac'].sum()

subarea_tours2_df = tours_filter(start_time, end_time, total_tours2_df, subarea_taz_df, tours_from_only, tours_end_only) 
subarea_tours2_df['arrhr'] = min_to_hour(subarea_tours2_df['tarorig'], 0)
subarea_tours2_df['dephr'] = min_to_hour(subarea_tours2_df['tlvorig'], 0)

TOD2_by_arrival = subarea_tours2_df[['arrhr', 'toexpfac']].groupby('arrhr').sum()
TOD2_by_arrival.reset_index(inplace = True)
TOD2_by_arrival['share'] = TOD2_by_arrival['toexpfac'] / TOD2_by_arrival['toexpfac'].sum()

TOD2_by_departure = subarea_tours2_df[['dephr', 'toexpfac']].groupby('dephr').sum()
TOD2_by_departure.reset_index(inplace = True)
TOD2_by_departure['share'] = TOD2_by_departure['toexpfac'] / TOD2_by_departure['toexpfac'].sum()

TOD_by_arrival = TOD1_by_arrival.merge(TOD2_by_arrival, on = 'arrhr')
TOD_by_departure = TOD1_by_departure.merge(TOD2_by_departure, on = 'dephr')


print('Done.')