import pandas as pd
import os

# To calculate mode share from daysim output _trips.tsv. User is allowed to define a subarea in the format of a list of TAZ. 
# If so, the mode share will be calculated for that subarea. Otherwise it will be for the whole region.
# The TAZ file contains only one column named TAZ. The column name cannot be changed to other names.

# 1/29/2019
# New feature: allows to look into mode share by time period

# 2/6/2019
# New feature: allows to select trips starting from subarea_taz_file or ending at subarea_taz_file or both

#8/23/2019
# fixed a bug in trips with both ends in subarea.
##############################################################################################################
# Below are inputs that need to modify

trips_file = r'D:\BKR0V1-1\outputs\_trip.tsv'

# enter a TAZ list if mode share for a specific subarea is desired. 
# if the list is empty (with the header 'TAZ only), the mode share for the whole region will be calculated.
subarea_taz_file = r'D:\BKR0V1-1\BKRTaz.txt'
Output_file = r'D:\BKR0V1-1\2014_BKR_trip_PMPK_mode_share.txt'

Output_file_trip_dist = r'D:\BKR0V1-1\2014_BKR0V1-1_trip_distance_BKR.txt'

# Below is the start and end time you want to query from daysim output. It is number of minutes after 12am. 
# if you want 24hr data, set all to 0.
start_time = 0  # minutes starting from 12am
end_time = 0   # minutes starting from 12am

trips_from_only = True  # if true, trips only from the TAZ list
trips_end_only = True    # if true, trips only to the TAZ list
#################################################################################################################

total_trips_df = pd.DataFrame.from_csv(trips_file, sep = '\t')
if (start_time == 0 and end_time == 0):
    trips_df = total_trips_df
else:
    trips_df = total_trips_df.loc[(total_trips_df['deptm'] >= start_time) & (total_trips_df['arrtm'] <= end_time)]

subarea_taz_df = pd.DataFrame.from_csv(subarea_taz_file)
subarea_taz_df.reset_index(inplace = True)

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

print 'Calculating mode share (all trip purpose)...'
model_df = subarea_trips_df[['mode', 'trexpfac', 'travdist']].groupby('mode').sum()
model_df['share'] = model_df['trexpfac'] / model_df['trexpfac'].sum()
model_df['avgdist'] = model_df['travdist'] / model_df['trexpfac']
model_df.reset_index(inplace = True)
mode_dict = {0:'Other',1:'Walk',2:'Bike',3:'SOV',4:'HOV2',5:'HOV3+',6:'Transit',8:'School Bus'}
model_df.replace({'mode': mode_dict}, inplace = True)
model_df.columns = ['mode', 'trips', 'total_dist', 'share', 'avgdist']
model_df.to_csv(Output_file, float_format = '%.3f')

print 'Calculating mode share (HBW only)...'
hbw_df = subarea_trips_df.loc[(subarea_trips_df['dpurp']==1) | (subarea_trips_df['dpurp']==0)][['mode', 'trexpfac', 'travdist']].groupby('mode').sum()
hbw_df['share'] = hbw_df['trexpfac'] / hbw_df['trexpfac'].sum()
hbw_df['avgdist'] = hbw_df['travdist'] / hbw_df['trexpfac']
hbw_df.reset_index(inplace = True)
hbw_df.replace({'mode': mode_dict}, inplace = True)
hbw_df.columns = ['mode', 'trips',  'total_dist', 'share', 'avgdist']
hbw_df.to_csv(Output_file, float_format = '%.3f', mode = 'a')

print 'Mode share calculation is finished.'

print 'Calculating tirp distance ...'
subtotal_trips = subarea_trips_df['trexpfac'].count()
trips_by_purpose = subarea_trips_df[['dpurp', 'travdist', 'trexpfac']].groupby('dpurp').sum()
trips_by_purpose['share'] = trips_by_purpose['trexpfac'] / subtotal_trips
trips_by_purpose['avgdist'] = trips_by_purpose['travdist'] / trips_by_purpose['trexpfac']
purp_dic = {0: 'home', 1: 'work', 2: 'school', 3: 'escort', 4: 'personal biz', 5: 'shopping', 6: 'meal', 7: 'social', 8: 'rec', 9: 'medical', 10: 'change'}
trips_by_purpose.reset_index(inplace = True)
trips_by_purpose.replace({'dpurp' : purp_dic}, inplace = True)
trips_by_purpose.columns = ['purp', 'dist', 'trips', 'share', 'avgdist']

# distance < 3 miles
subarea_trips_df['dist<=3'] = subarea_trips_df['travdist'] <= 3
trips_by_dist = subarea_trips_df[['travdist', 'dist<=3', 'trexpfac']].groupby('dist<=3').sum()
trips_by_dist['share'] = trips_by_dist['trexpfac'] / subtotal_trips
trips_by_dist['avgdist'] = trips_by_dist['travdist'] / trips_by_dist['trexpfac']

# calculate commute trips
commute_trips = subarea_trips_df.loc[(subarea_trips_df['dpurp']==1) | (subarea_trips_df['dpurp']==0)][['trexpfac', 'travdist', 'dist<=3']].groupby('dist<=3').sum()
commute_trips['share'] = commute_trips['trexpfac'] / commute_trips['trexpfac'].sum()
commute_trips['avgdist'] = commute_trips['travdist'] / commute_trips['trexpfac']

# output to file
with open(Output_file_trip_dist, 'w') as f:
    f.write('Subarea: %s\n' % subarea_taz_file)
    f.write('Trip file: %s\n' % trips_file)
    f.write('Start time: %d\n' % start_time)
    f.write('End time: %d\n' % end_time)
    f.write('Trips only from the defined subarea: %s\n' % trips_from_only)
    f.write('Trips only to the defined subarea: %s\n' % trips_end_only)
    f.write('Total trips within the defined subarea: %d\n' % subtotal_trips)
    f.write('Trips by purpose\n')
    f.write('%s' % trips_by_purpose)
    f.write('\n\n')
    f.write('Trips by distance (for all purpose)\n')
    f.write('%s' % trips_by_dist)
    f.write('\n\n')
    f.write("Distance of commute trips\n")
    f.write('%s' % commute_trips)

print 'Done'
