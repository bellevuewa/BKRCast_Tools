import pandas as pd
import os
import datetime

# To calculate mode share from daysim output _trips.tsv. User is allowed to define a subarea in the format of a list of TAZ. 
# If so, the mode share will be calculated for that subarea. Otherwise it will be for the whole region.
# The TAZ file contains only one column named TAZ. The column name cannot be changed to other names.

# 1/29/2019
# New feature: allows to look into mode share by time period

# 2/6/2019
# New feature: allows to select trips starting from subarea_taz_file or ending at subarea_taz_file or both

#8/23/2019
# fixed a bug in trips with both ends in subarea.
# add trip distance calculation.

# 3/26/2021
# new feature: calculate total trips by HBW, HBSchool, HBO, and NHB 
##############################################################################################################
# Below are inputs that need to modify

# 5/26/2021
# new feature: calculate trips by residence and workplaces


trips_file = r'D:\projects\2018baseyear\outputs\_trip.tsv'
hhs_file = r'D:\projects\2018baseyear\outputs\_household.tsv'

# enter a TAZ list if mode share for a specific subarea is desired. 
# if the list is empty (with the header 'TAZ only), the mode share for the whole region will be calculated.
subarea_taz_file = r'D:\projects\2018baseyear\BellevueDTTAZ.txt'
Output_file = r'D:\projects\2018baseyear\2018_from_BelCBD_to_all_trip_daily_mode_share.txt'

Output_file_trip_dist = r'D:\projects\2018baseyear\2018_from_BelCBD_to_all_daily_trip_distance.txt'

# Below is the start and end time you want to query from daysim output. It is number of minutes after 12am. 
# if you want 24hr data, set all to 0.
start_time = 0  # minutes starting from 12am
end_time = 0   # minutes starting from 12am

trips_from_only = True  # if true, trips only from the TAZ list
trips_end_only = False    # if true, trips only to the TAZ list
#################################################################################################################
mode_dict = {0:'Other',1:'Walk',2:'Bike',3:'SOV',4:'HOV2',5:'HOV3+',6:'Transit',8:'School Bus'}
purp_dict = {-1: 'All_Purpose', 0: 'home', 1: 'work', 2: 'school', 3: 'escort', 4: 'personal_biz', 5: 'shopping', 6: 'meal', 7: 'social', 8: 'rec', 9: 'medical', 10: 'change'}

total_trips_df = pd.read_csv(trips_file, low_memory = False, sep = '\t')
if (start_time == 0 and end_time == 0):
    trips_df = total_trips_df
else:
    trips_df = total_trips_df.loc[(total_trips_df['deptm'] >= start_time) & (total_trips_df['arrtm'] <= end_time)]

subarea_taz_df = pd.read_csv(subarea_taz_file)
subarea_taz_df.reset_index(inplace = True)

if subarea_taz_df.empty == False:
    if trips_from_only == True:
        from_subarea_trips_df = trips_df.merge(subarea_taz_df, left_on = 'otaz', right_on = 'TAZ', how = 'inner')
    if trips_end_only == True:
        to_subarea_trips_df = trips_df.merge(subarea_taz_df, left_on = 'dtaz', right_on = 'TAZ', how = 'inner')
    if ((trips_from_only == True) and (trips_end_only == True)):
        subarea_trips_df = from_subarea_trips_df.merge(subarea_taz_df, left_on = 'dtaz', right_on = 'TAZ')
    elif trips_from_only == True:
        subarea_trips_df = pd.concat([from_subarea_trips_df])
    else:
        subarea_trips_df = pd.concat([to_subarea_trips_df])
else:
    print 'No subarea is defined. Use the whole trip table.'
    subarea_trips_df = trips_df

def write_file_header(output_file, overwritten = False):
    if overwritten:
        file_mode = 'w'
    else:
        file_mode = 'a'
    
    with open(output_file, file_mode) as output:
        output.write(str(datetime.datetime.now()) + '\n')
        output.write(trips_file + '\n')
        output.write(hhs_file + '\n')
        output.write(subarea_taz_file + '\n')
        output.write('Start time: ' + str(start_time) + '\n')
        output.write('End time: ' + str(end_time) + '\n')
        output.write('From the subarea: ' + str(trips_from_only) + '\n')
        output.write('To the subarea: ' + str(trips_end_only) + '\n')
        output.write('\n')

def calculateModeSharebyTripPurpose(purpose, trip_df, Output_file, overwritten=False, comments=''):
    if purpose == -1:
        # all purpose
        model_df = trip_df[['mode', 'trexpfac', 'travdist']].groupby('mode').sum()
    elif purpose >=0 and purpose <= 10:
        model_df = trip_df.loc[((trip_df['opurp'] == 0) & (trip_df['dpurp'] == purpose)) | ((trip_df['opurp'] == purpose) & (trip_df['dpurp'] == 0))][['mode', 'trexpfac', 'travdist']].groupby('mode').sum()
    else:
        print 'Purpose ' + str(purpose) + 'is invalid'
        return

    model_df['share'] = model_df['trexpfac'] / model_df['trexpfac'].sum()
    model_df['avgdist'] = model_df['travdist'] / model_df['trexpfac']
    model_df.reset_index(inplace = True)
    model_df.replace({'mode': mode_dict}, inplace = True)
    model_df.columns = ['mode', 'trips', 'total_dist', 'share', 'avgdist']
    model_df['trips'] = model_df['trips'].astype(int)
    model_df['total_dist'] = model_df['total_dist'].map('{:.1f}'.format)
    model_df['share'] = model_df['share'].map('{:.1%}'.format)
    model_df['avgdist'] = model_df['avgdist'].map('{:.1f}'.format)

    if overwritten:
        filemode = 'w'
    else: 
        filemode = 'a'

    with open(Output_file, filemode) as output:
        output.write(comments + '\n')
        output.write('Mode Share from trips, ' + purp_dict[purpose] + '\n')
        output.write('%s' % model_df)
        output.write('\n\n')

write_file_header(Output_file, overwritten = True)
print 'Calculating mode share (all trip purpose)...'
calculateModeSharebyTripPurpose(-1, subarea_trips_df, Output_file, overwritten = False)

print 'Calculating mode share (HBW only)...'
hbw_df = subarea_trips_df.loc[((subarea_trips_df['oadtyp']==1) & (subarea_trips_df['dadtyp']==2))| ((subarea_trips_df['oadtyp']==2) & (subarea_trips_df['dadtyp']==1))]
calculateModeSharebyTripPurpose(-1, hbw_df, Output_file, overwritten = False, comments = 'HBW')

print 'Calculating mode share by other purposes... '
for purpose in [1,2,3,4,5,6,7, 8, 9, 10]:
    calculateModeSharebyTripPurpose(purpose, subarea_trips_df, Output_file, overwritten = False)

# calculate mode share by residence
print 'Calculating mode share by residence...'
hhs_df = pd.read_csv(hhs_file, sep = '\t' )
hhs_df = hhs_df[['hhno','hhparcel', 'hhtaz']]
trips_by_residence_df = trips_df.merge(hhs_df, left_on = 'hhno', right_on = 'hhno', how = 'left')
trips_by_residence_df = trips_by_residence_df.merge(subarea_taz_df, left_on = 'hhtaz', right_on = 'TAZ', how = 'inner')
calculateModeSharebyTripPurpose(-1, trips_by_residence_df, Output_file, comments = 'by residence only' )
calculateModeSharebyTripPurpose(1, trips_by_residence_df, Output_file, comments = 'by residence only' )

# calculate mode share by workplace
print 'Calculating mode share by workplace...'
trips_to_workplace_df = trips_df.loc[trips_df['dadtyp'] == 2].merge(subarea_taz_df, left_on = 'dtaz', right_on = 'TAZ', how = 'inner')
trips_from_workplace_df = trips_df.loc[trips_df['oadtyp'] == 2].merge(subarea_taz_df, left_on = 'otaz', right_on = 'TAZ', how = 'inner')
trips_by_workplace_df = pd.concat([trips_to_workplace_df, trips_from_workplace_df])
calculateModeSharebyTripPurpose(-1, trips_by_workplace_df, Output_file, comments = 'by workplace only')
calculateModeSharebyTripPurpose(1, trips_by_workplace_df, Output_file, comments = 'by workplace only')
print 'Mode share calculation is finished.'

print 'Calculating tirp distance ...'
subtotal_trips = subarea_trips_df['trexpfac'].count()
trips_by_purpose = subarea_trips_df[['dpurp', 'travdist', 'trexpfac']].groupby('dpurp').sum()
trips_by_purpose['share'] = trips_by_purpose['trexpfac'] / subtotal_trips
trips_by_purpose['avgdist'] = trips_by_purpose['travdist'] / trips_by_purpose['trexpfac']
trips_by_purpose.reset_index(inplace = True)
trips_by_purpose.replace({'dpurp' : purp_dict}, inplace = True)
trips_by_purpose.columns = ['purp', 'dist', 'trips', 'share', 'avgdist']
trips_by_purpose['avgdist'] = trips_by_purpose['avgdist'].map('{:.1f}'.format)
trips_by_purpose['dist'] = trips_by_purpose['dist'].map('{:.1f}'.format)
trips_by_purpose['share'] = trips_by_purpose['share'].map('{:.1%}'.format)
trips_by_purpose['trips'] = trips_by_purpose['trips'].astype(int)

# distance < 3 miles
subarea_trips_df['dist<=3'] = subarea_trips_df['travdist'] <= 3
trips_by_dist = subarea_trips_df[['travdist', 'dist<=3', 'trexpfac']].groupby('dist<=3').sum()
trips_by_dist['share'] = trips_by_dist['trexpfac'] / subtotal_trips
trips_by_dist['avgdist'] = trips_by_dist['travdist'] / trips_by_dist['trexpfac']
trips_by_dist['avgdist'] = trips_by_dist['avgdist'].map('{:.1f}'.format)
trips_by_dist['share'] = trips_by_dist['share'].map('{:.1%}'.format)
trips_by_dist['travdist'] = trips_by_dist['travdist'].map('{:.1f}'.format)

# calculate commute trips
commute_trips = subarea_trips_df.loc[(subarea_trips_df['dpurp']==1) | (subarea_trips_df['dpurp']==0)][['trexpfac', 'travdist', 'dist<=3']].groupby('dist<=3').sum()
commute_trips['share'] = commute_trips['trexpfac'] / commute_trips['trexpfac'].sum()
commute_trips['avgdist'] = commute_trips['travdist'] / commute_trips['trexpfac']
commute_trips['avgdist'] = commute_trips['avgdist'].map('{:.1f}'.format)
commute_trips['share'] = commute_trips['share'].map('{:.1%}'.format)
commute_trips['travdist'] = commute_trips['travdist'].map('{:.1f}'.format)

print 'Total trips: ' + str(subtotal_trips)
# NHB trips calculation
nhb_df = subarea_trips_df[['oadtyp','dadtyp', 'otaz', 'dtaz', 'trexpfac', 'opurp', 'dpurp']]
nhb_df = nhb_df.loc[(nhb_df['oadtyp'] != 1) & (nhb_df['dadtyp'] != 1)]
nhb_counts = nhb_df['trexpfac'].sum()
print 'Total NHB trips: ' + str(nhb_counts)

# HBW trip calculation
hbw_df = subarea_trips_df[['oadtyp','dadtyp','otaz', 'dtaz', 'trexpfac', 'opurp', 'dpurp']]
hbw_df = hbw_df.loc[((hbw_df['oadtyp'] == 1) & (hbw_df['dadtyp'] == 2)) | ((hbw_df['oadtyp'] == 2) & (hbw_df['dadtyp'] == 1))]
hbw_counts = hbw_df['trexpfac'].sum()
print 'Total NBW trips: ' + str(hbw_counts)

# HBSchool trip calculation
hbsch_df = subarea_trips_df[['oadtyp','dadtyp','otaz', 'dtaz', 'trexpfac', 'opurp', 'dpurp']]
hbsch_df = hbsch_df.loc[((hbsch_df['oadtyp'] == 1) & (hbsch_df['dadtyp'] == 3)) | ((hbsch_df['oadtyp'] == 3) & (hbsch_df['dadtyp'] == 1))]
hbsch_counts = hbsch_df['trexpfac'].sum()
print 'Total HBSchool trips: ' + str(hbsch_counts)

# HBO trip calculation
hbo_df = subarea_trips_df[['oadtyp','dadtyp','otaz', 'dtaz', 'trexpfac', 'opurp', 'dpurp']]
hbo_df = hbo_df.loc[((hbo_df['oadtyp'] == 1) & (hbo_df['dadtyp'] > 3)) | ((hbo_df['oadtyp'] > 3) & (hbo_df['dadtyp'] == 1))]
hbo_counts = hbo_df['trexpfac'].sum()
print 'Total HBO trips: ' + str(hbo_counts)

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
    f.write('\n\n')
    f.write('Total trips: %d\n' % subtotal_trips)
    f.write('HBW trips: %d\n' % hbw_counts)
    f.write('HBSchool trips: %d\n' % hbsch_counts)
    f.write('HBO trips: %d\n' % hbo_counts)
    f.write('NHB trips: %d\n' % nhb_counts)
print 'Done'
