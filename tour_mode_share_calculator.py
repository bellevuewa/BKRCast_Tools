import pandas as pd
import os

# To calculate mode share (from tours) from daysim output _tour.tsv. User is allowed to define a subarea in the format of a list of TAZ. 
# If so, the mode share will be calculated for that subarea. Otherwise it will be for the whole region.
# The TAZ file contains only one column named TAZ. The column name cannot be changed to other names.


# 2/6/2019
# New feature: allows to select trips starting from subarea_taz_file or ending at subarea_taz_file or both

tours_file = r'D:\2035BKRCastBaseline\2035BKRCastBaseline\outputs\_tour.tsv'

# enter a TAZ list if mode share for a specific subarea is desired. 
# if the list is empty (with the header 'TAZ only), the mode share for the whole region will be calculated.
subarea_taz_file = r'D:\2035BKRCastBaseline\2035BKRCastBaseline\BellevueDTTAZ.txt'
Output_file = r'D:\2035BKRCastBaseline\2035BKRCastBaseline\from_BelCBD_tour_mode_share_2035_PM.txt'

# Below is the start and end time you want to query from daysim output. It is number of minutes after 12am. 
# if you want 24hr data, set all to 0.
start_time = 1020  # minutes starting from 12am, 1530
end_time = 1080   # minutes starting from 12am, 1830

tours_from_only = True  # if true, trips only from the TAZ list
tours_end_only = False    # if true, trips only to the TAZ list

pd.options.display.float_format = '{:,.1%}'.format
total_tours_df = pd.DataFrame.from_csv(tours_file, sep = '\t')
if (start_time == 0 and end_time == 0):
    tours_df = total_tours_df
else:
    tours_df = total_tours_df.loc[((total_tours_df['tlvorig'] >= start_time) & (total_tours_df['tardest'] <= end_time)) | ((total_tours_df['tlvdest'] >= start_time) & (total_tours_df['tarorig'] <= end_time))]

subarea_taz_df = pd.DataFrame.from_csv(subarea_taz_file)
subarea_taz_df.reset_index(inplace = True)

if subarea_taz_df.empty == False:
    if tours_from_only == True:
        #tours_df.set_index('totaz', inplace = True)
        from_subarea_tours_df = tours_df.join(subarea_taz_df.set_index('TAZ'), on = 'totaz', how = 'right')
    if tours_end_only == True:
        #tours_df.set_index('tdtaz', inplace = True)
        to_subarea_tours_df = tours_df.join(subarea_taz_df.set_index('TAZ'), on = 'tdtaz', how = 'right')
    if ((tours_from_only == True) and (tours_end_only == True)):
        subarea_tours_df = pd.concat([from_subarea_tours_df, to_subarea_tours_df])
    elif tours_from_only == True:
        subarea_tours_df = pd.concat([from_subarea_tours_df])
    else:
        subarea_tours_df = pd.concat([to_subarea_tours_df])
else:
    print 'No subarea is defined. Use the whole trip table.'
    subarea_tours_df = tours_df

print 'Calculating mode share (all tour purpose)...'
model_df = subarea_tours_df[['tmodetp', 'toexpfac']].groupby('tmodetp').sum()
model_df['share'] = model_df['toexpfac'] / model_df['toexpfac'].sum()
model_df.reset_index(inplace = True)
mode_dict = {0:'Other',1:'Walk',2:'Bike',3:'SOV',4:'HOV2',5:'HOV3+',6:'Transit', 7: 'Park n Ride', 8:'School Bus'}
model_df.replace({'tmodetp': mode_dict}, inplace = True)
model_df.columns = ['mode', 'trips', 'share']
model_df.to_csv(Output_file, float_format = '%.3f')

print 'Calculating mode share (HBW only)...'
hbw_df = subarea_tours_df.loc[subarea_tours_df['pdpurp']==1][['tmodetp', 'toexpfac']].groupby('tmodetp').sum()
hbw_df['share'] = hbw_df['toexpfac'] / hbw_df['toexpfac'].sum()
hbw_df.reset_index(inplace = True)
hbw_df.replace({'tmodetp': mode_dict}, inplace = True)
hbw_df.columns = ['mode', 'trips', 'share']
hbw_df.to_csv(Output_file, float_format = '%.3f', mode = 'a')

print 'Tour mode share calculation is finished.'

