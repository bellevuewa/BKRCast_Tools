import pandas as pd
import os

# To calculate mode share from daysim output _trips.tsv. User is allowed to define a subarea in the format of a list of TAZ. 
# If so, the mode share will be calculated for that subarea. Otherwise it will be for the whole region.
# The TAZ file contains only one column named TAZ. The column name cannot be changed to other names.

# 1/29/2019
# New feature: allows to look into mode share by time period

# 2/6/2019
# New feature: allows to select trips starting from subarea_taz_file or ending at subarea_taz_file or both

trips_file = r'D:\BKRCast_Calibration\BKRCast_v1-2_Calibration\BKRCast_Run21-S42-neg_coef6-nopopsampler\outputs\_trip.tsv'

# enter a TAZ list if mode share for a specific subarea is desired. 
# if the list is empty (with the header 'TAZ only), the mode share for the whole region will be calculated.
subarea_taz_file = r'D:\BRK0V1\BellevueDTTAZ.txt'
Output_file = r'D:\BKRCast_Calibration\BKRCast_v1-2_Calibration\BKRCast_Run21-S42-neg_coef6-nopopsampler\from_BelCBD_mode_share_2014.txt'

# Below is the start and end time you want to query from daysim output. It is number of minutes after 12am. 
# if you want 24hr data, set all to 0.
start_time = 0  # minutes starting from 12am, 1530
end_time = 0   # minutes starting from 12am, 1830

trips_from_only = True  # if true, trips only from the TAZ list
trips_end_only = False    # if true, trips only to the TAZ list

pd.options.display.float_format = '{:,.1%}'.format
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
        subarea_trips_df = pd.concat([from_subarea_trips_df, to_subarea_trips_df])
    elif trips_from_only == True:
        subarea_trips_df = pd.concat([from_subarea_trips_df])
    else:
        subarea_trips_df = pd.concat([to_subarea_trips_df])
else:
    print 'No subarea is defined. Use the whole trip table.'
    subarea_trips_df = trips_df

print 'Calculating mode share (all trip purpose)...'
model_df = subarea_trips_df[['mode', 'trexpfac']].groupby('mode').sum()
model_df['share'] = model_df['trexpfac'] / model_df['trexpfac'].sum()
model_df.reset_index(inplace = True)
mode_dict = {0:'Other',1:'Walk',2:'Bike',3:'SOV',4:'HOV2',5:'HOV3+',6:'Transit',8:'School Bus'}
model_df.replace({'mode': mode_dict}, inplace = True)
model_df.columns = ['mode', 'trips', 'share']
model_df.to_csv(Output_file, float_format = '%.3f')

print 'Calculating mode share (HBW only)...'
hbw_df = subarea_trips_df.loc[(subarea_trips_df['dpurp']==1) | (subarea_trips_df['dpurp']==0)][['mode', 'trexpfac']].groupby('mode').sum()
hbw_df['share'] = hbw_df['trexpfac'] / hbw_df['trexpfac'].sum()
hbw_df.reset_index(inplace = True)
hbw_df.replace({'mode': mode_dict}, inplace = True)
hbw_df.columns = ['mode', 'trips', 'share']
hbw_df.to_csv(Output_file, float_format = '%.3f', mode = 'a')

print 'Mode share calculation is finished.'

