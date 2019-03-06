import pandas as pd
import os

# To calculate mode share from daysim output _trips.tsv. User is allowed to define a subarea in the format of a list of TAZ. 
# If so, the mode share will be calculated for that subarea. Otherwise it will be for the whole region.
# The TAZ file contains only one column named TAZ. The column name cannot be changed to other names.

trips_file = r'D:\2ndStAnalysis\BRK0V1-2nSt\outputs\_trip.tsv'

# enter a TAZ list if mode share for a specific subarea is desired. 
# if the list is empty (with the header 'TAZ only), the mode share for the whole region will be calculated.
subarea_taz_file = r'D:\2ndStAnalysis\BRK0V1-2nSt\Bellevue_TAZ.txt'
Output_file = r'D:\2ndStAnalysis\BRK0V1-2nSt\Bellevue_mode_share_2014.txt'

pd.options.display.float_format = '{:,.1%}'.format
trips_df = pd.DataFrame.from_csv(trips_file, sep = '\t')
subarea_taz_df = pd.DataFrame.from_csv(subarea_taz_file)
subarea_taz_df.reset_index(inplace = True)
if subarea_taz_df.empty == False:
    trips_df.set_index('otaz', inplace = True)
    from_subarea_trips_df = trips_df.join(subarea_taz_df.set_index('TAZ'), how = 'right')
    trips_df.set_index('dtaz', inplace = True)
    to_subarea_trips_df = trips_df.join(subarea_taz_df.set_index('TAZ'), how = 'right')
    subarea_trips_df = pd.concat([from_subarea_trips_df, to_subarea_trips_df])
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

