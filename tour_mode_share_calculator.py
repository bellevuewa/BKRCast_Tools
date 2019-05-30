import pandas as pd
import os

# To calculate mode share (from tours) from daysim output _tour.tsv. User is allowed to define a subarea in the format of a list of TAZ. 
# If so, the mode share will be calculated for that subarea. Otherwise it will be for the whole region.
# The TAZ file contains only one column named TAZ. The column name cannot be changed to other names.


# 2/6/2019
# New feature: allows to select trips starting from subarea_taz_file or ending at subarea_taz_file or both

tours_file = r'D:\BRK0V1\outputs\_tour.tsv'

# enter a TAZ list if mode share for a specific subarea is desired. 
# if the list is empty (with the header 'TAZ only), the mode share for the whole region will be calculated.
subarea_taz_file = r'D:\BRK0V1\Kirkland_TAZ.txt'
Output_file = r'D:\BRK0V1\2014_Kirkland_tour_PMPK_mode_share.txt'

# Below is the start and end time you want to query from daysim output. It is number of minutes after 12am. 
# if you want 24hr data, set all to 0.
start_time = 1020  # minutes starting from 12am, 1530
end_time = 1080   # minutes starting from 12am, 1830

# if both of them are true, it will pull trips from the lists and trips to the lists. But they are not internal trips!!!
tours_from_only = True  # if true, trips from the TAZ list
tours_end_only = True    # if true, trips to the TAZ list

tour_purpose = {0: 'all',
                1: 'work',
                2: 'school',
                3: 'escort',
                4: 'personal business',
                5: 'shopping',
                6: 'meal',
                7: 'social'}
mode_dict = {0:'Other',1:'Walk',2:'Bike',3:'SOV',4:'HOV2',5:'HOV3+',6:'Transit', 7: 'Park n Ride', 8:'School Bus'}

#purpose: 0: all purpose, 1: work, 2:school,3:escort, 4: personal buz, 5: shopping, 6: meal, 7: social/recreational, 8: not defined, 9: not defined
def CalModeSharebyPurpose(purpose, tour_df, Output_file, overwritten=False):
    purpose_df = None
    if (purpose > 0 and purpose <= 7): 
        print 'Calculating mode share for purpose ', purpose, ':', tour_purpose[purpose];
        purpose_df = tour_df.loc[tour_df['pdpurp']==purpose][['tmodetp', 'toexpfac']].groupby('tmodetp').sum()
    elif purpose == 0:
        print 'Calculating mode share for all purpose...'
        purpose_df = tour_df[['tmodetp', 'toexpfac']].groupby('tmodetp').sum()
    else:
        print 'invalid purpose ', purpose
        return

    purpose_df['share'] = purpose_df['toexpfac'] / purpose_df['toexpfac'].sum()
    purpose_df.reset_index(inplace = True)
    purpose_df.replace({'tmodetp': mode_dict}, inplace = True)
    purpose_df.columns = ['mode', 'trips', 'share']
    if overwritten:
        with open(Output_file, 'w') as output:
            if purpose == 0:
                output.write('All purposes\n')
            else: 
                output.write(tour_purpose[purpose] + '\n')
    else:
        with open(Output_file, 'a') as output:
            if purpose == 0:
                output.write('All purpose\n')
            else:
                output.write(tour_purpose[purpose] + '\n')
    purpose_df.to_csv(Output_file, float_format = '%.3f', mode = 'a')



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


CalModeSharebyPurpose(0, subarea_tours_df, Output_file, True)        
for purpose in [1,2,3,4,5,6,7]:
    CalModeSharebyPurpose(purpose, subarea_tours_df, Output_file)        

print 'Tour mode share calculation is finished.'

