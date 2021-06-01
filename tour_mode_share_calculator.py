import pandas as pd
import os
import datetime

# To calculate mode share (from tours) from daysim output _tour.tsv. User is allowed to define a subarea in the format of a list of TAZ. 
# If so, the mode share will be calculated for that subarea. Otherwise it will be for the whole region.
# The TAZ file contains only one column named TAZ. The column name cannot be changed to other names.


# 2/6/2019
# New feature: allows to select trips starting from subarea_taz_file or ending at subarea_taz_file or both

# 8/29/2019
# fixed a bug in trip filtering.

# 5/25/2021
# New feature: allow to calculate mode share by residence, workplace and subtours at workplace.


tours_file = r'D:\projects\2018baseyear\outputs\_tour.tsv'
hhs_file = r'D:\projects\2018baseyear\outputs\_household.tsv'

# enter a TAZ list if mode share for a specific subarea is desired. 
# if the list is empty (with the header 'TAZ only), the mode share for the whole region will be calculated.
subarea_taz_file = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\Bellevue_TAZ.txt'
Output_file = r'D:\projects\2018baseyear\outputs\2018_to_COB_from_all_tour_daily_mode_share.txt'

# Below is the start and end time you want to query from daysim output. It is number of minutes after 12am. 
# if you want 24hr data, set all to 0.
start_time = 0  # minutes starting from 12am, 1530
end_time = 0   # minutes starting from 12am, 1830

# if both of them are true, both ends are inside the defined subarea!!!
tours_from_only = False  # if true, trips from the TAZ list
tours_end_only = True    # if true, trips to the TAZ list

tour_purpose = {0: 'all',
                1: 'work',
                2: 'school',
                3: 'escort',
                4: 'personal business',
                5: 'shopping',
                6: 'meal',
                7: 'social'}
mode_dict = {0:'Other',1:'Walk',2:'Bike',3:'SOV',4:'HOV2',5:'HOV3+',6:'Transit', 7: 'Park_n_Ride', 8:'School_Bus'}

#purpose: 0: all purpose, 1: work, 2:school,3:escort, 4: personal buz, 5: shopping, 6: meal, 7: social/recreational, 8: not defined, 9: not defined
def CalModeSharebyPurpose(purpose, tour_df, Output_file, overwritten=False, comments=''):
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
    purpose_df['trips'] = purpose_df['trips'].astype(int)
    purpose_df['share'] = purpose_df['share'].map('{:.1%}'.format)

    if overwritten:
        filemode = 'w'
    else:
        filemode = 'a'

    with open(Output_file, filemode) as output:
        if comments != '':
            output.write(comments + '\n')
        if purpose == 0:
            output.write('All purposes\n')
        else: 
            output.write(tour_purpose[purpose] + '\n')
        output.write('%s' % purpose_df)
        output.write('\n\n')

def write_file_header(output_file, overwritten = False):
    if overwritten:
        file_mode = 'w'
    else:
        file_mode = 'a'
    
    with open(output_file, file_mode) as output:
        output.write(str(datetime.datetime.now()) + '\n')
        output.write(tours_file + '\n')
        output.write(hhs_file + '\n')
        output.write(subarea_taz_file + '\n')
        output.write('Start time: ' + str(start_time) + '\n')
        output.write('End time: ' + str(end_time) + '\n')
        output.write('From the subarea: ' + str(tours_from_only) + '\n')
        output.write('To the subarea: ' + str(tours_end_only) + '\n')
        output.write('\n')

total_tours_df = pd.read_csv(tours_file, sep = '\t')
if (start_time == 0 and end_time == 0):
    tours_df = total_tours_df
else:
    tours_df = total_tours_df.loc[((total_tours_df['tlvorig'] >= start_time) & (total_tours_df['tardest'] <= end_time)) | ((total_tours_df['tlvdest'] >= start_time) & (total_tours_df['tarorig'] <= end_time))]

subarea_taz_df = pd.read_csv(subarea_taz_file)
subarea_taz_df.reset_index(inplace = True)

if subarea_taz_df.empty == False:
    if tours_from_only == True:
        from_subarea_tours_df = tours_df.join(subarea_taz_df.set_index('TAZ'), on = 'totaz', how = 'right')
    if tours_end_only == True:
        to_subarea_tours_df = tours_df.join(subarea_taz_df.set_index('TAZ'), on = 'tdtaz', how = 'right')
    if ((tours_from_only == True) and (tours_end_only == True)):
        subarea_tours_df = from_subarea_tours_df.merge(subarea_taz_df, left_on = 'tdtaz', right_on = 'TAZ')
    elif tours_from_only == True:
        subarea_tours_df = pd.concat([from_subarea_tours_df])
    else:
        subarea_tours_df = pd.concat([to_subarea_tours_df])
else:
    print 'No subarea is defined. Use the whole trip table.'
    subarea_tours_df = tours_df

write_file_header(Output_file, True)
CalModeSharebyPurpose(0, subarea_tours_df, Output_file)        
for purpose in [1,2,3,4,5,6,7]:
    CalModeSharebyPurpose(purpose, subarea_tours_df, Output_file)        

print 'Tour mode share calculation is finished.'
hhs_df = pd.read_csv(hhs_file, sep = '\t' )
hhs_df = hhs_df[['hhno','hhparcel', 'hhtaz']]
tours_by_residence_df = tours_df.loc[(tours_df['parent'] == 0)].merge(hhs_df, left_on = 'hhno', right_on = 'hhno', how = 'left')
tours_by_residence_df = tours_by_residence_df.merge(subarea_taz_df, left_on = 'hhtaz', right_on = 'TAZ', how = 'inner')

CalModeSharebyPurpose(0, tours_by_residence_df, Output_file, comments='By Residence Only')        
for purpose in [1,2,3,4,5,6,7]:
    CalModeSharebyPurpose(purpose, tours_by_residence_df, Output_file, comments='By Residence Only')        

print 'Tour mode share by residence is finished.'

# tours by workplace = tours from home for work purpose + all subtours from workplace.
tours_work_purpose_df = tours_df.loc[(tours_df['parent'] == 0) & (tours_df['pdpurp'] == 1)]
tours_work_purpose_df = tours_work_purpose_df.merge(subarea_taz_df, how = 'inner', left_on = 'tdtaz', right_on= 'TAZ')
work_subtours_df = tours_df.loc[tours_df['parent'] > 0]
work_subtours_df = work_subtours_df.merge(subarea_taz_df, how = 'inner', left_on = 'totaz', right_on = 'TAZ')

tours_by_workplace_df = pd.concat([tours_work_purpose_df, work_subtours_df])
CalModeSharebyPurpose(0, tours_by_workplace_df, Output_file, comments='By Workplace Only (with subtours)')        
print 'Tour mode share by workplace (with subtours) is finished.'

CalModeSharebyPurpose(0, tours_work_purpose_df, Output_file, comments='By Workplace Only (without subtours)')        
print 'Tour mode share by workplace (without subtours) is finished.'

CalModeSharebyPurpose(0, work_subtours_df, Output_file, comments='Subtours at Workplace Only')        
for purpose in [1,2,3,4,5,6,7]:
    CalModeSharebyPurpose(purpose, work_subtours_df, Output_file, comments='Subtours at Workplace Only')

print 'Tour mode share by subtours at workplace is finished.'        
print 'Done.'
