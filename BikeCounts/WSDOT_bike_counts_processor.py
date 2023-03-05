import os
import pandas as pd
import datetime

## This program is used to calculate average bike counts from WSDOT permenant bike counting file.
## User can specify desired count locations, weekday or weekend or both, all year around or only summer (may to october)
## 
## input 
working_folder = r'I:\Modeling and Analysis Group\03_Data\bikecounts'
wsdot_count_file_name = r'PTRBikePedSummary2020.csv'

selectedLocations = [100031002, 100021744, 100035378, 100035377, 100019136, 100019135]


is_weekday_included = True
is_weekend_included = False
is_may_to_oct = True
export_filename = '2020_WSDOT_Bikecounts_weekday_summer.xlsx'
################################


if is_weekday_included == True and is_weekend_included == False:
    days = [1, 2, 3, 4, 5]
elif is_weekday_included == True and is_weekend_included == True:
    days = [1, 2, 3, 4, 5, 6, 7]
elif is_weekday_included == False and is_weekend_included == True:
    days = [6, 7]
else: 
    print('no days of week is selected.')
    exit(-1)

counts_df = pd.read_csv(os.path.join(working_folder, wsdot_count_file_name), low_memory = False)
counts_df.columns=['LocationName', 'LocationDesc', 'Longitude', 'Latitude', 'Notes', 'StartYear', 'RecordBrand', 'RecorderModel', 'RecorderType', 'StartTime', 'EndTime', 'IntervalMinutes', 'Direction', 'PedCount', 'BikeCount', 'OtherCount']
counts_df['StartTime'] = pd.to_datetime(counts_df['StartTime'])
counts_df['EndTime'] = pd.to_datetime(counts_df['EndTime'])
counts_df['StartHr'] = counts_df['StartTime'].dt.hour
counts_df['EndHr'] = counts_df['EndTime'].dt.hour
 
selected_df = counts_df.loc[counts_df['LocationName'].isin(selectedLocations)]

if is_may_to_oct == True:
    selected_df = selected_df.loc[(selected_df['StartTime'].dt.weekday.isin(days)) & ((selected_df['StartTime'].dt.month >= 5) & (selected_df['StartTime'].dt.month <= 10))]
else:
    selected_df = selected_df.loc[(selected_df['StartTime'].dt.weekday.isin(days))]

selected_by_hr_df = selected_df[['LocationName', 'StartHr', 'Direction', 'PedCount', 'BikeCount']].groupby(['LocationName', 'Direction', 'StartHr']).mean().reset_index()

writer = pd.ExcelWriter(os.path.join(working_folder, export_filename), engine = 'xlsxwriter')
wksheet = writer.book.add_worksheet('readme')
wksheet.write(0,0,str(datetime.datetime.now()))
wksheet.write(1, 0, 'data file')
wksheet.write(1, 1, os.path.join(working_folder, wsdot_count_file_name))
wksheet.write(3, 0, 'data selection filter')
wksheet.write(4, 1, 'days: ')
wksheet.write(4, 2, str(days))
wksheet.write(5, 1, 'may_to_oct only?')
wksheet.write(5, 2, is_may_to_oct)

locations = selected_df['LocationName'].unique()
for loc in locations:
    sel_directional = selected_by_hr_df.loc[selected_by_hr_df['LocationName'] == loc]
    sel_directional.to_excel(writer, sheet_name = str(loc), index = False)    
    sel_all = sel_directional[['StartHr', 'PedCount', 'BikeCount']].groupby('StartHr').sum().reset_index()
    sel_all.to_excel(writer, sheet_name = str(loc)+'_all', index = False)

writer.save()
print('Done')