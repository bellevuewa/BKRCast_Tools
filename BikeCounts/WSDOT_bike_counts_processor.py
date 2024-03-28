from calendar import weekday
import os
import pandas as pd
import datetime

## This program is used to calculate average bike counts from WSDOT permenant bike counting file.
## User can specify desired count locations, weekday or weekend or both, all year around or only summer (may to october)
## 
## input 
working_folder = r"I:\Modeling and Analysis Group\03_Data\bikecounts\WSDOT_counts"
wsdot_count_file_name = r'PTRBikePedSummary2019.csv'
wsdot_location_file = r'PTRBikePedCount.PTRBikePedLocation-2024.csv'

selectedLocations = [100030163, 100038318, 100030164, 100038317, 100021744, 100031002, 100035378, 100035377, 100031001, 100022295, 100022296, 100023865, 100038319, 100019136, 100019135, 100033843]


is_weekday_included = True
is_weekend_included = False
is_may_to_oct = True
export_filename = '2019_WSDOT_Bikecounts_summer.xlsx'
################################

weekdays = [1, 2, 3, 4, 5]
weekends = [6, 7]


def travel_pattern(sum_all_df, loc):
    sum_all_df.reset_index(inplace = True)
    peak_weekend = sum_all_df['Bike_weekend'].max()
    peak_weekday = sum_all_df['Bike_weekday'].max()
    if peak_weekday == 0:
        peak_weekday = 0.01            
    weekend_ratio = peak_weekend / peak_weekday
    am_peak_weekday = sum_all_df.loc[sum_all_df['StartHr'].isin([7,8]), 'Bike_weekday'].mean()
    md_peak_weekday = sum_all_df.loc[sum_all_df['StartHr'].isin([11, 12]), 'Bike_weekday'].mean() 
    if md_peak_weekday == 0:
        md_peak_weekday = 0.01            
    am_ratio = am_peak_weekday / md_peak_weekday

    travel_pattern = ''
    if (weekend_ratio < 1.0) and (am_ratio > 1.5):            
        travel_pattern = 'Commute'            
    elif (weekend_ratio < 1.0) and (am_ratio < 1.5):
        travel_pattern = 'Mixed'
    elif (weekend_ratio > 1.0) and (weekend_ratio < 1.8) and (am_ratio > 1.5):
        travel_pattern = 'Mixed'
    elif (weekend_ratio > 1.0) and (weekend_ratio < 1.8) and (am_ratio < 1.5):
        travel_pattern = 'Non-commute or Noon activity'
    elif weekend_ratio > 1.8:            
        travel_pattern = 'Non-commute or Noon activity'
            
    data = [{'peak_weekend': peak_weekend, 'peak_weekday':peak_weekday, 'weekend_ratio':weekend_ratio, 'avg_am (7 - 9am)': am_peak_weekday, 'avg_md (11am - 1pm)': md_peak_weekday, 'morning ratio': am_ratio, 'travel_pattern':travel_pattern}]
    pattern_df = pd.DataFrame(data).T
    pattern_df.columns = [str(loc)]    
    return pattern_df      


counts_df = pd.read_csv(os.path.join(working_folder, wsdot_count_file_name), low_memory = False)
counts_df['StartTime'] = pd.to_datetime(counts_df['StartIntervalDateTime'])
counts_df['EndTime'] = pd.to_datetime(counts_df['EndIntervalDateTime'])
counts_df['StartHr'] = counts_df['StartTime'].dt.hour
counts_df['EndHr'] = counts_df['EndTime'].dt.hour
counts_df['Year'] = counts_df['StartTime'].dt.year
counts_df['Day'] = counts_df['StartTime'].dt.day

location_df = pd.read_csv(os.path.join(working_folder, wsdot_location_file))
 
selected_df = counts_df.loc[counts_df['LocationName'].isin(selectedLocations)].copy()

if is_may_to_oct == True:
    selected_df = selected_df.loc[((selected_df['StartTime'].dt.month >= 5) & (selected_df['StartTime'].dt.month <= 10))]

selected_by_hr_df = selected_df[['LocationName', 'Year', 'Day', 'StartHr', 'PedestrianCount', 'BicyclistCount']].groupby(['LocationName', 'Year', 'Day', 'StartHr']).mean().reset_index()


with pd.ExcelWriter(os.path.join(working_folder, export_filename), engine = 'xlsxwriter') as writer:
    wksheet = writer.book.add_worksheet('readme')
    wksheet.write(0,0,str(datetime.datetime.now()))
    wksheet.write(1, 0, 'data file')
    wksheet.write(1, 1, os.path.join(working_folder, wsdot_count_file_name))
    wksheet.write(3, 0, 'data selection filter')
    wksheet.write(4, 1, f'week days: {weekdays}')
    wksheet.write(5, 1, f'weekend: {weekends}')
    wksheet.write(6, 1, 'may_to_oct only?')
    wksheet.write(6, 2, is_may_to_oct)
    wksheet.write(10, 0, 'travel pattern analysis is based on https://wsdot.wa.gov/sites/default/files/2021-10/Bike-Ped-Count-Guidebook.pdf')
    
    locations = selected_df['LocationName'].unique()
    
    for loc in locations:
        start = 1        
        loc_desc = location_df.loc[location_df['LocationName'] == str(loc), 'LocationDescription'].values[0]        
        sel_all = selected_by_hr_df.loc[selected_by_hr_df['LocationName'] == loc]
        sel_weekdays = sel_all.loc[sel_all['Day'].isin(weekdays)]    
        sel_weekends = sel_all.loc[sel_all['Day'].isin(weekends)]            

        weekday_sum_df = sel_weekdays[['Year', 'StartHr', 'PedestrianCount', 'BicyclistCount']].groupby(['Year', 'StartHr']).sum()
        weekend_sum_df = sel_weekends[['Year', 'StartHr', 'PedestrianCount', 'BicyclistCount']].groupby(['Year', 'StartHr']).sum()
        weekday_sum_df.rename(columns = {'PedestrianCount':'Ped_weekday', 'BicyclistCount':'Bike_weekday' }, inplace = True)
        weekend_sum_df.rename(columns = {'PedestrianCount':'Ped_weekend', 'BicyclistCount':'Bike_weekend' }, inplace = True)
        sum_all_df = weekday_sum_df.merge(weekend_sum_df, left_index = True, right_index = True, how = 'outer')
        
        # sum all columns by 'Year' multiIndex now becomes single index 'Year'        
        sum_row = sum_all_df.groupby(level=0).sum()
        sum_row.index = pd.MultiIndex.from_tuples([(level, 'Total') for level in sum_row.index])
        sum_all_df = sum_all_df.append(sum_row)        
        sum_all_df.to_excel(writer, sheet_name = str(loc), startrow = start, index = True)
        
        # write table title                  
        wksheet = writer.sheets[str(loc)]
        wksheet.write(0, 0, f'{loc}: {loc_desc}')  
        
        pattern_df = travel_pattern(sum_all_df, loc)

        start = start + sum_all_df.shape[0] + 5
        pattern_df.to_excel(writer, sheet_name = str(loc), startrow = start, index = True) 
        wksheet.write(start - 1, 0, 'Travel Pattern Analysis')                     

print('Done')

