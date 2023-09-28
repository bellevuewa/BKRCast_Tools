import pandas as pd
import os

'''
This script is to calculate daily trips and share destined the study area, summarized by residence location. Study area is defined in target_area_file,
which is a csv file with only one colume named BKRCastTAZ. 
'''
working_folder = r"I:\Modeling and Analysis Group\02_Model Applications\Grants\SMART"
target_area_file = r"I:\Modeling and Analysis Group\02_Model Applications\Grants\SMART\destination_area.csv"
trip_file = r"D:\base_year_models\2019baseyear\BKR3-19-6_v2\outputs\daysim\_trip.tsv"
TAZ_Subarea_File_Name = r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\TAZ_subarea.csv"
hhs_file = r"D:\base_year_models\2019baseyear\BKR3-19-6_v2\outputs\daysim\_household.tsv"

target_df = pd.read_csv(target_area_file)
trips_df = pd.read_csv(trip_file, low_memory = True, sep = '\t')
subarea_df = pd.read_csv(TAZ_Subarea_File_Name)
hhs_df = pd.read_csv(hhs_file, sep = '\t')

hhs_df = hhs_df.merge(subarea_df[['BKRCastTAZ', 'Jurisdiction']], left_on = 'hhtaz', right_on = 'BKRCastTAZ', how = 'left').reset_index()

trips_df = trips_df.merge(hhs_df[['hhno', 'Jurisdiction', 'hhtaz']], on = 'hhno')

trips_to_target_df = trips_df.loc[trips_df['dtaz'].isin(target_df['BKRCastTAZ'])]

trips_to_target_by_juris_df = trips_to_target_df[['Jurisdiction', 'trexpfac']].groupby('Jurisdiction').sum()
trips_to_target_by_juris_df['%'] = trips_to_target_by_juris_df.apply(lambda x: x/x.sum())
trips_to_target_by_juris_df.to_csv(os.path.join(working_folder, 'person_trips_to_study_area_by_residence.csv'))

print('Done')


