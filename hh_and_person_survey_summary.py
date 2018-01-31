### purpose:
### to summarize households and persons of survey data by jurisdiction, mma and taz level.

import os
import pandas as pd
import h5py
import sys
import numpy as np

### inputs
hh_person_folder = r'D:\BKRCast_KirklandTest\BKRCast_Kirkland_Test3\daysim_summaries\bkrcast_all\data'
hh_person_file = "Household_bkr_new.dat"
TAZ_Subarea_File_Name = r'Z:\Modeling Group\BKRCast\Job Conversion Test\TAZ_subarea.csv'

print 'Loading hh and person file...'
hh_df = pd.DataFrame.from_csv(os.path.join(hh_person_folder, hh_person_file), sep = ' ')
taz_subarea = pd.DataFrame.from_csv(TAZ_Subarea_File_Name, sep = ",", index_col = "TAZNUM")

hh_taz = hh_df.join(taz_subarea, on = 'hhtaz')
hh_taz['total_persons'] = hh_taz['hhexpfac'] * hh_taz['hhsize']
hh_taz['total_hhs'] = hh_taz['hhexpfac']
summary_by_jurisdiction = hh_taz.groupby('Jurisdiction')['total_hhs', 'total_persons'].sum()   
summary_by_mma = hh_taz.groupby('Subarea')['total_hhs', 'total_persons'].sum()

taz_subarea.reset_index()
subarea_def = taz_subarea[['Subarea', 'SubareaName']]
subarea_def = subarea_def.drop_duplicates(keep = 'first')
subarea_def.set_index('Subarea', inplace = True)
summary_by_mma = summary_by_mma.join(subarea_def)
summary_by_taz = hh_taz.groupby('hhtaz')['total_hhs', 'total_persons'].sum()

print 'exporting summary by Jurisdiction ... '
summary_by_jurisdiction.to_csv(os.path.join(hh_person_folder, "survey_summary_by_jurisdiction.csv"), header = True)
print 'exporting summary by mma... '
summary_by_mma.to_csv(os.path.join(hh_person_folder, "survey_summary_by_mma.csv"), header = True)
print 'exporting summary by taz... '
summary_by_taz.to_csv(os.path.join(hh_person_folder, "survey_summary_by_taz.csv"), header = True)
print 'done.'

