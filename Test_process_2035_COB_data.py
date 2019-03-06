import os
import pandas as pd

cob2035_filename = r'Z:\Modeling Group\BKRCast\2035Parcel_Sqft_based\COB estimate\2035_COB_Jobs_Estimate.csv'
TAZ_Subarea_File_Name = r"Z:\Modeling Group\BKRCast\Job Conversion Test\TAZ_subarea.csv"
parcels = pd.DataFrame.from_csv(cob2035_filename, sep = ",")
taz_subarea = pd.DataFrame.from_csv(TAZ_Subarea_File_Name, sep = ",")

Output_Field_35 = ['EMPEDU_P_35', 'EMPFOO_P_35', 'EMPGOV_P_35', 'EMPIND_P_35', 'EMPMED_P_35', 'EMPOFC_P_35', 'EMPOTH_P_35', 'EMPRET_P_35', 'EMPRSC_P_35', 'EMPSVC_P_35', 'EMPTOT_P_35', 'HH_P_35']
Output_Field_14 = ['EMPEDU_P_14', 'EMPFOO_P_14', 'EMPGOV_P_14', 'EMPIND_P_14', 'EMPMED_P_14', 'EMPOFC_P_14', 'EMPOTH_P_14', 'EMPRET_P_14', 'EMPRSC_P_14', 'EMPSVC_P_14', 'EMPTOT_P_14', 'HH_P_14']

Output_Field = Output_Field_35 + Output_Field_14

parcels = parcels.merge(taz_subarea, how = 'left', left_on = 'BKRCastTAZ', right_on = 'TAZNUM')

summary_by_jurisdiction = parcels.groupby('Jurisdiction')[Output_Field].sum()
print "Exporting \"summary_by_jurisdiction.csv\""
summary_by_jurisdiction.to_csv(r'Z:\Modeling Group\BKRCast\2035Parcel_Sqft_based\COB estimate\summary_by_jurisdiction.csv')
summary_by_taz = parcels.groupby('BKRCastTAZ')[Output_Field].sum()
print "Exporting \"summary_by_TAZ.csv\""
summary_by_taz.to_csv(r'Z:\Modeling Group\BKRCast\2035Parcel_Sqft_based\COB estimate\summary_by_TAZ.csv') 
print "Exporting \"summary_by_subarea.csv\""
summary_by_subarea = parcels[parcels['Subarea'] > 0].groupby('Subarea')[Output_Field].sum()
taz_subarea = parcels[['Subarea', 'SubareaName']].drop_duplicates()
taz_subarea.set_index('Subarea', inplace = True)
summary_by_subarea = summary_by_subarea.join(taz_subarea['SubareaName'])
summary_by_subarea.to_csv(r'Z:\Modeling Group\BKRCast\2035Parcel_Sqft_based\COB estimate\summary_by_subarea.csv')

print 'Done.'