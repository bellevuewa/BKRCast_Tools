import pandas as pd
import os
import utility

working_folder = r'Z:\Modeling Group\BKRCast\LandUse\Complan\Complan2044\2044LU'
file1 = 'original_2044 Housing Estimates.csv'
file2 = 'original_2044 Jobs Estimates.csv'

file1_df = pd.read_csv(os.path.join(working_folder, file1))
file2_df = pd.read_csv(os.path.join(working_folder, file2))

combined_df = pd.merge(file2_df[['PSRC_ID', 'jobs44_edu', 'jobs44_food', 'jobs44_gov', 'jobs44_indus', 'jobs44_med', 'jobs44_off', 'jobs44_oth', 'jobs44_ret', 'jobs44_serv' ,'jobs44_total']], file1_df[['PSRC_ID', 'sf44' ,'mf44']], on = 'PSRC_ID', how = 'outer')
combined_df = combined_df.fillna(0)
rename_dict = {'jobs44_edu':'JOBS_EDU', 'jobs44_food':'JOBS_FOOD', 'jobs44_gov':'JOBS_GOV', 'jobs44_indus':'JOBS_IND', 'jobs44_med':'JOBS_MED', 'jobs44_off':'JOBS_OFF', 'jobs44_oth':'JOBS_OTH', 'jobs44_ret':'JOBS_RET', 'jobs44_serv':'JOBS_SERV', 'jobs44_total':'JOBS_TOTAL', 'sf44':'UNITS_SF', 'mf44':'UNITS_MF'}
combined_df.rename(columns = rename_dict, inplace = True)
combined_df['JOBS_RSV'] = 0
combined_df.to_csv(os.path.join(working_folder, 'combined_LU_2044_complan.csv'))

utility.backupScripts(__file__, os.path.join(working_folder, os.path.basename(__file__)))

print('Done')
