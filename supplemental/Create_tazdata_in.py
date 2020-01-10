import pandas as pd
import math
from datetime import date

# configuration
landuse_file = r"D:\projects\2018baseyear\BKR0V1-02\inputs\supplemental\generation\landuse\tazdata_2014.csv"
updated_hhs = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\2035SyntheticPopulation_S_DT_Access_Study\hh_summary_by_taz.csv"
new_jobs_file = r"Z:\Modeling Group\BKRCast\2035SDTAccess\outputs\summary_by_TAZ.csv"

## output files
updated_landuse_file = r"D:\projects\2ndStAnalysis\2035\2035BKRCastBaseline\inputs\2035\supplemental\generation\landuse\updated_tazdata.in"

## end of configuration

lu_df = pd.read_csv(landuse_file, sep = ',')
new_hhs_df = pd.read_csv(updated_hhs, sep = ',')

lu_df['hhs'] = lu_df['102'] + lu_df['103'] + lu_df['104'] + lu_df['105']
lu_df = lu_df.merge(new_hhs_df, how = 'left', left_on = 'TAZ', right_on = 'hhtaz')
lu_df['ratio'] = lu_df.loc[lu_df['hhs'] > 0, 'total_hhs'] * 1.0 / lu_df['hhs']
lu_df['ratio'].fillna(0, inplace = True)
lu_df['total_hhs'].fillna(0, inplace = True)

# update households by proportionally increase col 102 ~ 105
print 'total hhs before update: ', lu_df[['102','103','104','105']].sum().sum()
lu_df['102'] = lu_df['102'] * lu_df['ratio']
lu_df['103'] = lu_df['103'] * lu_df['ratio']
lu_df['104'] = lu_df['104'] * lu_df['ratio']
lu_df['105'] = lu_df['105'] * lu_df['ratio']
print 'total hhs after update: ', lu_df[['102','103','104','105']].sum().sum()

# update jobs
print 'total jobs before update: ', lu_df[['109', '112', '115', '118', '120']].sum().sum()
new_jobs_df = pd.read_csv(new_jobs_file, sep = ',')
new_jobs_df['Retail'] = new_jobs_df['EMPFOO_P'] + new_jobs_df['EMPRET_P']
new_jobs_df['FIRES'] = new_jobs_df['EMPMED_P'] + new_jobs_df['EMPOFC_P'] + new_jobs_df['EMPSVC_P']
new_jobs_df['GOV'] = new_jobs_df['EMPGOV_P']
new_jobs_df['EDU'] = new_jobs_df['EMPEDU_P']
new_jobs_df['IND'] = new_jobs_df['EMPIND_P'] + new_jobs_df['EMPOTH_P']

lu_df = lu_df.merge(new_jobs_df, how = 'left', left_on = 'TAZ', right_on = 'TAZ_P')
lu_df['109'] = lu_df['Retail']
lu_df['112'] = lu_df['FIRES']
lu_df['115'] = lu_df['GOV']
lu_df['118'] = lu_df['EDU']
lu_df['120'] = lu_df['IND']
print 'total jobs after update: ', lu_df[['109', '112', '115', '118', '120']].sum().sum()

lu_df = lu_df.drop(new_jobs_df.columns, axis = 1)
lu_df = lu_df.drop(new_hhs_df.columns, axis = 1)
lu_df = lu_df.drop(['hhs', 'ratio'], axis = 1)
lu_df = lu_df.fillna(0)

lu_df = lu_df.loc[lu_df['TAZ'] <= 1359]
# export the updated file
#lu_df.to_csv(updated_landuse_file, sep = ',')
with open(updated_landuse_file, mode = 'w') as output:
    output.write('c updated tazdata.in {0}\n'.format(date.today()))
    output.write('c  hhs  by  income  (4  ranges),  employment,  group  quarter  populations,  college  (fte)  enrollment\n')
    output.write('t  matrices\n')
    output.write('m  matrix="hhemp"\n')

    for index, row in lu_df.iterrows():
        for col in xrange(101, 125):
            line = '{0} {1}: {2:.2f}\n'.format(row['TAZ'].astype(int), col, row[col])
            output.write(line)

        
    


print 'Done'