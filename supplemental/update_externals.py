import pandas as pd
import math
from datetime import date

# configuration
external_file = r"D:\projects\2019baseyear\BKR0V2\inputs\2019\supplemental\generation\externals-2014.csv"
landuse_file = r"D:\projects\2019baseyear\BKR0V2\inputs\2019\supplemental\generation\landuse\tazdata_2014.csv"
updated_hhs = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2019\hh_summary_by_taz.csv"
new_jobs_file = r"Z:\Modeling Group\BKRCast\LandUse\2019baseyear\summary_by_TAZ.csv"

## output files
external_output_file = r"D:\projects\2019baseyear\BKR0V2\inputs\2019\supplemental\generation\externals.csv"
updated_landuse_file = r"D:\projects\2019baseyear\BKR0V2\inputs\2019\supplemental\generation\landuse\updated_tazdata.in"

annual_growth_rate = 0.0114
original_year = 2014
target_year = 2019

## end of configuration

# update external Prod and Attr
external_df = pd.read_csv(external_file, sep = ',', index_col = 'taz')
print 'total external prod and attr before update: ', external_df.sum().sum()
new_external_df = external_df * math.pow((1 + annual_growth_rate), target_year - original_year) 
new_external_df = new_external_df.round(0).astype(int)
print 'total external prod and attr after update: ', new_external_df.sum().sum()
new_external_df.to_csv(external_output_file, sep = ',')

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
            line = '{0} {1}: {2}\n'.format(row['TAZ'].astype(int), col, row[col])
            output.write(line)

        
    


print 'Done'