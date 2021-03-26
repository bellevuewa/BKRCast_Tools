import pandas as pd
import math
from datetime import date


###
###This program is not working. A lot of things need to be redone.  Do not run this program.
## Need to manually process truck externals and special generators.
## The easiest way to update truck external is using Excel spreadsheet.
##
## 

truck_model_directory = r'D:\projects\2019baseyear\BKR0V2\inputs\2019\trucks'
truck_external_files = {'heavy_trucks_ee_2014.in':'heavy_trucks_ee.in', 'heavy_trucks_ei_2014.in':'heavy_trucks_ei.in', 'heavy_trucks_ie_2014.in':'heavy_trucks_ie.in',
    'medium_trucks_ee_2014.in':'medium_trucks_ee.in', 'medium_trucks_ei_2014.in':'medium_trucks_ei.in', 'medium_trucks_ie_2014.in':'medium_trucks_ie.in'}
special_generator_files = {'special_gen_heavy_trucks_2014.in':'special_gen_heavy_trucks.in', 
    'special_gen_medium_trucks_2014.in':'special_gen_medium_trucks.in',
    'special_gen_light_trucks_2014.in':'special_gen_light_trucks.in'} 

base_year = 2014
target_year = 2019
annaul_growth_rate = 0.0137

for key, val in trucktruck_external_files.iteritems():
    external_df = pd.read_csv(key, skiprows=5)
    

def export_to_file(updated_external)
    with open(updated_external, mode = 'w') as output:
        output.write('c updated tazdata.in {0}\n'.format(date.today()))
        output.write('c  hhs  by  income  (4  ranges),  employment,  group  quarter  populations,  college  (fte)  enrollment\n')
        output.write('t  matrices\n')
        output.write('m  matrix="hhemp"\n')

        for index, row in lu_df.iterrows():
            for col in xrange(101, 125):
                line = '{0} {1}: {2:.2f}\n'.format(row['TAZ'].astype(int), col, row[col])
                output.write(line)
