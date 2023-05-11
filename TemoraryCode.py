import os, sys
sys.path.append(os.getcwd())
import pandas as pd

infile = r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv"

df = pd.read_csv(infile, low_memory = False)
new_df = df.drop(['OID_', 'PIN', 'COUNTY', 'City', 'HousingUnits2014', 'COUNTYFP10', 'TRACTCE10'], axis = 1)
new_df.to_csv(r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\new_parcel_TAZ_2014_lookup.csv', index = False)
print('Done')


