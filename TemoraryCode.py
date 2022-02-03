import os, sys
sys.path.append(os.getcwd())
import pandas as pd

columns = {'SFUnits':'SFUnits_44', 'MFUnits':'MFUnits_44'}
local_estimate_1 = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\test\2018TFPSensitivity\2018TFPSensitivity_COB_hhs_estimate.csv'
local_estimate_2 = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2044\2044Test_COB_hhs_estimate.csv'

local1_df = pd.read_csv(local_estimate_1)
local2_df = pd.read_csv(local_estimate_2)
local2_df.rename(columns = columns, inplace = True)

local_df = pd.merge(local1_df, local2_df, on = 'PSRC_ID', how = 'outer')

print 'Done'



