import pandas as pd

OFM1_name = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2018TFPSensitivity\ACS2016_controls_2018TFPsensitivity_estimate.csv"
OFM2_name = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2044\ACS2016_controls_2044_estimate.csv"

OFM1_df = pd.read_csv(OFM1_name)
OFM2_df = pd.read_csv(OFM2_name)

OFM_df = pd.merge(OFM1_df, OFM2_df[['block_group_id', 'hh_bg_weight', 'pers_bg_weight']], how = 'left', on = 'block_group_id')
OFM_df.to_csv(r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2044\popsim_control_comparison.csv')

print 'Done'