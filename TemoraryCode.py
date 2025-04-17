import os, sys
sys.path.append(os.getcwd())
import pandas as pd

new_file = r"D:\daysim_outputs_processing\errand_trips.tsv"
old_file = r"D:\WFH_test\tours_and_trips\BKR3-19-L44-preferred_WFH_30%WFH_50%errand\outputs\daysim\trips_to_be_removed.csv"

new_df = pd.read_csv(new_file, sep = '\t')
old_df = pd.read_csv(old_file)

new_df['tripid']=new_df['tour_id'].astype(str) + '_' + new_df['half'].astype(str) + '_' + new_df['tseg'].astype(str)
old_df['tripid']=old_df['tour_id'].astype(str) + '_' + old_df['half'].astype(str) + '_' + old_df['tseg'].astype(str)

new_df
print('Done')


