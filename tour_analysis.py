import pandas as pd
import os
import numpy as np

trips_file = r'D:\projects\Complan\2044complan\new_popsim_approach\BKR3-19-L44-BASELINE_opcost36\outputs\daysim\_trip.tsv'
tour_file = r'D:\projects\Complan\2044complan\new_popsim_approach\BKR3-19-L44-Alt1\outputs\daysim\_tour.tsv'

tours_df = pd.read_csv(tour_file, sep = '\t')
tour_taz344_df = tours_df[tours_df['tdtaz']==344]
tour_taz344_df[['pdpurp', 'toexpfac']].groupby('pdpurp').sum().reset_index()


print('Done')


