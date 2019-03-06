import pandas as pd
import os
import numpy as np

trips_file = r'D:\2035BKRCastBaseline\2035BKRCastBaseline - Copy\outputs\daysim\_trip.tsv'
tour_file = r'D:\2035BKRCastBaseline\2035BKRCastBaseline - Copy\outputs\_tour.tsv'

tours_df = pd.DataFrame.from_csv(tour_file, sep = '\t')

tours = tours_df[tours_df['topcl'] == 796823]

tours.to_csv(r'D:\2035BKRCastBaseline\tours_my_house.csv')
print 'Done'


