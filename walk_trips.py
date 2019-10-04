import os, sys
sys.path.append(os.getcwd())
import pandas as pd
import numpy as np

trips_file = r'D:\BKR0V1-1-newcap-newpop-S93-newdensity\outputs\_trip.tsv'
total_trips_df = pd.DataFrame.from_csv(trips_file, sep = '\t')

walk_trips = total_trips_df.loc[total_trips_df['mode'] == 1]
walk_to_BSquare = walk_trips.loc[walk_trips['dtaz'] == 11]
too_long_walk_trips_to_BSquare = walk_to_BSquare.loc[walk_to_BSquare['travdist'] > 3]
walk_trips_to_BS_by_otaz = walk_to_BSquare.groupby('otaz')['trexpfac'].sum()
long_walk_to_BS_by_otaz = too_long_walk_trips_to_BSquare.groupby('otaz')['trexpfac'].sum()

too_long_walk_trips_to_BSquare.to_csv('D:\BKR0V1-1-newcap-newpop-S93-newdensity\long_walk_trips_to_BSquare.csv')
long_walk_to_BS_by_otaz.to_csv('D:\BKR0V1-1-newcap-newpop-S93-newdensity\outputs\long_walk_trips_to_BSquare_by_otaz.csv')
walk_trips_to_BS_by_otaz.to_csv('D:\BKR0V1-1-newcap-newpop-S93-newdensity\outputs\walk_trips_to_BSquare_by_otaz.csv')
print 'Done'
