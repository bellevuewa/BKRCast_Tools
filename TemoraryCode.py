import os, sys
sys.path.append(os.getcwd())
import pandas as pd

tour_fn = r"D:\projects\tnc_mode_test_in_py3\BKRCast\outputs\_tour.tsv"

tour_df = pd.read_csv(tour_fn, sep = '\t')
tour_mode_df = tour_df[['tmodetp', 'toexpfac']].groupby('tmodetp').sum()

trip_fn = r"D:\projects\tnc_mode_test_in_py3\BKRCast\outputs\_trip.tsv"

trip_df = pd.read_csv(trip_fn, sep = '\t')
trip_mode_df = trip_df[['mode', 'trexpfac']].groupby('mode').sum()

print('Done')


