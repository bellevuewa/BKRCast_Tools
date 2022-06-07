import os, sys
sys.path.append(os.getcwd())
import pandas as pd

tour_fn = r"D:\TFP\2033TFP\Full 2033 TFP\BKR2-19-T33\outputs\_tour.tsv"

tour_df = pd.read_csv(tour_fn, sep = '\t')
tour_mode_df = tour_df[['tmodetp', 'toexpfac']].groupby('tmodetp').sum()

trip_fn = r"D:\TFP\2033TFP\Full 2033 TFP\BKR2-19-T33\outputs\_trip.tsv"

trip_df = pd.read_csv(trip_fn, sep = '\t')
trip_mode_df = trip_df[['mode', 'trexpfac']].groupby('mode').sum()

print('Done')


