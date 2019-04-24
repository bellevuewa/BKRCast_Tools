import os, sys
sys.path.append(os.getcwd())
import pandas as pd
import numpy as np

filename = r'Z:\Modeling Group\BKRCast\2014ESD\parcels_urbansim.txt'
print "Loading input files ..."
parcels = pd.DataFrame.from_csv(filename, sep = " ", index_col = "PARCELID")

#sum_by_county = parcels.groupby('JURISDICTION').sum()
sum = parcels.sum()
sum.to_csv(r'Z:\Modeling Group\BKRCast\2014ESD\LUsummary.txt')
print 'Done'

#sum_by_county.to_csv(r'Z:\Modeling Group\BKRCast\2018LU\2018sqft_by_jurisdiction.csv')



