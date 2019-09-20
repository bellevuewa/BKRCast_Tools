import os, sys
sys.path.append(os.getcwd())
import pandas as pd
import numpy as np

filename = r'D:\BKR0V1-1-newcap-newpop-coeff5-nettreaks2\outputs\_person.tsv'
print "Loading input files ..."
persons = pd.DataFrame.from_csv(filename, sep = '\t')

parcels = pd.DataFrame.from_csv(r"Z:\Modeling Group\BKRCast\2014_ParkingCost\Half_parking_Cost_Bellevue_newpop\parcels_urbansim.txt", sep = ' ')
jobs_by_taz = parcels.groupby('TAZ_P').sum()

persons_by_taz = persons.groupby('pwtaz').sum()


print 'Done'

#sum_by_county.to_csv(r'Z:\Modeling Group\BKRCast\2018LU\2018sqft_by_jurisdiction.csv')



