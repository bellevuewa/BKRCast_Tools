import pandas as pd
import os
import matplotlib
matplotlib.use('Qt4Agg')
import matplotlib.pyplot as plt
import numpy as np
import datetime


def plotPnR(pnrLoad, xlabel, ylabel, title):
    tm = []
    for hr in range(24):
        for min in range(60):
            tm.append(datetime.time(hr, min))
    tm = np.array(tm)
    pnrplt = plt.plot(tm, pnrLoad)
    pnrplt.xlabel(xlabel)
    pnrplt.ylabel(ylabel)
    pnrplt.title(title)



trips_file = r'D:\Soundcast\SC2040\soundcast-2.1\outputs\daysim\_trip.tsv'
tour_file = r'D:\BKR0V1-1-newcap-newpop-S93-newdensity-shortwalkdist-coeff8-newadjfacs-nohomejobs-BSq_attractiveness\outputs\_tour.tsv'
park_and_ride_file = r'D:\BKR0V1-1-newcap-newpop-S93-newdensity-shortwalkdist-coeff8-newadjfacs-nohomejobs-BSq_attractiveness\inputs\p_r_nodes.csv'
pnr_pricing_sc_fie = r'D:\Soundcast\SC2040\soundcast-2.1\outputs\daysim\archive_park_and_ride_shadow_prices.txt'
pnr_pricing_bkr_file = r'D:\BKR0V1-1-newcap-newpop-S93-newdensity-shortwalkdist-coeff8-newadjfacs-nohomejobs-BSq_attractiveness\working\park_and_ride_shadow_prices.txt'

#trips_df = pd.DataFrame.from_csv(trips_file, sep = '\t')
#pnr_df = pd.DataFrame.from_csv(park_and_ride_file, index_col = 'ZoneID')
#tour_df = pd.DataFrame.from_csv(tour_file, sep = '\t')

pnr_pricing_sc_df = pd.DataFrame.from_csv(pnr_pricing_sc_fie, sep = '\t')
pnr_pricing_bkr_df = pd.DataFrame.from_csv(pnr_pricing_bkr_file, sep = '\t')
pnr_bkr_load_df = pnr_pricing_bkr_df.iloc[:, 4320:5760]
pnr_bkr_price_df = pnr_pricing_bkr_df.iloc[:, 1440:2880]
sbpnr = np.array(pnr_bkr_load_df.loc[1392])
egpnr = np.array(pnr_bkr_load_df.loc[1394])
both = sbpnr + egpnr

sbpnrprice = np.array(pnr_bkr_price_df.loc[1392])
egpnrprice = np.array(pnr_bkr_price_df.loc[1394])
bothprice = sbpnrprice + egpnrprice

tm = []
for hr in [3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,0,1,2]:
    for min in range(60):
        tm.append(datetime.time(hr, min))
tm = np.array(tm)

plt.figure(1)
sbpnrplt = plt.plot(tm, sbpnr, label = 'S Bel PnR')
egpnrplt = plt.plot(tm, egpnr, label = 'Eastgate PnR')
bothplt = plt.plot(tm, both, label = 'S Bel + Eastgate PnR')
plt.ylabel('Occupied')
plt.title('Park and Ride Use')
plt.grid(True, which = 'both', axis = 'both')
plt.legend()

plt.figure(2)
sbpnrpriceplt = plt.plot(tm, sbpnrprice, label = 'S Bel PnR')
egpnrpriceplt = plt.plot(tm, egpnrprice, label = 'Eastgate PnR')
#bothpriceplt = plt.plot(tm, bothprice, label = 'S Bel + Eastgate PnR')
plt.ylabel('Occupied')
plt.title('Park and Ride Use - Pricing')
plt.grid(True, which = 'both', axis = 'both')
plt.legend()
plt.show()

    


print ('Done')