'''
   synthetic population from PSRC is based on PSRC TAZ. This tool is to convert PSRC TAZ in household to BKRCast TAZ.

2/3/2022
upgrade to python 3.7

'''

import os, shutil
import pandas as pd
import h5py
import numpy as np
from utility import *

#input settings
wd = r"Z:\Modeling Group\BKRCast\SoundCast\2050_Inputs"
popsynFileName = "2050_SC_hh_and_persons.h5"
parcel_bkr_taz_file = r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel2014_BKRCastTAZ.csv"


#read popsyn file
print('Loading hh and person file...')
hdf_file = h5py.File(os.path.join(wd, popsynFileName), "r")
person_df = h5_to_df(hdf_file, 'Person')
hh_df = h5_to_df(hdf_file, 'Household')

#read parcel to bkr taz correspondence
parcel_bkr_taz = pd.read_csv(parcel_bkr_taz_file, sep = ',')

#merge to households
print('assign bkr tazs')
households = pd.merge(hh_df, parcel_bkr_taz, left_on = "hhparcel", right_on = "PARCELID")
    
households["hhtaz"] = households["BKRCastTAZ"].astype(np.int32)
households.drop(parcel_bkr_taz.columns, inplace=True, axis=1)
households = households.sort_values("hhno")

#write result file by copying input file and writing over arrays
popsynOutFileName = popsynFileName.split(".")[0]+ "_bkr.h5"
out_h5_file = h5py.File(os.path.join(wd, popsynOutFileName), 'w')
print('Export households and persons...')
df_to_h5(households, out_h5_file, 'Household')
df_to_h5(person_df, out_h5_file, 'Person')
out_h5_file.close()


print('Done')




