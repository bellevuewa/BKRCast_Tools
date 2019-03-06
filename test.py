import os
import pandas as pd

filename = r"Z:\Modeling Group\BKRCast\Job Conversion Test\parcel_level\test11\parcels_urbansim.txt"
parcels = pd.DataFrame.from_csv(filename, sep = " ")
parcels.reset_index(inplace = True)
Output_Field = ['PARCELID', 'LUTYPE_P', 'TAZ_P', 'XCOORD_P', 'YCOORD_P']

parcels[Output_Field].to_csv(r'Z:\Modeling Group\BKRCast\Job Conversion Test\parcel_level\test11\parcels.txt', sep = ' ')

