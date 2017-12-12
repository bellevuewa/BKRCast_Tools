import pandas as pd
import h5py
import numpy as np


h5fn_in = r'D:\BKRCast_Calibration\BKRCast_v1-2_Calibration\BKRCast_Run21-S42-neg_coef6-nopopsampler\inputs\hh_and_persons.h5'
outputFn = r'D:\BKRCast_Calibration\BKRCast_v1-2_Calibration\BKRCast_Run21-S42-neg_coef6-nopopsampler\outputs\standard_workers_by_taz.csv'
print('Input ' + h5fn_in)
print('output ' + outputFn)
hdf_file = h5py.File(h5fn_in, "r")
#parcel_df = pd.read_csv(r'R:\SoundCast\Inputs\2040_plan_update\landuse\parcels_urbansim.txt', sep = ' ')


#transit_df = pd.read_csv(r'W:\gis\projects\stefan\TOD_2017\parcels_2040.csv')
#transit_df = transit_df[transit_df.transit_stop_id >0]
#transit_df = transit_df[['parcelid']]


def h5_to_df(h5_file, group_name):
    """
    Converts the arrays in a H5 store to a Pandas DataFrame. 
    h5_file: h5  file name
    group_name: group name
    """
    col_dict = {}
    h5_set = hdf_file[group_name]
    for col in h5_set.keys():
        my_array = np.asarray(h5_set[col])
        col_dict[col] = my_array
    df = pd.DataFrame(col_dict)
    return df

print("")
person_df = h5_to_df(hdf_file, 'Person')
hh_df = h5_to_df(hdf_file, 'Household')

# add some fields from HH to person table:
merge_df = hh_df[['hhno', 'hhtaz', 'hhexpfac']]
person_df = person_df.merge(merge_df, how='left', left_on = 'hhno', right_on = 'hhno')
workers = person_df[person_df.pwtyp >0]
workers_per_taz = pd.DataFrame(workers.groupby('hhtaz').hhexpfac.sum())      # Ben (RSG) said we should use hhexpfac, not psexpfac.
workers_per_taz.reset_index(level = 0, inplace=True)
workers_per_taz.columns = ['TAZ', '# of workers']
workers_per_taz.to_csv(outputFn)



