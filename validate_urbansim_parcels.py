import os
import pandas as pd

parcel_name = r'Z:\Modeling Group\BKRCast\LandUse\Complan\Complan2044\NA\parcels_urbansim.txt'

parcels_df = pd.read_csv(parcel_name, sep = ' ')

JOB_CATEGORY = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPOTH_P', 'EMPRET_P', 'EMPSVC_P', 'EMPTOT_P', 'HH_P', 'STUGRD_P', 'STUHGH_P', 'STUUNI_P']

## check duplicated ID
dup_id = parcels_df.loc[parcels_df['PARCELID'].duplicated()]
if dup_id.empty == False:
    dup_id.to_csv(os.path.join(os.path.dirname(parcel_name), 'duplicated_parcels.txt'))
    print('Duplicated IDs are exported.')
    
# check NA value
nan_rows = parcels_df[parcels_df.isnull().any(1)]
if nan_rows.empty == False:
    nan_rows.to_csv(os.path.join(os.path.dirname(parcel_name), 'missing_value_parcels.txt'))
    print('Parcels with missing values are exported.')

for col in JOB_CATEGORY:
    if min(parcels_df[col] < 0):
        print(f'{col}  has some negative numbers')

print('Done')
