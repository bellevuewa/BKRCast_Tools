import sys, os
import pandas as pd


Job_Categories = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPOTH_P', 'EMPRET_P', 'EMPRSC_P', 'EMPSVC_P']
Summary_Categories = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPOTH_P', 'EMPRET_P', 'EMPSVC_P', 'EMPTOT_P', 'STUGRD_P', 'STUHGH_P', 'STUUNI_P', 'HH_P']

def interpolate_two_parcel_files(parcel_file_name_ealier, parcel_file_name_latter, earlier_year, latter_year, horizon_year):
    '''generate a new parcel from lower_fiel and upper_fle by interpolation. Only jobs and students are interpolated.
        output datagfram is returned back to the caller.
        parcel files are separated by ' '.
    '''
    parcel_earlier_df = pd.read_csv(parcel_file_name_ealier, sep = ' ', low_memory = False)
    parcel_earlier_df.columns = [i.upper() for i in parcel_earlier_df.columns]
    parcel_latter_df = pd.read_csv(parcel_file_name_latter, sep = ' ', low_memory = False)
    parcel_latter_df.columns = [i.upper() for i in parcel_latter_df.columns]

    columns = list(Job_Categories)
    columns.append('PARCELID')
    job_std = list(Job_Categories)
    job_std.extend(['STUGRD_P', 'STUHGH_P', 'STUUNI_P'])

    parcel_latter_df.set_index('PARCELID', inplace = True)
    parcels_from_latter_df = parcel_latter_df.loc[:,job_std]
    parcels_from_latter_df.columns = [i + '_L' for i in parcels_from_latter_df.columns]
    parcels_from_latter_df['EMPTOT_L'] = 0
    for cat in Job_Categories:
        parcels_from_latter_df['EMPTOT_L'] = parcels_from_latter_df[cat + '_L'] + parcels_from_latter_df['EMPTOT_L']

    print('Total jobs in year ', latter_year, ' are ', parcels_from_latter_df['EMPTOT_L'].sum())
    parcel_horizon_df = parcel_earlier_df.merge(parcels_from_latter_df.reset_index(), how = 'inner', left_on = 'PARCELID', right_on = 'PARCELID')

    parcel_horizon_df['EMPTOT_E'] = 0
    for cat in Job_Categories:
        parcel_horizon_df['EMPTOT_E'] = parcel_horizon_df['EMPTOT_E'] + parcel_horizon_df[cat]
    parcel_horizon_df['EMPTOT_P'] = parcel_horizon_df['EMPTOT_E']
    print('Total jobs in year ', earlier_year, ' are ', parcel_horizon_df['EMPTOT_P'].sum())

    # interpolate number of jobs, and round to integer.
    for cat in job_std:
        parcel_horizon_df[cat] = parcel_horizon_df[cat] + ((horizon_year - earlier_year) * 1.0 / (latter_year - earlier_year) * (parcel_horizon_df[cat + '_L'] - parcel_horizon_df[cat])) 
        parcel_horizon_df[cat] = parcel_horizon_df[cat].round(0).astype(int)

    parcel_horizon_df['EMPTOT_P'] = 0
    for cat in Job_Categories:
        parcel_horizon_df['EMPTOT_P'] = parcel_horizon_df['EMPTOT_P'] + parcel_horizon_df[cat]

    parcel_horizon_df = parcel_horizon_df.drop([i + '_L' for i in job_std], axis = 1)
    parcel_horizon_df = parcel_horizon_df.drop(['EMPTOT_L', 'EMPTOT_E'], axis = 1)

    return parcel_horizon_df

def validate_parcel_file(base_parcel_df):
    validation_dict = {}  
    output_list = []
    header = ["Column", "Data Type", "Unique Values", "Missing Values", "Duplicated", "Min", "Max", "Mean"]

    for col in base_parcel_df.columns:
        series = base_parcel_df[col]
        unique_non_null = series.nunique(dropna = True)
        missing = series.isna().sum()
        duplicates = len(series) - unique_non_null - missing
        is_numeric = pd.api.types.is_numeric_dtype(series)
        min = series.min() if is_numeric else ""
        max = series.max() if is_numeric else ""
        mean = series.mean() if is_numeric else ""

        outputs = {
            "Column": col,
            "Data Type": str(series.dtype),
            "Unique Values": unique_non_null,
            "Missing Values": missing,
            "Duplicated": duplicates,
            "Min": min,
            "Max": max,
            "Mean": mean
        }

        output_list.append(outputs)

    # df: validation of data_df
    df = pd.DataFrame(output_list, columns = header)

    # df2: data_df shape
    df2 = pd.DataFrame([{"Rows": base_parcel_df.shape[0], "Columns": base_parcel_df.shape[1]}])

    df3 = base_parcel_df.head(100)   
    
    validation_dict = {
        "Validation": df,
        "Summary": df2,
        "Raw Data Samples": df3
    }
    
    return validation_dict

def summarize_parcel_data(parcel_df, subarea_df, output_dir):
    parcel_df = parcel_df.merge(subarea_df[['BKRCastTAZ', 'Jurisdiction', 'Subarea']], left_on="TAZ_P", right_on = "BKRCastTAZ", how="left")
    summary_jurisdictions = parcel_df.groupby('Jurisdiction')[Summary_Categories].sum().reset_index()
    summary_taz = parcel_df.groupby('TAZ_P')[Summary_Categories].sum().reset_index()
    summary_subarea = parcel_df.groupby('Subarea')[Summary_Categories].sum().reset_index()
    summary_subarea = summary_subarea.merge(subarea_df[['Subarea', 'SubareaName']].drop_duplicates(), on='Subarea', how='left')

    if output_dir is None:
        output_dir = os.getcwd()
    summary_jurisdictions.to_csv(os.path.join(output_dir, 'parcel_summary_by_jurisdiction.csv'), index=False)
    summary_taz.to_csv(os.path.join(output_dir, 'parcel_summary_by_taz.csv'), index=False)
    summary_subarea.to_csv(os.path.join(output_dir, 'parcel_summary_by_subarea.csv'), index=False)

    summary_dict = {
        "Jurisdiction": summary_jurisdictions,
        "Subarea": summary_subarea,
        "TAZ": summary_taz
    }

    return summary_dict