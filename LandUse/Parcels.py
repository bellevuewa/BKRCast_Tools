import os, sys
sys.path.append(os.getcwd())

import random
import logging
import pandas as pd
from abc import ABC, abstractmethod
import utility
import numpy as np

from utility import *

if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")

logger = logging.getLogger(__name__)


class Parcels:
    def __init__(self, subarea_file, lookup_file, filename, data_year):
        """
        Initialize the class with data.
        :param data: Dictionary containing land use data.
        """
        self.data_year = data_year # year of the parcel data
        self.filename = filename # original parcel data filename

        #####
        # Step 1
        #####
        self.lookup_df = pd.read_csv(lookup_file, sep = ',', low_memory = False)
        self.subarea_df = pd.read_csv(subarea_file, sep = ',', low_memory = False)

        #####
        # Step 2
        #####
        self.original_parcels_df = pd.read_csv(filename, sep = ' ', low_memory = False)

        # self.parcels_df : pd.DataFrame= None

    def copy(self) -> "Parcels":
        """for deep copy"""

        return Parcels.from_dataframe(self.original_parcels_df, self.data_year, self.filename, self.subarea_df)

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame, data_year: int, filename: str, subarea_df: pd.DataFrame, lookup_df: pd.DataFrame) -> "Parcels":
        """Initialize Parcels from a DataFrame. a python way to have multiple constructors."""
        obj = cls.__new__(cls) # Create an uninitialized instance
        obj.original_parcels_df = df.copy()
        obj.data_year = data_year
        obj.filename = filename
        obj.lookup_df = lookup_df.copy()
        obj.subarea_df = subarea_df.copy()
        # obj.parcels_df = None
        return obj


    def summarize_parcel_data(self, output_dir) -> dict:
        parcel_df = self.original_parcels_df.merge(self.subarea_df[['BKRCastTAZ', 'Jurisdiction', 'Subarea']], left_on="TAZ_P", right_on = "BKRCastTAZ", how="left")
        summary_jurisdictions = parcel_df.groupby('Jurisdiction')[Summary_Categories].sum().reset_index()
        summary_taz = parcel_df.groupby('TAZ_P')[Summary_Categories].sum().reset_index()
        summary_subarea = parcel_df.groupby('Subarea')[Summary_Categories].sum().reset_index()
        summary_subarea = summary_subarea.merge(self.subarea_df[['Subarea', 'SubareaName']].drop_duplicates(), on='Subarea', how='left')

        if output_dir is None:
            output_dir = os.getcwd()

        # the exported files could overwrite other parcel summary files. need to think about better naming convention later.
        summary_jurisdictions.to_csv(os.path.join(output_dir, 'parcel_summary_by_jurisdiction.csv'), index=False)
        summary_taz.to_csv(os.path.join(output_dir, 'parcel_summary_by_taz.csv'), index=False)
        summary_subarea.to_csv(os.path.join(output_dir, 'parcel_summary_by_subarea.csv'), index=False)

        logging.info(f'Parcel summary by jurisdiction exported to: {os.path.join(output_dir, "parcel_summary_by_jurisdiction.csv")}')
        logging.info(f'Parcel summary by TAZ exported to: {os.path.join(output_dir, "parcel_summary_by_taz.csv")}')
        logging.info(f'Parcel summary by subarea exported to: {os.path.join(output_dir, "parcel_summary_by_subarea.csv")}')

        summary_dict = {
            "Jurisdiction": summary_jurisdictions,
            "Subarea": summary_subarea,
            "TAZ": summary_taz
        }

        return summary_dict

    def validate_parcel_file(self) -> dict:
        import debugpy
        debugpy.breakpoint()
        
        validation_dict = {}  
        output_list = []
        header = ["Column", "Data Type", "Unique Values", "Missing Values", "Duplicated", "Min", "Max", "Mean"]

        for col in self.original_parcels_df.columns:
            series = self.original_parcels_df[col]
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
        df2 = pd.DataFrame([{"Rows": self.original_parcels_df.shape[0], "Columns": self.original_parcels_df.shape[1]}])

        df3 = self.original_parcels_df.head(100)   
        
        validation_dict = {
            "Validation": df,
            "Summary": df2,
            "Raw Data Samples": df3
        }
        
        return validation_dict

    # this method works as a separate function. need refactoring later to fit into the class structure
    def controlled_rounding(self, data_df, attr_name, control_total, index_attr_name):
        # find residential parcels within taz     
        updated_data_df = data_df.loc[data_df[attr_name] > 0].copy()
        total_rows = updated_data_df.shape[0]
        if total_rows != 0:
            already_assigned = updated_data_df[attr_name].sum()
        else:
            already_assigned = 0
        
        # how many need to be assigned or removed to match the control total
        diff = int(control_total - already_assigned)
        if (diff == 0) | total_rows == 0:
            return data_df

        if total_rows >= abs(diff):
            selected_indices = np.random.choice(updated_data_df.index, size = abs(diff), replace = False) 
        else:
            selected_indices = np.random.choice(updated_data_df.index, size = abs(diff), replace = True) 
        
        unique_indices, counts = np.unique(selected_indices, return_counts=True)
        sorted_zipped = sorted(zip(unique_indices, counts), key=lambda x: x[1], reverse=True)

        index_for_2nd_round = []
        if control_total >= already_assigned: # need to add to match the control total, 
            for index, count in sorted_zipped:
                updated_data_df.loc[index, attr_name] += count   
        else:  # need to remove to match the control total. more complicated.
            remaining = 0
            for index, count in sorted_zipped:
                count += remaining
                # need to ensure no negative values
                if updated_data_df.loc[index, attr_name] >= count:
                    updated_data_df.loc[index, attr_name] -= count
                    remaining = 0
                    index_for_2nd_round.append(index)
                else:
                    remaining = count - updated_data_df.loc[index, attr_name]
                    updated_data_df.loc[index, attr_name] = 0

            if (remaining > 0):
                for index in index_for_2nd_round:
                    curValue = updated_data_df.loc[index, attr_name]
                    if curValue >= remaining:
                        updated_data_df.loc[index, attr_name] = curValue - remaining
                        remaining = 0
                        break
                    else:
                        remaining -= updated_data_df.loc[index, attr_name]
                        updated_data_df.loc[index, attr_name] = 0
        
        new_data_df = data_df.copy()
        new_data_df = new_data_df.loc[~new_data_df[index_attr_name].isin(updated_data_df[index_attr_name])]
        new_data_df = pd.concat([new_data_df, updated_data_df])
        
        return new_data_df

    # def step_1_prepare_land_use(self):
    #     """
    #     Step 1: prepare land use data for analysis.
    #     :return: Prepared land use data.
    #     """
    #     # 2/23/2021
    #     # this script is used to join parcel data from Community Development to BKRCastTAZ and subarea
    #     # # via PSRC_ID, and save jobs and sqft data to different data files. IF the parcels provided in the kingsqft file are not valid parcels in
    #     # lookup_file, these invalid parcels will be exported to error_parcel_file for further investigation. 
    #     # Sometimes BKRCastTAZ and subarea column in the data from CD are a little mismatched
    #     # # so it is always good to remap parcel data to lookup file to ensure we always
    #     # # summarize land use  on the same base data.

    #     # 2/28/2022
    #     # upgrade to Python 3.7

    #     # 5/1/2025
    #     # move the paths into config.py

    #     self.kc_df = pd.read_csv(os.path.join(working_folder_lu, kingcsqft), sep = ',', low_memory = False)
    #     self.subarea_df = pd.read_csv(subarea_file, sep = ',')

    #     # rename columns to fit modeling input format
    #     self.kc_df.rename(columns = job_rename_dict, inplace = True)
    #     self.kc_df.rename(columns = sqft_rename_dict, inplace = True)
    #     self.kc_df.rename(columns = du_rename_dict, inplace = True)

    #     logging.info('Exporting job file...')
    #     # kc_df dataframe may already have ['PSRC_ID', 'JURIS', 'BKRCASTTAZ'], the merge below just to ensure these features match with our BKRCast model
    #     # TODO: why below merging with inner instead of left, but for sqft data is with left instead of inner?
    #     updated_jobs_kc = self.kc_df[jobs_columns_List].merge(self.lookup_df[['PSRC_ID', 'Jurisdiction', 'BKRCastTAZ']], left_on = 'PSRC_ID', right_on = 'PSRC_ID', how = 'inner')
    #     updated_jobs_kc = updated_jobs_kc.merge(self.subarea_df[['BKRCastTAZ', 'Subarea', 'SubareaName']], left_on = 'BKRCastTAZ', right_on = 'BKRCastTAZ', how = 'left')
    #     if subset_area != []:
    #         updated_jobs_kc = updated_jobs_kc[updated_jobs_kc['Jurisdiction'].isin(subset_area)]
    #     # calculate sum of the worker each parcel; EMPTOT_P: the total number of employees working on a parcel
    #     updated_jobs_kc['EMPTOT_P'] = updated_jobs_kc[job_cat_list].sum(axis = 1)
    #     updated_jobs_kc.to_csv(os.path.join(working_folder_lu, kc_job_file), sep = ',', index = False)

    #     if SQFT_data_available: 
    #         logging.info('Exporting sqft file...')
    #         # kc_df dataframe may already have ['PSRC_ID', 'JURIS', 'BKRCASTTAZ'], the merge below just to ensure these features match with our BKRCast model
    #         updated_sqft_kc = self.kc_df[sqft_columns_list].merge(self.lookup_df[['PSRC_ID', 'Jurisdiction', 'BKRCastTAZ']], left_on = 'PSRC_ID', right_on = 'PSRC_ID', how = 'left')
    #         updated_sqft_kc = updated_sqft_kc.merge(self.subarea_df[['BKRCastTAZ', 'Subarea', 'SubareaName']], left_on = 'BKRCastTAZ', right_on = 'BKRCastTAZ', how = 'left')
    #         if subset_area != []:
    #             updated_sqft_kc = updated_sqft_kc[updated_sqft_kc['Jurisdiction'].isin(subset_area)]
    #         updated_sqft_kc['SQFT_TOT'] = updated_sqft_kc[sqft_cat_list].sum(axis = 1)       
    #         updated_sqft_kc.to_csv(os.path.join(working_folder_lu, kc_SQFT_file), sep = ',', index = False)
    #         logging.info(f'Sqft file exported: {os.path.join(working_folder_lu, kc_SQFT_file)}')

    #     logging.info('Exporting King County dwelling units...')
    #     # TODO: why below merging with inner instead of left, but for sqft data (above) is with left instead of inner?
    #     du_kc = self.kc_df[dwellingunits_list].merge(self.lookup_df[['PSRC_ID', 'Jurisdiction', 'BKRCastTAZ']], left_on = 'PSRC_ID', right_on = 'PSRC_ID', how = 'inner')
    #     du_kc = du_kc.merge(self.subarea_df[['BKRCastTAZ', 'Subarea', 'SubareaName']], left_on = 'BKRCastTAZ', right_on = 'BKRCastTAZ', how = 'left')
    #     if subset_area != []:
    #         du_kc = du_kc[du_kc['Jurisdiction'].isin(subset_area)]
    #     du_kc.to_csv(os.path.join(working_folder_lu, kc_du_file), sep  = ',', index = False)
    #     logging.info(f'King County dwelling units file exported: {os.path.join(working_folder_lu, kc_du_file)}')

    #     du_cob = du_kc[du_kc['Jurisdiction'] == 'BELLEVUE']
    #     du_cob.to_csv(os.path.join(working_folder_lu, cob_du_file), sep = ',', index = False)
    #     logging.info(f'Bellevue dwelling units file exported: {os.path.join(working_folder_lu, cob_du_file)}')

    #     error_parcels = self.kc_df[~self.kc_df['PSRC_ID'].isin(self.lookup_df['PSRC_ID'])]
    #     error_parcels.to_csv(os.path.join(working_folder_lu, error_parcel_file), sep = ',', index = False)
    #     if error_parcels.shape[0] > 0:
    #         logging.warning('Exporting error file...')
    #         logging.warning(f'Please check the error file first: {os.path.join(working_folder_lu, error_parcel_file)}')

    #     logging.info('Backing up the scripts for step 1...')
    #     os.makedirs(os.path.join(working_folder_lu, self.backup_folder, version), exist_ok=True)
    #     utility.backupScripts(__file__, os.path.join(working_folder_lu, self.backup_folder, version, os.path.basename(__file__)))
    #     logging.info(f'Scripts for step 1 backup exported: {os.path.join(working_folder_lu, self.backup_folder, version, os.path.basename(__file__))}')
    #     logging.info('Step 1 done. Land use data has been prepared.\n')

    # def step_2_validate_input_parcels(self):
    #     """
    #     Step 2: Validate parcel data from local jurisdiction.
    #     Validate the input parcel land use data file provided by community development. It checks:
    #             1. uniqueness of PSRC_ID
    #             2. parcels in lookup file but missing in the land use data file. 
    #             3. parcels in land use data file but not in lookup file. 
    #     :return: Validation results.
    #     """

    #     # 5/1/2025
    #     # move the paths into config.py
    #     # remove specifying year "2014", replace it with year_parcel

    #     self.parcels_df = pd.read_csv(os.path.join(working_folder_lu, parcel_data_file_name), sep = ',')
    #     # check if the parcel data has duplicated PSRC ids
    #     duplicated_parcels_df = self.parcels_df[self.parcels_df.duplicated('PSRC_ID', keep = False)]
    #     if duplicated_parcels_df.shape[0] != 0:
    #         duplicated_parcels_df.to_csv(os.path.join(working_folder_lu, f'duplicated_parcels_{modeller_initial}_{version}.csv'))
    #         logging.warning(f"Some parcels have duplicated PSRC_ID. See {os.path.join(working_folder_lu, f'duplicated_parcels_{modeller_initial}_{version}.csv')} for details.")
    #         # export cleaned copy, only keep the first one if duplicated.
    #         self.parcels_df = self.parcels_df[~self.parcels_df.duplicated('PSRC_ID', keep = 'first')]
    #         self.parcels_df.to_csv(os.path.join(working_folder_lu, 'cleaned_' + parcel_data_file_name), index = False)
    #     else:
    #         logging.info('No parcel with duplicated PSRC_ID is found. ')

    #     parcels_df = self.parcels_df.groupby('PSRC_ID').sum()

    #     # check and export parcels that are given in the parcel_data_file_name but are not included in lookup_df
    #     not_in_year_PSRC_parcels = parcels_df.loc[~parcels_df.index.isin(self.lookup_df['PSRC_ID'])]
    #     if not_in_year_PSRC_parcels.empty == False:
    #         not_in_year_PSRC_parcels.to_csv(os.path.join(working_folder_lu, f'parcels_not_in_{year_parcel}_PSRC_parcels_{modeller_initial}_{version}.csv'))
    #         logging.warning(f"Some parcels are not within parcel lookup file, See {os.path.join(working_folder_lu, f'parcels_not_in_{year_parcel}_PSRC_parcels_{modeller_initial}_{version}.csv')} for details.")
    #     else:
    #         logging.info('All parcels given are within parcel lookup file.')

    #     if Jurisdiction != None:
    #         selected_parcels_lookup_df = self.lookup_df.loc[self.lookup_df['Jurisdiction'] == Jurisdiction]
    #     else:
    #         selected_parcels_lookup_df = self.lookup_df

    #     # check and export parcels that are in lookup_df but not in the parcel_data_file_name.
    #     not_in_given_parcel_dataset = selected_parcels_lookup_df.loc[~selected_parcels_lookup_df['PSRC_ID'].isin(parcels_df.index)]
    #     if not_in_given_parcel_dataset.empty == False:
    #         not_in_given_parcel_dataset.to_csv(os.path.join(working_folder_lu, f'{year_parcel}_PSRC_parcels_not_in_given_parcel_data_{modeller_initial}_{version}.csv'))
    #         logging.warning(f'Some {year_parcel} PSRC parcels are missing from the given parcel data_{modeller_initial}_{version}.')
    #         logging.warning(f"Go to {os.path.join(working_folder_lu, f'{year_parcel}_PSRC_parcels_not_in_given_parcel_data_{modeller_initial}_{version}.csv')} and check the output error file for details. ")
    #     else:
    #         logging.info(f'No {year_parcel} PSRC parcels are missing in the given parcel dataset.')

    #     logging.info('Step 2 done.')
    #     logging.info('Please make sure the output numbers making sense. Then continue the steps in the Synthetic Population folder.\n')


    # def step_4_update_parcel_columns(self):
    #     """
    #     Step 4: replace_parcel_columns_with_new_tables.py
    #         Replace parcel columns with new tables. Since CD will provide numebr of jobs instead of sqft, we will use this sript to 
    #         replace PSRC's pacel data within King County with sqft converted jobs from CD.
    #         We do not need to run other scripts to handle sqft and conversion so the land use
    #         preparation process becomes more straightforward and clean.
    #     """

    #     # 3/9/2022
    #     # upgraded to python 3.7

    #     # 2023
    #     # allow input jobs file using old trip model TAZ (originally for kirkland complan support)

    #     # 05/01/2025
    #     # move the paths into config.py

    #     # 05/22/2025
    #     # allowed kirkland input job file to update the original interpolated parcel job data

    #     self.new_bellevue_parcel_data = pd.read_csv(os.path.join(working_folder_lu, new_bellevue_parcel_data_file_name), sep = ',', low_memory = False)
    #     self.original_parcel_data_df = pd.read_csv(os.path.join(working_folder_lu, original_parcel_file_name), sep = ' ', low_memory = False)
    #     # processing Bellevue parcel-level jobs
    #     logging.info('Processing Bellevue jobs...')
    #     full_bellevue_parcels_df = self.lookup_df.loc[self.lookup_df['Jurisdiction'] == 'BELLEVUE']
    #     actual_bel_parcels_df = self.new_bellevue_parcel_data.loc[self.new_bellevue_parcel_data['PSRC_ID'].isin(full_bellevue_parcels_df['PSRC_ID'])]
    #     not_in_full_bellevue_parcels = actual_bel_parcels_df.loc[~actual_bel_parcels_df['PSRC_ID'].isin(full_bellevue_parcels_df['PSRC_ID'])]
    #     missing_bellevue_parcels_df = self.original_parcel_data_df.loc[self.original_parcel_data_df['PARCELID'].isin(full_bellevue_parcels_df.loc[~full_bellevue_parcels_df['PSRC_ID'].isin(self.new_bellevue_parcel_data['PSRC_ID']), 'PSRC_ID'])]
    #     if len(not_in_full_bellevue_parcels) > 0:
    #         fname = os.path.join(working_folder_lu, f'not_valid_bellevue_parcels_{modeller_initial}_{version}.csv')
    #         not_in_full_bellevue_parcels.to_csv(fname, sep = ',', index = False)
    #         logging.warning(f'Some parcels missing compared to the Bellevue lookup table. Exported in {fname}\n')
    #     if len(missing_bellevue_parcels_df) > 0:
    #         fname = os.path.join(working_folder_lu, f'missing_bellevue_parcels_{modeller_initial}_{version}.csv')
    #         missing_bellevue_parcels_df.to_csv(fname, sep = ',', index = False)
    #         logging.warning(f'Some parcels are not covered in the Bellevue lookup table. Exported in {fname}\n')        
    #     # compare between the old and new job totals
    #     newjobs_bellevue = self.new_bellevue_parcel_data['EMPTOT_P'].sum() 
    #     logging.info(f'New Bellevue parcel data file has {newjobs_bellevue:,.0f} jobs.\n')
    #     new_bellevue_parcel_data = self.new_bellevue_parcel_data.set_index('PSRC_ID')
    #     updated_parcel_df = self.original_parcel_data_df.copy()
    #     updated_parcel_df = updated_parcel_df.set_index('PARCELID')
    #     oldjobs_bellevue = updated_parcel_df.loc[updated_parcel_df.index.isin(new_bellevue_parcel_data.index), 'EMPTOT_P'].sum()
    #     logging.info(f'Bellevue parcels to be replaced have {oldjobs_bellevue:,.0f} jobs')
    #     logging.info(f'Bellevue parcels after changing have {newjobs_bellevue:,.0f} jobs')
    #     logging.info(f'Bellevue jobs gained {(newjobs_bellevue - oldjobs_bellevue):,.0f}\n')
    #     updated_parcel_df.loc[updated_parcel_df.index.isin(new_bellevue_parcel_data.index), columns_list] = new_bellevue_parcel_data[columns_list]
    #     # process Kirkland parcel-level jobs
    #     if new_kirkland_parcel_data_file_name != '':
    #         logging.info('Processing Kirkland jobs...')
    #         # process the new parcel jobs
    #         new_kirkland_parcel_data = pd.read_excel(os.path.join(working_folder_lu, new_kirkland_parcel_data_file_name), sheet_name='Employment', header=2)
    #         new_kirkland_parcel_data = new_kirkland_parcel_data[['TAZ', 'Total']].copy(deep=True)
    #         new_kirkland_parcel_data.rename(columns={'Total': 'Control', 'TAZ': 'BKRTMTAZ'}, inplace=True)
    #         new_kirkland_parcel_data['Control'] = new_kirkland_parcel_data['Control'].round(0)
    #         kirkland_parcel_tmtazs = self.lookup_df[self.lookup_df['Jurisdiction']=='KIRKLAND']
    #         parcel_data_kirkland = self.original_parcel_data_df[self.original_parcel_data_df['PARCELID'].isin(kirkland_parcel_tmtazs['PSRC_ID'])].copy(deep=True)
    #         parcel_data_kirkland = parcel_data_kirkland.merge(self.lookup_df[['Jurisdiction', 'PSRC_ID', 'BKRTMTAZ', 'BKRCastTAZ']], \
    #                                               left_on='PARCELID', right_on='PSRC_ID', how='left')
    #         # calculate scaling factor
    #         parcel_data_kirkland_tmtaz = parcel_data_kirkland.groupby(by='BKRTMTAZ').sum()['EMPTOT_P'].reset_index()
    #         parcel_data_kirkland_tmtaz = parcel_data_kirkland_tmtaz.merge(new_kirkland_parcel_data[['Control', 'BKRTMTAZ']], how='left')
    #         parcel_data_kirkland_tmtaz['Control'] = parcel_data_kirkland_tmtaz['Control'].round(0)
    #         parcel_data_kirkland_tmtaz['Control'] = parcel_data_kirkland_tmtaz['Control'].astype('float64')
    #         parcel_data_kirkland_tmtaz['EMPTOT_P'] = parcel_data_kirkland_tmtaz['EMPTOT_P'].astype('float64')
    #         parcel_data_kirkland_tmtaz['scaling factor'] = parcel_data_kirkland_tmtaz['Control'] / parcel_data_kirkland_tmtaz['EMPTOT_P']
    #         parcel_data_kirkland_tmtaz['scaling factor'].fillna(0, inplace=True)
    #         # use the scaling factor to all the jobs in parcels within the corresponding travel model TAZ (TMTAZ)
    #         job_columns = [i for i in columns_list if i != 'EMPTOT_P']
    #         parcel_data_kirkland = parcel_data_kirkland.merge(parcel_data_kirkland_tmtaz[['scaling factor', 'BKRTMTAZ']], on='BKRTMTAZ', how='left')
    #         for job_column in job_columns:
    #             parcel_data_kirkland[f"{job_column}_SCALED"] = (parcel_data_kirkland[job_column].astype('float64') * parcel_data_kirkland['scaling factor']).round(0)
    #         job_columns_scaled = [f"{i}_SCALED" for i in columns_list if i != 'EMPTOT_P']
    #         parcel_data_kirkland['EMPTOT_P_SCALED'] = parcel_data_kirkland[job_columns_scaled].sum(axis=1)
    #         # compare the scaled job number with the control (new parcel data)
    #         parcel_data_kirkland_scaled = parcel_data_kirkland.groupby(by='BKRTMTAZ').sum()['EMPTOT_P_SCALED'].reset_index()
    #         parcel_data_kirkland_scaled = parcel_data_kirkland_scaled.merge(parcel_data_kirkland_tmtaz[['Control', 'BKRTMTAZ']], how='left')
    #         parcel_data_kirkland_scaled['difference'] = parcel_data_kirkland_scaled['EMPTOT_P_SCALED'] - parcel_data_kirkland_scaled['Control']
    #         logging.info(f"New Kirkland parcel data file has {parcel_data_kirkland_scaled['Control'].sum():,.0f} jobs.")
    #         logging.info(f"Scaled Kirkland parcel data file has {parcel_data_kirkland_scaled['EMPTOT_P_SCALED'].sum():,.0f} jobs.")
    #         logging.info(f"Old parcel data file has {parcel_data_kirkland['EMPTOT_P'].sum():,.0f} jobs in Kirkland.")
    #         # merge the scaling factor to the table
    #         parcel_data_kirkland_scaled = parcel_data_kirkland_scaled.merge(parcel_data_kirkland_tmtaz[['scaling factor', 'BKRTMTAZ']], on='BKRTMTAZ', how='left')
    #         # walk through each TMTAZ and distribute the difference back to parcels
    #         logging.info(f"(Scaled Kirkland job total - New Kirkland job total) = {parcel_data_kirkland['EMPTOT_P_SCALED'].sum() - new_kirkland_parcel_data['Control'].sum()}")
    #         logging.info("\nFinetuning the number of jobs in each parcel in Kirkland...")
    #         job_scaled_columns = [f'{i}_SCALED' for i in columns_list if i != 'EMPTOT_P']
    #         for _, row in parcel_data_kirkland_scaled.iterrows():
    #             tmtaz = row['BKRTMTAZ']
    #             difference = row['difference']
    #             parcels_in_tmtaz_df = parcel_data_kirkland.loc[(parcel_data_kirkland['BKRTMTAZ'] == tmtaz) & \
    #                                                            (parcel_data_kirkland['EMPTOT_P_SCALED'] > 0)]
    #             if len(parcels_in_tmtaz_df) == 0: continue
    #             if difference < 0:
    #                 # need to add more jobs in those parcels that already have jobs
    #                 for _ in range(int(abs(difference))):
    #                     parcels_in_tmtaz_df = parcel_data_kirkland.loc[(parcel_data_kirkland['BKRTMTAZ'] == tmtaz) & \
    #                                                                    (parcel_data_kirkland['EMPTOT_P_SCALED'] > 0)]
    #                     parcel_ids = list(parcels_in_tmtaz_df['PARCELID'])
    #                     # randomly select a parcel and a job category
    #                     selected_parcel = random.choice(parcel_ids)
    #                     selected_parcel_df = parcel_data_kirkland[(parcel_data_kirkland['PARCELID']==selected_parcel)][job_scaled_columns]
    #                     job_pool = selected_parcel_df.columns[(selected_parcel_df > 0).all()].tolist()
    #                     selected_job = random.choice(job_pool)
    #                     # add one more job on this job category in this selected parcel
    #                     parcel_data_kirkland.loc[parcel_data_kirkland['PARCELID']==selected_parcel, selected_job] += 1
    #                     parcel_data_kirkland['EMPTOT_P_SCALED'] = parcel_data_kirkland[job_scaled_columns].sum(axis=1)
    #             else:
    #                 # need to reduce the number of jobs in those parcels that already have jobs
    #                 for _ in range(int(abs(difference))):
    #                     parcels_in_tmtaz_df = parcel_data_kirkland.loc[(parcel_data_kirkland['BKRTMTAZ'] == tmtaz) & \
    #                                                                    (parcel_data_kirkland['EMPTOT_P_SCALED'] > 0)]
    #                     parcel_ids = list(parcels_in_tmtaz_df['PARCELID'])
    #                     # randomly select a parcel and a job category
    #                     selected_parcel = random.choice(parcel_ids)
    #                     selected_parcel_df = parcel_data_kirkland[(parcel_data_kirkland['PARCELID']==selected_parcel)][job_scaled_columns]
    #                     job_pool = selected_parcel_df.columns[(selected_parcel_df > 0).all()].tolist()
    #                     selected_job = random.choice(job_pool)
    #                     # add one more job on this job category in this selected parcel
    #                     parcel_data_kirkland.loc[parcel_data_kirkland['PARCELID']==selected_parcel, selected_job] -= 1
    #                     parcel_data_kirkland['EMPTOT_P_SCALED'] = parcel_data_kirkland[job_scaled_columns].sum(axis=1)
    #         # compare the final scaled job number with the control
    #         logging.info('Finetuning jobs in Kikrland complete! Comparing them again...')
    #         parcel_data_kirkland_scaled = parcel_data_kirkland.groupby(by='BKRTMTAZ').sum()['EMPTOT_P_SCALED'].reset_index()
    #         parcel_data_kirkland_scaled = parcel_data_kirkland_scaled.merge(parcel_data_kirkland_tmtaz[['Control', 'BKRTMTAZ']], how='left')
    #         parcel_data_kirkland_scaled['difference'] = parcel_data_kirkland_scaled['EMPTOT_P_SCALED'] - parcel_data_kirkland_scaled['Control']
    #         logging.info(f"New Kirkland parcel data file has {parcel_data_kirkland_scaled['Control'].sum():,.0f} jobs.")
    #         logging.info(f"Finetuned scaled Kirkland parcel data file has {parcel_data_kirkland_scaled['EMPTOT_P_SCALED'].sum():,.0f} jobs.")
    #         logging.info(f"(Finetuned and scaled Kirkland job total - New Kirkland job total) = {parcel_data_kirkland['EMPTOT_P_SCALED'].sum() - new_kirkland_parcel_data['Control'].sum()}")
    #         parcel_data_kirkland_scaled.to_csv(os.path.join(working_folder_lu, updated_parcel_file_kirkland_name.split('.')[0] + f'_{modeller_initial}_{version}.csv'))
    #         logging.info(f"Exporting Kirkland parcel matching file to: {os.path.join(working_folder_lu, updated_parcel_file_kirkland_name.split('.')[0] + f'_{modeller_initial}_{version}.csv')}\n")
    #         # replace the old parcels in Kirkland with those processed parcels
    #         logging.info('Replacing the old parcels in Kirkland with the parcels processed with the new numebr of jobs.')
    #         parcel_data_kirkland.set_index('PARCELID', inplace=True)
    #         job_scaled_columns = [f'{i}_SCALED' for i in columns_list]
    #         for job_column in columns_list:
    #             updated_parcel_df.loc[updated_parcel_df.index.isin(parcel_data_kirkland.index), job_column] = parcel_data_kirkland[job_column + '_SCALED']

    #     # update the total jobs 
    #     updated_parcel_df['EMPTOT_P'] = 0
    #     for col in columns_list:
    #         if col != 'EMPTOT_P':
    #             updated_parcel_df['EMPTOT_P'] += updated_parcel_df[col]     

    #     if set_Jobs_to_Zeros_All_Bel_Parcels_Not_in_New_Parcel_Data_File == True:
    #         jobs_to_be_zeroed_out = updated_parcel_df.loc[updated_parcel_df.index.isin(missing_bellevue_parcels_df['PARCELID']), 'EMPTOT_P'].sum()
    #         updated_parcel_df.loc[updated_parcel_df.index.isin(missing_bellevue_parcels_df['PARCELID']), columns_list] = 0
    #         logging.info('-----------------------------------------')
    #         logging.warning('Some COB parcels are not provided in the ' + new_bellevue_parcel_data_file_name + '.')
    #         logging.warning('But they exist in ' + original_parcel_file_name + '.')
    #         logging.warning(f'Number of jobs in these parcels are now zeroed out: {jobs_to_be_zeroed_out:,.0f}\n')

    #     logging.info(f"Total jobs before change: {self.original_parcel_data_df['EMPTOT_P'].sum():,.0f}")
    #     logging.info(f"Total jobs after change: {updated_parcel_df['EMPTOT_P'].sum():,.0f}\n")

    #     logging.info('Exporting parcel file(s)...')
    #     updated_parcel_df.to_csv(os.path.join(working_folder_lu, updated_parcel_file_name), sep = ' ')
    #     logging.info(f'Updated parcel file is exported in {os.path.join(working_folder_lu, updated_parcel_file_name)}.')

    #     logging.info('Backing up the scripts for step 4...')
    #     os.makedirs(os.path.join(working_folder_lu, self.backup_folder, version), exist_ok=True)
    #     utility.backupScripts(__file__, os.path.join(working_folder_lu, self.backup_folder, version, os.path.basename(__file__)))
    #     logging.info(f'Scripts for step 4 backup exported: {os.path.join(working_folder_lu, self.backup_folder, version, os.path.basename(__file__))}')
    #     logging.info('Step 4 done. Parcel files are updated with required columns.\n')


    # def step_5_sync_pop2parcels(self):
    #     """
    #     Step 5: Sync population to parcels.
    #         This program is used to pass number of households by parcel from synthetic population to parcel file. After the program,
    #         the households in parcel file is consistent with synthetic population file.
    #     """

    #     # 3/9/2022
    #     # upgraded to python 3.7

    #     # 05/01/2025
    #     # move the paths into config.py
        
    #     logging.info('\nLoading hh_and_persons.h5...')
    #     hdf_file = h5py.File(os.path.join(working_folder_synpop, h5_file_name), "r")
    #     hh_df = utility.h5_to_df(hdf_file, 'Household')

    #     logging.info("Updating number of households using the synthetic population's households...")
    #     hhs = hh_df.groupby('hhparcel')[['hhexpfac', 'hhsize']].sum().reset_index()
    #     parcel_df = pd.read_csv(os.path.join(working_folder_lu, updated_parcel_file_name), sep = ' ')
    #     parcel_df = parcel_df.merge(hhs, how = 'left', left_on = 'PARCELID', right_on = 'hhparcel')

    #     parcel_df['HH_P']  = 0
    #     parcel_df['HH_P'] = parcel_df['hhexpfac']
    #     parcel_df.fillna(0, inplace = True)
    #     parcel_df.drop(['hhexpfac', 'hhsize', 'hhparcel'], axis = 1, inplace = True)
    #     parcel_df['HH_P'] = parcel_df['HH_P'].round(0).astype(int)

    #     logging.info('\nExporting future parcel file...')
    #     parcel_df.to_csv(os.path.join(working_folder_lu, output_parcel_file), sep = ' ', index = False)
    #     logging.info(f'Future parcel file is exported in {os.path.join(working_folder_lu, output_parcel_file)}...')

    #     logging.info('Backing up the scripts for step 5...')
    #     os.makedirs(os.path.join(working_folder_lu, self.backup_folder, version), exist_ok=True)
    #     utility.backupScripts(__file__, os.path.join(working_folder_lu, self.backup_folder, version, os.path.basename(__file__)))
    #     logging.info(f'Scripts for step 5 backup exported: {os.path.join(working_folder_lu, self.backup_folder, version, os.path.basename(__file__))}')
    #     logging.info('Step 5 done. Synchronizing the synthetic population to parcel file is completed\n')
    #     logging.info('Land use process is complete. Please check the output numbers.\n')