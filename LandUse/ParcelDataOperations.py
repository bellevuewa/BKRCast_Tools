import pandas as pd
import numpy as np
from utility import Data_Scale_Method, Job_Categories, Parcel_Data_Format, Summary_Categories
import logging, os, sys
from Parcels import Parcels
 

class ParcelDataOperations:
    def __init__(self, base_parcels: Parcels, output_dir: str, output_filename: str):
        self.base_parcel = base_parcels
        self.subarea_df = base_parcels.subarea_df.copy()
        self.lookup_df = base_parcels.lookup_df.copy()
        self.output_dir = output_dir
        self.output_filename = os.path.join(output_dir, output_filename)
        self.updated_parcels_df = base_parcels.original_parcels_df.copy()
        self.logger = logging.getLogger()


    def export_updated_parcels(self, export_name: str = None) -> Parcels: 
        if self.updated_parcels_df is None:
            self.logger.error("Updated parcel dataframe is not available for export.")
            raise ValueError("Updated parcel dataframe is not available.")

        if export_name is not None:
            fn = os.path.join(self.output_dir, export_name)
        else:
            fn = self.output_filename
        self.updated_parcels_df.to_csv(fn, sep = ' ', index=False)
        out = Parcels.from_dataframe(self.updated_parcels_df, filename=fn, data_year=self.base_parcel.data_year, subarea_df=self.base_parcel.subarea_df, lookup_df=self.base_parcel.lookup_df)
        self.logger.info(f'Updated parcel data exported to: {fn}')
        return out
        
    def controlled_rounding(self, attr_name, control_total, index_attr_name):
        # find residential parcels within taz     
        updated_data_df = self.updated_parcels_df.loc[self.updated_parcels_df[attr_name] > 0].copy()
        total_rows = updated_data_df.shape[0]
        if total_rows != 0:
            already_assigned = updated_data_df[attr_name].sum()
        else:
            already_assigned = 0
        
        # how many need to be assigned or removed to match the control total
        diff = int(control_total - already_assigned)
        if (diff == 0) | total_rows == 0:
            return self.updated_parcels_df

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
        
        new_data_df = self.updated_parcels_df.copy()
        new_data_df = new_data_df.loc[~new_data_df[index_attr_name].isin(updated_data_df[index_attr_name])]
        new_data_df = pd.concat([new_data_df, updated_data_df])
        
        return new_data_df

    def generate_employment_data_for_jurisiction(self, process_rule):
        # process_rule: dict 'Jurisdiction', 'File', 'Data_Format', 'Scale_Method'
        updated_parcel_dict = {}
        if process_rule['Data Format'] == Parcel_Data_Format.Processed_Parcel_Data.value:
            if process_rule['Scale Method'] == Data_Scale_Method.Keep_the_Data_from_the_Partner_City.value:
                # only need to overwrite parcels in base_parcel_df with parcel data from process_rule['File]
                updated_parcel_dict = self.replace_selected_base_data_from_with_local_jurisdiction(process_rule['Jurisdiction'], True, process_rule['File'])

        return updated_parcel_dict

    def replace_selected_base_data_from_with_local_jurisdiction(self, jurisdiction, set_juris_base_jobs_to_zero, local_parcel_data_file) -> dict:
        jobs_cat = Job_Categories.copy()
        jobs_cat.append('EMPTOT_P')

        updated_parcels_df = self.updated_parcels_df.copy()
        local_data_df = pd.read_csv(local_parcel_data_file, low_memory=False)

        # keep only required columns
        required_cols = ['PSRC_ID'] + jobs_cat
        local_data_df = local_data_df[required_cols]

        full_juris_parcels_df = self.lookup_df.loc[self.lookup_df['Jurisdiction'] == jurisdiction.upper()]  #  a complete list of parcels in Jurisdiction
        actual_juris_parcels_df = local_data_df.loc[local_data_df['PSRC_ID'].isin(full_juris_parcels_df['PSRC_ID'])] # parcels included in local job file
        not_in_full_juris_parcels = actual_juris_parcels_df.loc[~actual_juris_parcels_df['PSRC_ID'].isin(full_juris_parcels_df['PSRC_ID'])] # parcels in local job file but not in the complete list
        missing_juris_parcels_df = updated_parcels_df.loc[updated_parcels_df['PARCELID'].isin(full_juris_parcels_df.loc[~full_juris_parcels_df['PSRC_ID'].isin(local_data_df['PSRC_ID']), 'PSRC_ID'])]
        missing_juris_parcels_df.to_csv(os.path.join(self.output_dir, f'missing_{jurisdiction}_parcels.csv'), sep = ',', index = False)
        not_in_full_juris_parcels.to_csv(os.path.join(self.output_dir, f'not_valid_{jurisdiction}_parcels.csv'), sep = ',', index = False)


        if set_juris_base_jobs_to_zero:
            # find parcels in base parcel data that are not in local data, set jobs to zero
            jobs_to_be_zeroed_out = updated_parcels_df.loc[updated_parcels_df.index.isin(missing_juris_parcels_df['PARCELID']), 'EMPTOT_P'].sum()
            updated_parcels_df.loc[updated_parcels_df.index.isin(missing_juris_parcels_df['PARCELID']), Job_Categories] = 0
        # index by parcel id for alignment
        updated_parcels_df = updated_parcels_df.set_index('PARCELID')
        local_data_df = local_data_df.set_index('PSRC_ID')

        # only update rows that exist in both
        common_ids = updated_parcels_df.index.intersection(local_data_df.index)
        b4_change_df = updated_parcels_df.loc[common_ids, jobs_cat]

        # replace values
        updated_parcels_df.loc[common_ids, jobs_cat] = local_data_df.loc[common_ids, jobs_cat]
        
        # calculate total jobs after change
        updated_parcels_df.fillna(0, inplace=True)
        updated_parcels_df['EMPTOT_P'] = updated_parcels_df[Job_Categories].sum(axis=1)

        df_dict = {
            "data_frame": updated_parcels_df.reset_index(),
            'local_data': local_data_df,
            'before_change': b4_change_df,
            "local_data_provider": jurisdiction
        }

        self.updated_parcels_df = updated_parcels_df.reset_index().copy()
        # restore index
        return df_dict 