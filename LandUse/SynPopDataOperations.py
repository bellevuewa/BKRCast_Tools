import pandas as pd
import numpy as np
from utility import Data_Scale_Method, Job_Categories, Parcel_Data_Format, dialog_level, IndentAdapter
import logging, os, sys
from synthetic_population import SyntheticPopulation
 
class SynPopDataOperations:
    def __init__(self, synpop: SyntheticPopulation, scen_name, output_dir: str, hhs_assumptions, indent):
        self.synpop = synpop # base synpop on which all the operations wwill be conducted.
        self.output_dir = output_dir
        self.indent = indent
        self.subarea_df = synpop.subarea_df
        self.lookup_df = synpop.lookup_df
        self.hhs_assumptions = hhs_assumptions
        self.scen_name = scen_name

        base_logger = logging.getLogger(__name__)
        self.logger = IndentAdapter(base_logger, indent)

        base_synpop_summary = self.synpop.summarize_synpop(self.output_dir, self.scen_name)
        self.updated_hhs_by_parcels_df = base_synpop_summary['summary_by_parcel'].copy()
        self.updated_hhs_by_parcels_df = self.updated_hhs_by_parcels_df.rename(columns = {'total_hhs_by_parcel': 'adj_hhs_by_parcel', 'total_persons_by_parcel':'adj_persons_by_parcel'})        

    def generate_total_hhs_data_for_jurisdiction(self, process_rule):
        self.logger.info(f"processing rule: {process_rule}")
        updated_parcel_dict = {}
        if process_rule['Data Format'] == Parcel_Data_Format.Processed_Parcel_Data.value:
            if process_rule['Scale Method'] == Data_Scale_Method.Keep_the_Data_from_the_Partner_City.value:
                # only need to overwrite parcels in base_parcel_df with parcel data from process_rule['File]
                updated_parcel_dict = self.replace_hhs_data_using_local_jurisdiction_estimate(process_rule['Jurisdiction'], True, process_rule['File'])
            elif process_rule['Scale Method'] == Data_Scale_Method.Scale_by_Job_Category.value:
                updated_parcel_dict = self.scale_by_job_category(process_rule['Jurisdiction'], True, process_rule['File'])
            elif process_rule['Scale Method'] == Data_Scale_Method.Scale_by_Total_Jobs_by_TAZ.value:
                updated_parcel_dict = self.scale_selected_base_data_by_total_jobs_by_TAZ(process_rule['Jurisdiction'], True, process_rule['File'])
            else:
                raise Exception(f'invalid scale method {process_rule["Scale Method"]}')
        elif process_rule['Data Format'] == Parcel_Data_Format.BKR_Trip_Model_TAZ_Forma.value:
            if process_rule['Scale Method'] == Data_Scale_Method.Scale_by_Total_Jobs_by_TAZ.value:
                updated_parcel_dict = self.scale_selected_base_data_by_total_jobs_by_TAZ(process_rule['Jurisdiction'], process_rule['File'], 'BKRTMTAZ', 'ControlTotalJobs')
        

        return updated_parcel_dict
    
    def replace_hhs_data_using_local_jurisdiction_estimate(self, jurisdiction, set_juris_base_hhs_to_zero, local_housing_unit_data_file) -> dict:
        # Replace hhs estimate with COB's forecast
        # if some parcels are missing from the cob_du_df, export them for further investigation.
        adjusted_hhs_by_parcel_df = self.updated_hhs_by_parcels_df.copy()

        local_du_df = pd.read_csv(os.path.join(self.output_dir, local_housing_unit_data_file))
        adjusted_hhs_by_parcel_df = adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['Jurisdiction'] == jurisdiction.upper()]
        local_parcels_provided = local_du_df.shape[0]
        if adjusted_hhs_by_parcel_df.shape[0] != local_parcels_provided:
            self.logger.info('COB forecast does not cover all parcels. Please cehck the missing parcel files for further investigation.')
            cob_missing_parcels_df = adjusted_hhs_by_parcel_df.loc[~adjusted_hhs_by_parcel_df['PSRC_ID'].isin(local_du_df['PSRC_ID'])]
            cob_missing_parcels_df.to_csv(os.path.join(self.output_dir, 'cob_missing_parcels.csv'), index = False)
            self.logger.info(f'{cob_missing_parcels_df.shape[0]} parcels are missing in {local_housing_unit_data_file}.')
        
        local_du_df['sfhhs'] = local_du_df['SFUnits'] * self.hhs_assumptions[jurisdiction]["sfhh_occ"] 
        local_du_df['mfhhs'] = local_du_df['MFUnits'] * self.hhs_assumptions[jurisdiction]["mfhh_occ"]
        local_du_df['sfpersons'] = local_du_df['sfhhs'] * self.hhs_assumptions[jurisdiction]["sfhhsize"]
        local_du_df['mfpersons'] = local_du_df['mfhhs'] * self.hhs_assumptions[jurisdiction]["mfhhsize"]
        local_du_df['source'] = 'local_parcel'

        adjusted_hhs_by_parcel_df = adjusted_hhs_by_parcel_df.merge(local_du_df[['PSRC_ID', 'source', 'sfhhs', 'mfhhs', 'sfpersons', 'mfpersons']], on = 'PSRC_ID', how = 'left')
        # reset hhs and persons in all COB parcels to zero. Only use local forecast.
        adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['Jurisdiction'] == jurisdiction.upper(), ['adj_hhs_by_parcel', 'adj_persons_by_parcel']] = 0

        # it is importand to use cobflag rather than Jurisdiction, because (hhs and persons in) parcels flagged by cobflag are provided by COB staff.
        adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['source'] == 'local_parcel', 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['sfhhs'] + adjusted_hhs_by_parcel_df['mfhhs']
        adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['source'] == 'local_parcel', 'adj_persons_by_parcel'] = adjusted_hhs_by_parcel_df['sfpersons'] + adjusted_hhs_by_parcel_df['mfpersons']

        selection = self.updated_hhs_by_parcels_df['PSRC_ID'].isin(adjusted_hhs_by_parcel_df['PSRC_ID'])
        b4_change_df = self.updated_hhs_by_parcels_df.loc[selection, ['adj_hhs_by_parcel','adj_persons_by_parcel']].copy()
        self.logger.info(f'before synthetic population update, total hhs in {jurisdiction}: {b4_change_df.sum().to_dict()}')
        
        # must set index first before using update
        self.updated_hhs_by_parcels_df.set_index('PSRC_ID', inplace = True)
        self.updated_hhs_by_parcels_df.update(adjusted_hhs_by_parcel_df.set_index('PSRC_ID')[['adj_hhs_by_parcel','adj_persons_by_parcel']])
        
        self.updated_hhs_by_parcels_df.reset_index(inplace = True)       
        self.logger.info(f"after synthetic population update, total hhs in {jurisdiction}: {self.updated_hhs_by_parcels_df.loc[selection, ['adj_hhs_by_parcel','adj_persons_by_parcel']].sum()}")
        self.logger.info(f"Inputs from {jurisdiction}'s local input: {local_du_df[['SFUnits', 'MFUnits']].sum().to_dict()}")
        df_dict = {
            "data_frame": self.updated_hhs_by_parcels_df,
            'local_data': local_du_df,
            'before_change': b4_change_df,
            "local_data_provider": jurisdiction
        }

        return df_dict
    
    def export_popsim_control_file(self, control_template_name, popsim_control_file):
        ### Create control file for PopulationSim
        popsim_control_df = pd.read_csv(os.path.join(self.output_dir, control_template_name), sep = ',')
        hhs_by_geoid10_df =  self.updated_hhs_by_parcels_df[['GEOID10', 'adj_hhs_by_parcel']].groupby('GEOID10').sum()
        adj_persons_by_GEOID10 = self.updated_hhs_by_parcels_df[['GEOID10', 'adj_persons_by_parcel']].groupby('GEOID10').sum()
        hhs_by_geoid10_df = hhs_by_geoid10_df.merge(adj_persons_by_GEOID10, left_index = True, right_index = True, how = 'left')
        hhs_by_geoid10_df.fillna(0, inplace = True)
        popsim_control_df = popsim_control_df.merge(hhs_by_geoid10_df, left_on = 'block_group_id', right_on = 'GEOID10', how = 'left')
        error_blkgrps_df = popsim_control_df.loc[popsim_control_df.isna().any(axis = 1)]
        if error_blkgrps_df.shape[0] > 0:
            self.logger.info('Some blockgroups are missing values. Please check the error_census_blockgroup.csv')
            self.logger.info('The missing values are all replaced with zeros.')
            error_blkgrps_df.to_csv(os.path.join(self.output_dir, 'error_census_blockgroup.csv'), index = False)

        popsim_control_df.fillna(0, inplace = True)
        popsim_control_df['hh_bg_weight'] = popsim_control_df['adj_hhs_by_parcel'].round(0).astype(int)
        popsim_control_df['hh_tract_weight'] = popsim_control_df['adj_hhs_by_parcel'].round(0).astype(int)
        popsim_control_df['pers_bg_weight'] = popsim_control_df['adj_persons_by_parcel'].round(0).astype(int)
        popsim_control_df['pers_tract_weight'] = popsim_control_df['adj_persons_by_parcel'].round(0).astype(int)
        popsim_control_df.drop(hhs_by_geoid10_df.columns, axis = 1, inplace = True)
        popsim_control_df.to_csv(os.path.join(self.output_dir, popsim_control_file), index = False)

        total_hhs = popsim_control_df['hh_bg_weight'].sum()
        total_persons = popsim_control_df['pers_bg_weight'].sum()
        self.logger.info(f'{total_hhs} households, {total_persons} persons are in the control file.')

