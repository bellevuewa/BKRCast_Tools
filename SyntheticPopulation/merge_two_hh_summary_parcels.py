import os, sys
sys.path.append(os.getcwd())
import pandas as pd
import numpy as np

work_folder = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2044_long_term_plan"
base_hh_summary_parcel_filename = "2044COB_complan_hh_summary_by_parcel.csv"
subset_hh_summary_parcel_filename = "2044_Kirkland_only_hh_summary_by_parcel.csv"
output_filename = "2044_COB_COK_PSRC_hh_summary_by_parcel.csv"

base_parcel_df = pd.read_csv(os.path.join(work_folder, base_hh_summary_parcel_filename))
subset_parcel_df = pd.read_csv(os.path.join(work_folder, subset_hh_summary_parcel_filename))

# Remove parcels from base that are in subset, then append subset values
merged_df = base_parcel_df[~base_parcel_df['PSRC_ID'].isin(subset_parcel_df['PSRC_ID'])].copy()
merged_df = pd.concat([merged_df, subset_parcel_df], ignore_index=True)

# Export merged result

merged_df.to_csv(os.path.join(work_folder, output_filename), index=False)

merged_df.groupby('Jurisdiction').agg({'total_hhs_by_parcel':'sum', 'total_persons_by_parcel':'sum'}).to_csv(os.path.join(work_folder, "hh_summary_by_jurisdiction_after_merge.csv"), index=True)
print(f"Exported to {output_filename}")



