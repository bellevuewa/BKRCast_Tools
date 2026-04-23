import os, sys
import pandas as pd

sys.path.append(os.getcwd())
import utility

### Inputs
working_folder = r'Z:\Modeling Group\BKRCast\LandUse\2030_DevReview_156th_corridor_Study'
base_jobs_file = '2024_Bellevue_jobs.csv'
base_sqft_file = '2024_Bellevue_sqft.csv'
base_housing_file = '2024_COB_housingunits_oa_v0.0.csv'

jobs_growth_file = '2030_COB_approved_jobs.csv'
housing_growth_file = '2030_COB_approved_housingunits.csv'
sqft_growth_file = '2030_COB_approved_sqft.csv'

new_jobs_file = '2030_COB_jobs.csv'
new_housing_file = '2030_COB_housingunits.csv'
new_sqft_file = '2030_COB_sqft.csv'
### End of inputs

print('Loading base files...')
base_jobs_df = pd.read_csv(os.path.join(working_folder, base_jobs_file))
base_sqft_df = pd.read_csv(os.path.join(working_folder, base_sqft_file))
base_housing_df = pd.read_csv(os.path.join(working_folder, base_housing_file))
print('Loading growth files...')
jobs_growth_df = pd.read_csv(os.path.join(working_folder, jobs_growth_file))
sqft_growth_df = pd.read_csv(os.path.join(working_folder, sqft_growth_file))
housing_growth_df = pd.read_csv(os.path.join(working_folder, housing_growth_file))

print('Processing and summarizing datasets...')


def merge_and_summarize(base_df, growth_df, key, numeric_cols, out_file, summary_out_file=None, label=None):
	"""Concatenate base+growth, aggregate by `key`.

	numeric_cols: list of numeric columns to SUM. Other columns are aggregated with 'first'.
	Writes merged dataframe to `out_file` and per-key numeric summary to `summary_out_file` if provided.
	Prints concise before/after summaries and top changes.
	Returns merged_df.
	"""
	lbl = f" ({label})" if label else ''

	# ensure summary_out_file
	if summary_out_file is None:
		summary_out_file = out_file.replace('.csv', '_summary.csv')

	# determine which numeric columns actually exist in inputs
	union_cols = set(base_df.columns.tolist()) | set(growth_df.columns.tolist())
	numeric_present = [c for c in numeric_cols if c in union_cols]

	# before summaries
	base_rows = len(base_df)
	growth_rows = len(growth_df)
	base_unique = base_df[key].nunique() if key in base_df.columns else 0
	growth_unique = growth_df[key].nunique() if key in growth_df.columns else 0

	base_totals = base_df[numeric_present].sum() if numeric_present else pd.Series(dtype='float64')
	growth_totals = growth_df[numeric_present].sum() if numeric_present else pd.Series(dtype='float64')

	print(f'Before merge{lbl}:')
	print(f' - base rows: {base_rows}, unique {key}: {base_unique}')
	print(f' - growth rows: {growth_rows}, unique {key}: {growth_unique}')
	if len(numeric_present) > 0:
		for c in numeric_present:
			print(f" - {c}: base={base_totals.get(c,0):,.0f}, growth={growth_totals.get(c,0):,.0f}")

	if key in base_df.columns and key in growth_df.columns:
		concat_df = pd.concat([base_df, growth_df], ignore_index=True)

		other_cols = [c for c in concat_df.columns if c not in numeric_present + [key]]
		agg_map = {c: 'sum' for c in numeric_present}
		agg_map.update({c: 'first' for c in other_cols})

		merged_df = concat_df.groupby(key, as_index=False).agg(agg_map)

		# compute base/growth aggregates by key for context
		base_by = base_df.groupby(key, as_index=True)[numeric_present].sum() if key in base_df.columns else pd.DataFrame(columns=numeric_present)
		growth_by = growth_df.groupby(key, as_index=True)[numeric_present].sum() if key in growth_df.columns else pd.DataFrame(columns=numeric_present)

		# Export parcels that would be clipped (pre-clip negatives) for review
		if len(numeric_present) > 0:
			neg_mask = (merged_df[numeric_present] < 0).any(axis=1)
			if neg_mask.any():
				clipped_rows = merged_df.loc[neg_mask, [key] + numeric_present].copy()
				# attach base and growth side info for context if available
				for c in numeric_present:
					clipped_rows[f'base_{c}'] = base_by.reindex(clipped_rows[key])[c].values
					clipped_rows[f'growth_{c}'] = growth_by.reindex(clipped_rows[key])[c].values
				clipped_out = os.path.join(working_folder, out_file.replace('.csv', '_clipped_parcels.csv'))
				clipped_rows.to_csv(clipped_out, index=False)
				print(f'Parcels with negative merged values written to: {clipped_out} ({len(clipped_rows)} rows)')

			# Now clip merged numeric values to >= 0
			for c in numeric_present:
				if c in merged_df.columns:
					merged_df[c] = merged_df[c].clip(lower=0)

		merged_rows = len(merged_df)
		merged_unique = merged_df[key].nunique()
		merged_totals = merged_df[numeric_present].sum() if numeric_present else pd.Series(dtype='float64')

		print(f'After merge{lbl}:')
		print(f' - merged rows: {merged_rows}, unique {key}: {merged_unique}')
		if len(numeric_present) > 0:
			for c in numeric_present:
				print(f" - {c}: merged={merged_totals.get(c,0):,.0f} (change={merged_totals.get(c,0)-base_totals.get(c,0):,.0f})")

		base_by = base_df.groupby(key, as_index=True)[numeric_present].sum() if key in base_df.columns else pd.DataFrame(columns=numeric_present)
		growth_by = growth_df.groupby(key, as_index=True)[numeric_present].sum() if key in growth_df.columns else pd.DataFrame(columns=numeric_present)
		merged_by = merged_df.set_index(key)[numeric_present]

		idx = merged_by.index.union(base_by.index).union(growth_by.index)
		base_by = base_by.reindex(idx).fillna(0)
		growth_by = growth_by.reindex(idx).fillna(0)
		merged_by = merged_by.reindex(idx).fillna(0)

		delta_by = merged_by - base_by
		delta_by['abs_total_change'] = delta_by.abs().sum(axis=1)
		top_changes = delta_by.sort_values('abs_total_change', ascending=False).head(10).drop(columns=['abs_total_change'])

		print(f'\nTop {key} changes for{lbl} (numeric columns):')
		if not top_changes.empty:
			print(top_changes.head(10).to_string())
		else:
			print(' (none)')

		# write summary
		summary_df = pd.DataFrame(index=idx)
		for c in numeric_present:
			summary_df[f'base_{c}'] = base_by[c]
			summary_df[f'growth_{c}'] = growth_by[c]
			summary_df[f'merged_{c}'] = merged_by[c]
			summary_df[f'delta_{c}'] = merged_by[c] - base_by[c]

		summary_df.reset_index().rename(columns={'index': key}).to_csv(os.path.join(working_folder, summary_out_file), index=False)
		print(f'Per-{key} numeric summary written to: {summary_out_file}')
	else:
		merged_df = pd.concat([base_df, growth_df], ignore_index=True)
		print(f'PSRC_ID not present in both inputs for{lbl} — performed simple concat.')

	# write merged
	out_path = os.path.join(working_folder, out_file)
	print(f'Writing merged{lbl} to: {out_path} ({len(merged_df)} rows)')
	merged_df.to_csv(out_path, index=False)
	return merged_df


# Now call the function for jobs, sqft, and housing
jobs_numeric = [
	'EMPEDU_P','EMPFOO_P','EMPGOV_P','EMPIND_P','EMPMED_P','EMPOFC_P',
	'EMPOTH_P','EMPRET_P','EMPRSC_P','EMPSVC_P','EMPTOT_P'
]
key = 'PSRC_ID'
merged_jobs_df = merge_and_summarize(base_jobs_df, jobs_growth_df, key, jobs_numeric, new_jobs_file, label='jobs')

# sqft specific columns
sqft_numeric = ['SQFT_EDU','SQFT_FOO','SQFT_GOV','SQFT_IND','SQFT_MED','SQFT_OFC','SQFT_RET','SQFT_RSV','SQFT_SVC','SQFT_OTH','SQFT_TOT']
merged_sqft_df = merge_and_summarize(base_sqft_df, sqft_growth_df, key, sqft_numeric, new_sqft_file, label='sqft')

# housing specific columns
housing_numeric = ['SFUnits', 'MFUnits']
merged_housing_df = merge_and_summarize(base_housing_df, housing_growth_df, key, housing_numeric, new_housing_file, label='housing')