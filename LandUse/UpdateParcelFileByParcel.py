### This tool is used to produce parcel files for BKRCast.
### It takes sqft by land use category and conversion rate as input files, and converts sqft to number of
### jobs. Additional adjustment based on subarea factors and TAZ factors is available to further fine tune the converted jobs.
## 
## Home based jobs is identified as zero commercial sqft with non zero jobs in ESD.

## Two extra features added 8/26/2019
## 1. add one switch remove home based jobs (BKR area, as long as sqft data are provided) from parcel file. 
## 2. add a parking cost adjustment factor for Bellevue only. Set it to one if no adjustment is needed.

## 9/27/2019
## add TAZ level adjustment factor. Only applied for Bellevue Square. But can extend to any TAZ.
##
## 3/31/2020
## backup this script to the output folder. So in the future user will know what script was used to generate
## this set of output.

import os, sys
sys.path.append(os.getcwd())
import pandas as pd
import numpy as np
import re
from pyproj import Proj, transform
import copy
from shutil import copyfile
import h5py
import utility

Common_Data_Folder = r'Z:\Modeling Group\BKRCast\CommonData'
working_folder = r'Z:\Modeling Group\BKRCast\LandUse\Concurrency\2020Concurrency'
Output_Parcel_Folder = r'Z:\Modeling Group\BKRCast\LandUse\Concurrency\2020Concurrency'

# Base year original ESD parcel file name can be an absolute name or relative name. If it is relative, will open from Common data folder. 
# this can be true original 2014, or introplated ESD dataset between 2014 and 2035. 
Original_ESD_Parcel_File_Name = r'Z:\Modeling Group\BKRCast\LandUse\2019Baseyear\interpolated_parcel_file_2019.txt'

Conversion_Factors_File_Name = r"BKRCast_Conversion_rate_2020.csv"
Subarea_Adjustment_Factor_File_Name = r"subarea_adjustment_factor-4-7-2020.csv"
Parcels_Sqft_File_Name = r"2019_king_county_lu_sqft_by_PSRC_ID.csv"
TAZ_Subarea_File_Name = r"TAZ_subarea.csv"
Output_Parcel_File_Name = "parcels_urbansim.txt"
Hh_and_person_file = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2020Concurrency\2020_concurrency_hh_and_persons.h5'
TAZ_adjustment_file_name = r'TAZ_adjustment_factors-4-7-2020.csv'

# if the Parcel_Job_File_Name is available, use job numbers in this file to overwrite jobs in the generated parcel file.
# otherwise set this variable to None
Parcel_Job_File_Name = '2020concurrency_COB_Jobs.csv'

# set False if want to drop home based jobs
KEEP_HOME_BASED_JOBS = False

ONLY_USE_BELLEVUE_JOB_ESTIMATE_IN_COB_LIMIT = True

# further adjust parking cost in Bellevue. Set it to 1 if no adjustment is made.
BELLEVUE_PARKING_COST_ADJUSTMENT_FACTOR = 0.5

CONVERSION_LEVEL = ['verylow', 'low', 'med', 'high', 'veryhigh']
EMPLOYMENT_TYPE = ['EDU', 'FOO', 'GOV', 'IND', 'MED', 'OFC', 'OTH', 'RET', 'SVC', 'NoEMP']

#Parcel_FileName_newjobs:
#PARCELID EMPEDU_P EMPFOO_P EMPGOV_P EMPIND_P EMPMED_P EMPOFC_P EMPOTH_P EMPRET_P EMPRSC_P EMPSVC_P EMPTOT_P HH_P 
JOB_CATEGORY = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPOTH_P', 'EMPRET_P', 'EMPSVC_P', 'EMPTOT_P']

if not os.path.exists(Output_Parcel_Folder):
    os.makedirs(Output_Parcel_Folder)

print "Loading input files ..."
if os.path.isabs(Original_ESD_Parcel_File_Name) == False:
    Original_ESD_Parcel_File_Name = os.path.join(Common_Data_Folder, Original_ESD_Parcel_File_Name)
    
parcels = pd.read_csv(Original_ESD_Parcel_File_Name, sep = " ", index_col = "PARCELID")

original_ESD_tot_jobs = parcels['EMPTOT_P'].sum()
print 'Original ESD jobs {0:.0f}'.format(original_ESD_tot_jobs)
conversion_rates = pd.DataFrame.from_csv(os.path.join(Common_Data_Folder, Conversion_Factors_File_Name), sep = ",")
adjustment_factors = pd.read_csv(os.path.join(Common_Data_Folder, Subarea_Adjustment_Factor_File_Name), sep = ",", index_col = "Subarea_ID")
TAZ_adjustment_factors_df = pd.read_csv(os.path.join(Common_Data_Folder, TAZ_adjustment_file_name), sep = ',')
taz_subarea = pd.read_csv(os.path.join(Common_Data_Folder, TAZ_Subarea_File_Name), sep = ",", index_col = "TAZNUM")

parcels_sqft = pd.read_csv(os.path.join(working_folder, Parcels_Sqft_File_Name), sep = ",")   
parcels_sqft = parcels_sqft.join(parcels['TAZ_P'], on = 'PSRC_ID')
parcels_sqft = parcels_sqft.join(taz_subarea[['Subarea', 'SubareaName']], on = 'TAZ_P')
parcels_sqft = parcels_sqft.join(adjustment_factors['Factor'], on = "Subarea")
parcels_sqft = parcels_sqft[~parcels_sqft["Factor"].isnull()]
parcels_sqft = parcels_sqft.merge(TAZ_adjustment_factors_df, how = 'left', left_on = 'TAZ_P', right_on = 'BKRCastTAZ')
parcels_sqft['TAZFactor'].fillna(1, inplace = True)

#capitalize all column names to avoid unexpected errors
parcels.columns = [i.upper() for i in parcels.columns]
conversion_rates.columns = [i.upper() for i in conversion_rates.columns]
parcels_sqft.columns = [i.upper() for i in parcels_sqft.columns]

#
# Pull conversion rate by level.
# level: verylow, low, med, high, veryhigh
# return: dataframe of conversion_rate for a certail level
#
def GetConversionRates(level):
    categories = []
    level = "_" + level.upper()
    for col in conversion_rates.columns: 
        loc = col.find(level) 
        if  loc > 0:
            categories.append(col)

    return conversion_rates[categories]

#
# Convert sqft to number of jobs by job category. The conversion rates are controlled by density in each parcel
# empType: EDU, FOO, GOV, IND, NOEMP, MED, OFC, OTH, RET, SVC
def Sqft_to_Jobs(empType, convertLevel):
    list = []
    parcels_sqft['EMPTOT_P'] = 0
    for type in empType:
        if type == 'NoEMP':
            continue
        totJobType = "EMP" + type + "_P"  
        parcels_sqft[totJobType] = 0
  
        for level in convertLevel:
            type_level = type.upper() + "_" + level.upper()
            type_level_Job = type_level + "_J"
            if type_level in parcels_sqft.columns:
                jobrate = conversion_rates.loc['Sqft_to_Job_Rate', type_level]
                occrate = conversion_rates.loc['Occupied_Rate', type_level]
                type_level_Job = type_level + "_J"
                parcels_sqft[type_level_Job] = parcels_sqft[type_level] * occrate / jobrate * parcels_sqft['FACTOR'] * parcels_sqft['TAZFACTOR']
            else:
                parcels_sqft[type_level_Job] = 0
            parcels_sqft[totJobType] = parcels_sqft[totJobType] + parcels_sqft[type_level_Job]
        parcels_sqft['EMPTOT_P'] = parcels_sqft['EMPTOT_P'] + parcels_sqft[totJobType]
    parcels_sqft['NOEMP_J'] = parcels_sqft['NOEMP'] * conversion_rates.loc['Occupied_Rate', 'NOEMP'] / conversion_rates.loc['Sqft_to_Job_Rate', 'NOEMP']
    return

  
def Print_DF_Columns(df):
    if type(df) is pd.DataFrame:
        for col in df.columns:
            print col
    else:
        print "Input is not a pandas dataframe"


Sqft_to_Jobs(EMPLOYMENT_TYPE, CONVERSION_LEVEL)  
ESD_jobs = parcels[JOB_CATEGORY]
ESD_jobs = ESD_jobs.rename(columns = lambda x: x.replace('_P', '_ESD'))
parcels_sqft = parcels_sqft.join(ESD_jobs, on = "PSRC_ID")

print "Exporting \"Converted_jobs_in_BKRarea.csv\""
parcels_sqft.to_csv(os.path.join(Output_Parcel_Folder, "Converted_jobs_in_BKRarea.csv"))
print 'Total converted jobs (in BKR) from sqft are {0:.0f}'.format(parcels_sqft['EMPTOT_P'].sum())

# if there are ESD jobs in a parcel without commercial sqft, they are home based jobs. 
homeoffice_detailedparcels = parcels_sqft.loc[(parcels_sqft['EMPTOT_P'] == 0) & (parcels_sqft['EMPTOT_ESD'] > 0) ]
# export parcels only with home based jobs, and sqft related data
homeoffice_detailedparcels.to_csv(os.path.join(Output_Parcel_Folder, "HomeOfficeParcels_detailed.csv"))  
# export ESD parcel data with only home based jobs      
homeoffice_parcels = parcels.loc[homeoffice_detailedparcels['PSRC_ID']]
homeoffice_parcels.to_csv(os.path.join(Output_Parcel_Folder, "HomeOfficeParcels.csv"))

# Because the limitation of sqft method, need to put the home office back to the parcel data.
# for home based jobs, put them back to the converted job list. It is now a hybrid of ESD data and converted jobs.
parcels_sqft = parcels_sqft.set_index('PSRC_ID') 
if KEEP_HOME_BASED_JOBS:
    parcels_sqft.loc[parcels_sqft.index.isin(homeoffice_parcels.index), JOB_CATEGORY] = homeoffice_parcels[JOB_CATEGORY]
    print 'Home based jobs are put back. '
else: 
    print 'Home based jobs are NOT put back'

print 'total home based jobs (in BKR) are {0:.0f}'.format(homeoffice_parcels['EMPTOT_P'].sum())
print 'Total converted jobs in BKR (with home based jobs back) are {0:.0f}'.format(parcels_sqft['EMPTOT_P'].sum())
print 'Total jobs in BKR area in the original parcel fiel are ', parcels.loc[parcels.index.isin(parcels_sqft.index), 'EMPTOT_P'].sum()

# restore the index to the previous one.
parcels_sqft.reset_index(inplace = True)

newESDLabels = [x.replace('_P', '_ESD') for x in JOB_CATEGORY]
outputlist = JOB_CATEGORY + newESDLabels

summary_by_jurisdiction = parcels_sqft.groupby('JURISDICTION')[outputlist].sum()
print "Exporting \"converted_vs_ESD_summary_by_jurisdiction.csv\""
summary_by_jurisdiction.to_csv(os.path.join(Output_Parcel_Folder, "converted_vs_ESD_summary_by_jurisdiction.csv"))
summary_by_taz = parcels_sqft.groupby('TAZ_P')[outputlist].sum()
print "Exporting \"converted_vs_ESD_summary_by_TAZ.csv\""
summary_by_taz.to_csv(os.path.join(Output_Parcel_Folder, "converted_vs_ESD_summary_by_TAZ.csv")) 

#"SUBRAREA" is the index
summary_by_subarea = parcels_sqft.groupby('SUBAREA')[outputlist].sum()
subarea_def = parcels_sqft[["SUBAREA", "SUBAREANAME"]]
subarea_def = subarea_def.drop_duplicates(keep = 'first')  
subarea_def.set_index("SUBAREA", inplace = True)     
summary_by_subarea = summary_by_subarea.join(subarea_def["SUBAREANAME"])
print "Exporting \"converted_vs_ESD_summary_by_subarea.csv\""
summary_by_subarea.to_csv(os.path.join(Output_Parcel_Folder, "converted_vs_ESD_summary_by_subarea.csv"))

parcels.drop_duplicates(keep = 'first', inplace = True) 
parcels_sqft.drop_duplicates(subset = 'PSRC_ID', keep = 'first', inplace = True)
parcels_sqft.set_index('PSRC_ID', inplace = True) 

parcels.loc[parcels.index.isin(parcels_sqft.index), JOB_CATEGORY] = parcels_sqft[JOB_CATEGORY]
#print("Before home office update, total jobs are ", parcels['EMPTOT_P'].sum())
## for parcels with only home based jobs, put them back after sqft conversion. Otherwise, all home based jobs will be missing.
#parcels.loc[parcels.index.isin(homeoffice_parcels.index), JOB_CATEGORY] = homeoffice_parcels[JOB_CATEGORY]
#print("After home office update, total jobs are ", parcels['EMPTOT_P'].sum())

# BEllevue CBD Parking Cost process
parcels =  parcels.join(taz_subarea['Subarea'], on = 'TAZ_P')
parcels.loc[parcels['Subarea'] == 3, 'PPRICDYP']  = parcels['PPRICDYP'] * BELLEVUE_PARKING_COST_ADJUSTMENT_FACTOR
parcels.loc[parcels['Subarea'] == 3, 'PPRICHRP']  = parcels['PPRICHRP'] * BELLEVUE_PARKING_COST_ADJUSTMENT_FACTOR
parcels = parcels.drop(['Subarea'], axis = 1)  

# Overwrite job numbers with external inputs, if applicable
if Parcel_Job_File_Name != None:
    subarea_parcel_jobs_df = pd.read_csv(os.path.join(working_folder, Parcel_Job_File_Name), sep = ',')
    subarea_parcel_jobs_df.rename(columns = {'PSRC_ID' : 'PARCELID'}, inplace = True)
    subarea_parcel_jobs_df = subarea_parcel_jobs_df.groupby('PARCELID').sum().round(0).astype(int)
    print 'Total jobs in ', Parcel_Job_File_Name, ' is ', subarea_parcel_jobs_df['EMPTOT_P'].sum()
    updated_parcel_job_df = parcels.loc[subarea_parcel_jobs_df.index]
    updated_parcel_job_df.to_csv(os.path.join(working_folder, 'jobs_to_be_replaced.csv'))
    jobs_tobereplaced =  updated_parcel_job_df['EMPTOT_P'].sum().round(0).astype(int)
    print 'Total jobs to be replaced: ', jobs_tobereplaced
    fields = subarea_parcel_jobs_df.columns
    subarea_parcel_jobs_df.rename(columns = lambda col: col + '_new', inplace = True)
    updated_parcel_job_df = updated_parcel_job_df.merge(subarea_parcel_jobs_df, left_index = True, right_index = True, how = 'left')

    for emp in JOB_CATEGORY:
        updated_parcel_job_df[emp] = updated_parcel_job_df[emp + '_new'].round(0).astype(int)
        
    updated_parcel_job_df.drop(subarea_parcel_jobs_df.columns, axis = 1, inplace = True)
    print 'Total replacement jobs: ', updated_parcel_job_df['EMPTOT_P'].sum()
    print 'Net job changes due to job number overwritten: ', updated_parcel_job_df['EMPTOT_P'].sum() - jobs_tobereplaced
    bellevue1 = parcels.reset_index().merge(taz_subarea.reset_index()[['TAZNUM', 'Jurisdiction']], left_on = 'TAZ_P', right_on = 'TAZNUM', how = 'left')
    bellevue1 = bellevue1.loc[bellevue1['Jurisdiction'] == 'BELLEVUE']
    other_bellevue_parcels = bellevue1.loc[~bellevue1['PARCELID'].isin(updated_parcel_job_df.index)]
    parcels.loc[other_bellevue_parcels['PARCELID'], JOB_CATEGORY] = 0
    print 'Jobs in ', other_bellevue_parcels.shape[0], ' parcels in Bellevue are zero out'
    print 'Total removed jobs are: ', other_bellevue_parcels['EMPTOT_P'].sum()
    parcels = parcels.drop(updated_parcel_job_df.index)
    # parcels.loc[other_bellevue_parcels.index, [JOB_CATEGORY]] = 0
    parcels.reset_index(inplace = True)
    updated_parcel_job_df.reset_index(inplace = True)
    parcels = parcels.append(updated_parcel_job_df.reset_index())
    print 'Specified jobs are all imported.'

# synchronizing synthetic population and parcel file
print 'Synchronizing synthetic population'
print 'Loading hh_and_persons.h5...'
hdf_file = h5py.File(Hh_and_person_file, "r")
hh_df = utility.h5_to_df(hdf_file, 'Household')
hh_df.set_index('hhparcel', inplace = True)

print 'Updating number of households...'
hhs = hh_df.groupby('hhparcel')['hhexpfac', 'hhsize'].sum()
parcels = parcels.merge(hhs, left_on = 'PARCELID', right_index = True, how = 'left')

parcels['HH_P']  = parcels['hhexpfac']
parcels.fillna(0, inplace = True)
parcels.drop(['hhexpfac', 'hhsize'], axis = 1, inplace = True)

print "Exporting updated urbansim parcel file ..."
parcels.to_csv(os.path.join(Output_Parcel_Folder, Output_Parcel_File_Name), sep = ' ')
print 'Total jobs in updated parcel file are {0:.0f}'.format(parcels['EMPTOT_P'].sum())

# backup input files inside input folder
print "Backup input files ..."
input_backup_folder = os.path.join(Output_Parcel_Folder, 'inputs')
if not os.path.exists(input_backup_folder):
    os.makedirs(input_backup_folder) 


copyfile(Original_ESD_Parcel_File_Name, os.path.join(input_backup_folder, os.path.basename(Original_ESD_Parcel_File_Name)))
copyfile(os.path.join(Common_Data_Folder, Conversion_Factors_File_Name), os.path.join(input_backup_folder, Conversion_Factors_File_Name))
copyfile(os.path.join(Common_Data_Folder, Subarea_Adjustment_Factor_File_Name), os.path.join(input_backup_folder, Subarea_Adjustment_Factor_File_Name))
copyfile(os.path.join(Common_Data_Folder, TAZ_adjustment_file_name), os.path.join(input_backup_folder, TAZ_adjustment_file_name))
copyfile(os.path.join(working_folder, Parcels_Sqft_File_Name), os.path.join(input_backup_folder, Parcels_Sqft_File_Name))
copyfile(Hh_and_person_file, os.path.join(input_backup_folder, os.path.basename(Hh_and_person_file)))

utility.backupScripts(__file__, os.path.join(input_backup_folder, os.path.basename(__file__)))
print "Finished"  