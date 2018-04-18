### This tool is used to produce parcel files for BKRCast.
### It takes sqft by land use category and conversion rate as input files, and converts sqft to number of
### jobs. Additional adjustment based on subarea factors is available to further fine tune the converted jobs.

import pandana as pdna
import os, sys
sys.path.append(os.getcwd())
import pandas as pd
import numpy as np
import re
from pyproj import Proj, transform
import copy
from shutil import copyfile

Original_Parcel_Folder = r"Z:\Modeling Group\BKRCast\Job Conversion Test"
Original_ESD_Parcel_File_Name = r"parcels_urbansim.txt"
Conversion_Factors_File_Name = r"BKRCast_Conversion_rate_02132018.csv"
Subarea_Adjustment_Factor_File_Name = r"subarea_adjustment_factor-02202018-6.csv"
Output_Parcel_Folder = r"Z:\Modeling Group\BKRCast\Job Conversion Test\parcel_level\test9"
Parcels_Sqft_File_Name = r"ParcelSummary_Revised_GrpPSRCIDs-modified_Final.csv"
TAZ_Subarea_File_Name = r"TAZ_subarea.csv"
Output_Parcel_File_Name = "parcels_urbansim_Updated.txt"

CONVERSION_LEVEL = ['verylow', 'low', 'med', 'high', 'veryhigh']
EMPLOYMENT_TYPE = ['EDU', 'FOO', 'GOV', 'IND', 'MED', 'OFC', 'OTH', 'RET', 'SVC', 'NoEMP']

#Parcel_FileName_newjobs:
#PARCELID EMPEDU_P EMPFOO_P EMPGOV_P EMPIND_P EMPMED_P EMPOFC_P EMPOTH_P EMPRET_P EMPRSC_P EMPSVC_P EMPTOT_P HH_P 
JOB_CATEGORY = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPOTH_P', 'EMPRET_P', 'EMPSVC_P', 'EMPTOT_P']

if not os.path.exists(Output_Parcel_Folder):
    os.makedirs(Output_Parcel_Folder)

print "Loading input files ..."
parcels = pd.DataFrame.from_csv(os.path.join(Original_Parcel_Folder, Original_ESD_Parcel_File_Name), sep = " ", index_col = "PARCELID")
original_ESD_tot_jobs = parcels['EMPTOT_P'].sum()
print 'Original ESD jobs {0:.0f}'.format(original_ESD_tot_jobs)
conversion_rates = pd.DataFrame.from_csv(os.path.join(Original_Parcel_Folder, Conversion_Factors_File_Name), sep = ",")
adjustment_factors = pd.DataFrame.from_csv(os.path.join(Original_Parcel_Folder, Subarea_Adjustment_Factor_File_Name), sep = ",", index_col = "Subarea_ID")
taz_subarea = pd.DataFrame.from_csv(os.path.join(Original_Parcel_Folder, TAZ_Subarea_File_Name), sep = ",", index_col = "TAZNUM")

parcels_sqft = pd.DataFrame.from_csv(os.path.join(Original_Parcel_Folder, Parcels_Sqft_File_Name), sep = ",")   
parcels_sqft = parcels_sqft.join(parcels['TAZ_P'], on = "PSRCID")
parcels_sqft = parcels_sqft.join(taz_subarea[['Subarea', 'SubareaName']], on = 'TAZ_P')
parcels_sqft = parcels_sqft.join(adjustment_factors['Factor'], on = "Subarea")
parcels_sqft = parcels_sqft[~parcels_sqft["Factor"].isnull()]

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
                parcels_sqft[type_level_Job] = parcels_sqft[type_level] * occrate / jobrate * parcels_sqft['FACTOR']
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
parcels_sqft = parcels_sqft.join(ESD_jobs, on = "PSRCID")

print "Exporting \"Converted_jobs_in_BKRarea.csv\""
parcels_sqft.to_csv(os.path.join(Output_Parcel_Folder, "Converted_jobs_in_BKRarea.csv"))
print 'Total converted jobs (in BKR) from sqft are {0:.0f}'.format(parcels_sqft['EMPTOT_P'].sum())

# if there are ESD jobs in a parcel without commercial sqft, they are home based jobs. 
homeoffice_detailedparcels = parcels_sqft.loc[(parcels_sqft['EMPTOT_P'] == 0) & (parcels_sqft['EMPTOT_ESD'] > 0) ]
# export parcels only with home based jobs, and sqft related data
homeoffice_detailedparcels.to_csv(os.path.join(Output_Parcel_Folder, "HomeOfficeParcels_detailed.csv"))  
# export ESD parcel data with only home based jobs      
homeoffice_parcels = parcels.loc[homeoffice_detailedparcels['PSRCID']]
homeoffice_parcels.to_csv(os.path.join(Output_Parcel_Folder, "HomeOfficeParcels.csv"))

# Because the limitation of sqft method, need to put the home office back to the parcel data.
# for home based jobs, put them back to the converted job list. It is now a hybrid of ESD data and converted jobs.
oldindex = parcels_sqft.index.name
parcels_sqft.reset_index(inplace = True)
parcels_sqft = parcels_sqft.set_index('PSRCID') 
parcels_sqft.loc[parcels_sqft.index.isin(homeoffice_parcels.index), JOB_CATEGORY] = homeoffice_parcels[JOB_CATEGORY]
print 'total home based jobs (in BKR) are {0:.0f}'.format(homeoffice_parcels['EMPTOT_P'].sum())
print 'Total converted jobs in BKR (with home based jobs back) are {0:.0f}'.format(parcels_sqft['EMPTOT_P'].sum())

# restore the index to the previous one.
parcels_sqft.reset_index(inplace = True)
parcels_sqft.set_index(oldindex, inplace = True)

newESDLabels = [x.replace('_P', '_ESD') for x in JOB_CATEGORY]
outputlist = JOB_CATEGORY + newESDLabels

summary_by_jurisdiction = parcels_sqft.groupby('JURISDICTION')[outputlist].sum()
print "Exporting \"summary_by_jurisdiction.csv\""
summary_by_jurisdiction.to_csv(os.path.join(Output_Parcel_Folder, "summary_by_jurisdiction.csv"))
summary_by_taz = parcels_sqft.groupby('TAZ_P')[outputlist].sum()
print "Exporting \"summary_by_TAZ.csv\""
summary_by_taz.to_csv(os.path.join(Output_Parcel_Folder, "summary_by_TAZ.csv")) 

#"SUBRAREA" is the index
summary_by_subarea = parcels_sqft.groupby('SUBAREA')[outputlist].sum()
subarea_def = parcels_sqft[["SUBAREA", "SUBAREANAME"]]
subarea_def = subarea_def.drop_duplicates(keep = 'first')  
subarea_def.set_index("SUBAREA", inplace = True)     
summary_by_subarea = summary_by_subarea.join(subarea_def["SUBAREANAME"])
print "Exporting \"summary_by_subarea.csv\""
summary_by_subarea.to_csv(os.path.join(Output_Parcel_Folder, "summary_by_subarea.csv"))

parcels.drop_duplicates(keep = 'first', inplace = True) 
parcels_sqft.drop_duplicates(subset = 'PSRCID', keep = 'first', inplace = True)
parcels_sqft.set_index('PSRCID', inplace = True) 

parcels.loc[parcels.index.isin(parcels_sqft.index), JOB_CATEGORY] = parcels_sqft[JOB_CATEGORY]
#print("Before home office update, total jobs are ", parcels['EMPTOT_P'].sum())
## for parcels with only home based jobs, put them back after sqft conversion. Otherwise, all home based jobs will be missing.
#parcels.loc[parcels.index.isin(homeoffice_parcels.index), JOB_CATEGORY] = homeoffice_parcels[JOB_CATEGORY]
#print("After home office update, total jobs are ", parcels['EMPTOT_P'].sum())
print "Exporting updated urbansim parcel file ..."
parcels.to_csv(os.path.join(Output_Parcel_Folder, Output_Parcel_File_Name), index = True, sep = ' ')
print 'Total jobs in updated parcel file are {0:.0f}'.format(parcels['EMPTOT_P'].sum())

# backup input files inside input folder
print "Backup input files ..."
input_backup_folder = os.path.join(Output_Parcel_Folder, 'inputs')
if not os.path.exists(input_backup_folder):
    os.makedirs(input_backup_folder) 
copyfile(os.path.join(Original_Parcel_Folder, Original_ESD_Parcel_File_Name), os.path.join(input_backup_folder, Original_ESD_Parcel_File_Name))
copyfile(os.path.join(Original_Parcel_Folder, Conversion_Factors_File_Name), os.path.join(input_backup_folder, Conversion_Factors_File_Name))
copyfile(os.path.join(Original_Parcel_Folder, Subarea_Adjustment_Factor_File_Name), os.path.join(input_backup_folder, Subarea_Adjustment_Factor_File_Name))
copyfile(os.path.join(Original_Parcel_Folder, Parcels_Sqft_File_Name), os.path.join(input_backup_folder, Parcels_Sqft_File_Name))

print "Finished"  