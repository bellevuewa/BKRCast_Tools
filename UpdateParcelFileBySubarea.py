import pandana as pdna
import os, sys
sys.path.append(os.getcwd())
import pandas as pd
import numpy as np
import re
from pyproj import Proj, transform
import copy


Original_Parcel_Folder = r"Z:\Modeling Group\BKRCast\Job Conversion Test"
Original_ESD_Parcel_File_Name = r"parcels_urbansim.txt"
#Parcel_FileName_newjobs = r"2014KCParcel_Jobs.csv"
Conversion_Factors_File_Name = r"BKRCast_Conversion_rate.csv"
Subarea_Conversion_Schedule_File_Name = r"subarea_conversion_schedule3.csv"
Output_Parcel_Folder = r"Z:\Modeling Group\BKRCast\Job Conversion Test\LUTest4"
Parcels_Sqft_File_Name = r"2014KCparcel_sqft_to_Jobs.csv"
TAZ_Subarea_File_Name = r"TAZ_subarea.csv"
Output_Parcel_File_Name = "parcels_urbansim_Updated.txt"

CONVERSION_LEVEL = ['verylow', 'low', 'med', 'high', 'veryhigh']
EMPLOYMENT_TYPE = ['EDU', 'FOO', 'GOV', 'IND', 'MED', 'OFC', 'OTH', 'RET', 'SVC', 'NoEMP']

#Parcel_FileName_newjobs:
#PARCELID EMPEDU_P EMPFOO_P EMPGOV_P EMPIND_P EMPMED_P EMPOFC_P EMPOTH_P EMPRET_P EMPRSC_P EMPSVC_P EMPTOT_P HH_P 
JOB_CATEGORY = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPOTH_P', 'EMPRET_P', 'EMPSVC_P', 'EMPTOT_P']

parcels = pd.DataFrame.from_csv(os.path.join(Original_Parcel_Folder, Original_ESD_Parcel_File_Name), sep = " ", index_col = "PARCELID")

#parcels_newjobs = pd.DataFrame.from_csv(os.path.join(Original_Parcel_Folder, Parcel_FileName_newjobs), sep = ",", index_col = "PARCELID")
conversion_rates = pd.DataFrame.from_csv(os.path.join(Original_Parcel_Folder, Conversion_Factors_File_Name), sep = ",")
conversion_schedule = pd.DataFrame.from_csv(os.path.join(Original_Parcel_Folder, Subarea_Conversion_Schedule_File_Name), sep = ",", index_col = "Subarea_ID")
taz_subarea = pd.DataFrame.from_csv(os.path.join(Original_Parcel_Folder, TAZ_Subarea_File_Name), sep = ",", index_col = "TAZNUM")

parcels_sqft = pd.DataFrame.from_csv(os.path.join(Original_Parcel_Folder, Parcels_Sqft_File_Name), sep = ",")   
parcels_sqft = parcels_sqft.join(parcels['TAZ_P'], on = "PSRCID")
parcels_sqft = parcels_sqft.join(taz_subarea[['Subarea', 'SubareaName']], on = 'TAZ_P')
parcels_sqft = parcels_sqft.join(conversion_schedule['Conversion'], on = "Subarea")
parcels_sqft = parcels_sqft[~parcels_sqft["Conversion"].isnull()]

#capitalize all column names to avoid unexpected errors
parcels.columns = [i.upper() for i in parcels.columns]
#parcels_newjobs.columns = [i.upper() for i in parcels_newjobs.columns]
conversion_rates.columns = [i.upper() for i in conversion_rates.columns]
parcels_sqft.columns = [i.upper() for i in parcels_sqft.columns]

# demonstrate how to merge 
#parcels.loc[parcels.index.isin(parcels_newjobs.index), JOB_CATEGORY] = parcels_newjobs[JOB_CATEGORY]
#parcels.to_csv(os.path.join(Output_Parcel_Folder, Output_Parcel_File_Name), index = False, sep = ' ')

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
                parcels_sqft[type_level_Job] = parcels_sqft[type_level] * occrate / jobrate
            else:
                parcels_sqft[type_level_Job] = 0
            parcels_sqft[totJobType] = parcels_sqft[totJobType] + parcels_sqft[type_level_Job]
        parcels_sqft['EMPTOT_P'] = parcels_sqft['EMPTOT_P'] + parcels_sqft[totJobType]
    parcels_sqft['NOEMP_J'] = parcels_sqft['NOEMP'] * conversion_rates.loc['Occupied_Rate', 'NOEMP'] / conversion_rates.loc['Sqft_to_Job_Rate', 'NOEMP']
    return


def CalculateJobs(row, type, totTypeSqft, conversion_verylow, conversion_low, conversion_med, conversion_high, conversion_veryhigh):
    conversion = None
    conversiontype = row['CONVERSION']
    if conversiontype == "verylow":
        conversion = conversion_verylow
    elif conversiontype == "low":
        conversion = conversion_low
    elif conversiontype == "med":
        conversion = conversion_med
    elif conversiontype == "high":
        conversion = conversion_high
    else:
        conversion = conversion_veryhigh

    type_level = type.upper() + "_" + conversiontype.upper()
    jobrate = conversion.loc['Sqft_to_Job_Rate', type_level]
    occrate = conversion.loc['Occupied_Rate', type_level]
    totjobs = row[totTypeSqft] * occrate / jobrate
    return totjobs        
#
# Convert sqft to number of jobs by job category. The conversion rates are controlled by subarea
# empType: EDU, FOO, GOV, IND, NOEMP, MED, OFC, OTH, RET, SVC
def Sqft_to_Jobs_by_Subarea(empType):
    conversion_verylow = GetConversionRates('verylow')
    conversion_low = GetConversionRates('low')
    conversion_med = GetConversionRates('med')
    conversion_high = GetConversionRates('high')
    conversion_veryhigh = GetConversionRates('veryhigh')
    conversion = None    

    list = []
    parcels_sqft['EMPTOT_P'] = 0
   
    for type in empType:
        if type == 'NoEMP':
            continue
        totJobType = "EMP" + type + "_P"  
        totTypeSqft = "SQFT_" + type
        parcels_sqft[totJobType] = 0
        parcels_sqft[totTypeSqft] = 0
        type_ = type + "_"
        for col in parcels_sqft.columns:
            if col.find(type_.upper()) == 0:
                parcels_sqft[totTypeSqft] = parcels_sqft[totTypeSqft] + parcels_sqft[col]

        parcels_sqft[totJobType] = parcels_sqft.apply(lambda row: CalculateJobs(row, type, totTypeSqft, conversion_verylow, conversion_low, conversion_med, conversion_high, conversion_veryhigh), axis = 1)
        parcels_sqft['EMPTOT_P'] = parcels_sqft['EMPTOT_P'] + parcels_sqft[totJobType]
    #parcels_sqft['NOEMP_J'] = parcels_sqft['NOEMP'] * conversion.loc['Occupied_Rate', 'NOEMP'] / conversion.loc['Sqft_to_Job_Rate', 'NOEMP']
    return
  
def Print_DF_Columns(df):
    if type(df) is pd.DataFrame:
        for col in df.columns:
            print col
    else:
        print "Input is not a pandas dataframe"


Sqft_to_Jobs_by_Subarea(EMPLOYMENT_TYPE)
#Sqft_to_Jobs(EMPLOYMENT_TYPE, CONVERSION_LEVEL)  
ESD_jobs = parcels[JOB_CATEGORY]
ESD_jobs = ESD_jobs.rename(columns = lambda x: x.replace('_P', '_ESD'))
parcels_sqft = parcels_sqft.join(ESD_jobs, on = "PSRCID")
parcels_sqft.to_csv(os.path.join(Output_Parcel_Folder, "Converted_jobs_in_BKRarea.csv"))

newESDLabels = [x.replace('_P', '_ESD') for x in JOB_CATEGORY]
outputlist = JOB_CATEGORY + newESDLabels

summary_by_jurisdiction = parcels_sqft.groupby('JURISDICTION')[outputlist].sum()
summary_by_jurisdiction.to_csv(os.path.join(Output_Parcel_Folder, "summary_by_jurisdiction.csv"))
summary_by_taz = parcels_sqft.groupby('TAZ_P')[outputlist].sum()
summary_by_taz.to_csv(os.path.join(Output_Parcel_Folder, "summary_by_TAZ.csv")) 

#"SUBRAREA" is the index
summary_by_subarea = parcels_sqft.groupby('SUBAREA')[outputlist].sum()
subarea_def = parcels_sqft[["SUBAREA", "SUBAREANAME"]]
subarea_def = subarea_def.drop_duplicates(keep = 'first')  
subarea_def.set_index("SUBAREA", inplace = True)     
summary_by_subarea = summary_by_subarea.join(subarea_def["SUBAREANAME"])
summary_by_subarea.to_csv(os.path.join(Output_Parcel_Folder, "summary_by_subarea.csv"))

parcels.drop_duplicates(keep = 'first', inplace = True) 
parcels_sqft.drop_duplicates(subset = 'PSRCID', keep = 'first', inplace = True)
parcels_sqft = parcels_sqft.set_index('PSRCID') 

parcels.loc[parcels.index.isin(parcels_sqft.index), JOB_CATEGORY] = parcels_sqft[JOB_CATEGORY]
parcels.to_csv(os.path.join(Output_Parcel_Folder, Output_Parcel_File_Name), index = True, sep = ' ')

print "Finished"  