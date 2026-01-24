import sys, os
import pandas as pd
from enum import Enum
from utility import *


class Parcel_Data_Format(Enum):
    Processed_Parcel_Data = "Processed_Parcel_Data"
    BKRCastTAZ_Format = "BKRCastTAZ_Format"
    BKR_Trip_Model_TAZ_Forma = "BKR_Trip_Model_TAZ_Forma"
class Data_Scale_Method(Enum):
    Keep_the_Data_from_the_Partner_City = "Keep_the_Data_from_the_Partner_City"
    Scale_by_Job_Category = "Scale_by_Job_Category"
    Scale_by_Total_Jobs_by_TAZ = "Scale_by_Total_Jobs_by_TAZ"


    