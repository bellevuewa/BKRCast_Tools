import pandas as pd
import numpy as np
from utility import Data_Scale_Method, Job_Categories, Parcel_Data_Format, dialog_level, IndentAdapter
import logging, os, sys
from Parcels import Parcels
 
class SynPopDataOperations:
    def __init__(self, synpop_filename: str, output_dir: str, output_filename: str, indent):
        self.filename = synpop_filename
        self.output_dir = output_dir
        self.output_filename = output_filename
        self.indent = indent

        base_logger = logging.getLogger(__name__)
        self.logger = IndentAdapter(base_logger, indent)
        pass

    def interpolate(self, ):
        pass