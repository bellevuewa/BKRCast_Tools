import sys, os
sys.path.append(os.getcwd())
import logging
import traceback

import pandas as pd
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QLabel, QPushButton,
    QFileDialog, QVBoxLayout, QHBoxLayout, QLineEdit, QMessageBox, QSizePolicy, QSplitter,
    QTableWidget, QTableWidgetItem, QMainWindow, QTabWidget, QListWidget, QDialog, QHeaderView
)

from PyQt6.QtCore import Qt, QThread, pyqtSignal
from enum import Enum
from GUI_support_utilities import (Shared_GUI_Widgets, NumericTableWidgetItem)
from land_use_data_processor_utilities import ThreadWrapper, ValidationAndSummary
from parcel_data_functions import Parcel_Data_Format, Data_Scale_Method
from synpop_interpolation import LinearSynPopInterpolator
from Parcels import Parcels
from ParcelDataOperations import ParcelDataOperations
from utility import IndentAdapter, dialog_level
from synthetic_population import SyntheticPopulation

class SynPopDataUserInterface(QDialog, Shared_GUI_Widgets):
    def __init__(self, project_setting, parent = None):
        super().__init__(parent)
        self.project_settings = project_setting
        self.output_dir = self.project_settings['output_dir']
        self.horizon_year = self.project_settings['horizon_year']
        self.scenario_name = self.project_settings['scenario_name']

        self.base_synpop : SyntheticPopulation = None
        self.final_synpop  : SyntheticPopulation = None

        self.__init_ui__()
        self.create_status_bar(self, 4)
        base_logger = logging.getLogger(__name__)
        indent = dialog_level(self)
        self.logger = IndentAdapter(base_logger, indent)
        self.logger.info("Popsim Data UI initialized.")

    def __init_ui__(self):
        self.setWindowTitle("Synthetic Population Data Processor")
        self.main_layout = QVBoxLayout()
        self.setLayout(self.main_layout)

        hbox = QHBoxLayout()
        hbox.addWidget(QLabel("Horizon Year"))
        year_box = QLabel(str(self.horizon_year))
        hbox.addWidget(year_box)
        self.main_layout.addLayout(hbox)

        hbox = QHBoxLayout()
        hbox.addWidget(QLabel("Output Directory"))
        hbox.addWidget(QLabel(self.output_dir))
        self.main_layout.addLayout(hbox) 

        hbox = QHBoxLayout()    
        base_button = QPushButton("Select Base Population Data Files")
        base_button.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        hbox.addWidget(base_button)
        self.base_file_label = QLabel("No files selected") 
        base_button.clicked.connect(self.select_base_popsim_file)
        hbox.addWidget(self.base_file_label)  
        self.main_layout.addLayout(hbox)
        pass

    def select_base_popsim_file(self):
        base_dialog = BaseSynPopDataGenerator(self, "Base PopSim Data Processor")
        if base_dialog.exec() == QDialog.DialogCode.Accepted:
            self.base_synpop = base_dialog.base_synpop
            self.base_file_label.setText(base_dialog.base_file)
        pass

class BaseSynPopDataGenerator(QDialog, Shared_GUI_Widgets):
    def __init__(self, parent = None, message = None):
        super().__init__(parent)
        self.__init_ui__(message)
        self.create_status_bar(self, 4)
        
        self.base_file = ""
        self.lower_boundary_file = ""
        self.upper_boundary_file = ""
        self.blockgroup_file = ''
        self.base_synpop : SyntheticPopulation = None

        base_logger = logging.getLogger(__name__)
        indent = dialog_level(self)
        self.logger = IndentAdapter(base_logger, indent)

        self.logger.info("Base Synthetic Population Data Generator initialized.")

    def __init_ui__(self, msg):
        """Initialize the user interface."""
        self.setWindowTitle("Base Parcel Process")
        self.main_layout = QVBoxLayout()
        self.setLayout(self.main_layout)

        info_label = QLabel(msg)
        self.main_layout.addWidget(info_label)
        hbox = QHBoxLayout()
        self.op1_label = QLabel("Select a Parcel File as the Base")
        hbox.addWidget(self.op1_label)
        select_btn = QPushButton("Select a Base File")
        select_btn.clicked.connect(lambda: self.select_file("Select a Base Population File", self.op1_label))
        hbox.addWidget(select_btn)
        self.main_layout.addLayout(hbox)

        self.base_filename_label = QLabel("No File is Selected")
        self.main_layout.addWidget(self.base_filename_label)

        groupbox_layout =  QVBoxLayout()
        groupbox_layout.addWidget(QLabel("Interpolate from Two Population Files"))
        hbox = QHBoxLayout()
        self.sel1_btn = QPushButton("Select the Population File for the Lower Boundary")
        self.sel1_btn.clicked.connect(lambda: self.select_file_for_interpolation("lower"))
        self.sel2_btn = QPushButton("Select the Population File for the Upper Boundary")
        self.sel2_btn.clicked.connect(lambda: self.select_file_for_interpolation("upper"))
        hbox.addWidget(self.sel1_btn)
        hbox.addWidget(self.sel2_btn)      
        groupbox_layout.addLayout(hbox)

        ofm_btn = QPushButton("Select a census block group File")
        ofm_btn.clicked.connect(lambda: self.select_block_group_file("Select a census block group File", self.ofm_label))
        self.main_layout.addWidget(ofm_btn)
        self.ofm_label = QLabel("no census block group file selected")
        self.main_layout.addWidget(self.ofm_label)

        self.table = QTableWidget()
        self.table.setColumnCount(3)
        self.table.setHorizontalHeaderLabels(["Side", "Year", "File"])
        self.table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table.customContextMenuRequested.connect(lambda pos: self.create_context_menu(self.table, pos))
        groupbox_layout.addWidget(self.table)

        self.interpolate_btn = QPushButton("Interpolate")
        self.interpolate_btn.clicked.connect(self.interpolation_btn_clicked)
        groupbox_layout.addWidget(self.interpolate_btn)
        self.main_layout.addLayout(groupbox_layout)

        hbox = QHBoxLayout()
        self.valid_btn = QPushButton("Validate")
        self.valid_btn.clicked.connect(self.validate_btn_clicked)
        self.valid_btn.setEnabled(False)
        hbox.addWidget(self.valid_btn)

        self.summarize_btn = QPushButton("Summarize")
        self.summarize_btn.clicked.connect(self.summarize_btn_clicked) 
        self.summarize_btn.setEnabled(False)
        hbox.addWidget(self.summarize_btn)
        self.main_layout.addLayout(hbox)        

    def select_file_for_interpolation(self, side):
        path, _ = QFileDialog.getOpenFileName(
                    self, f"Please select a population file for {side} boundary", "",
                    "HDF5 Files (*.h5);;All Files(*.*)"
                )
        self.op1_label.setText('No file is selected')
        self.base_file = ''
        self.base_synpop = None
        rowCount = self.table.rowCount()
        self.table.insertRow(rowCount)
        self.table.setItem(rowCount, 0, QTableWidgetItem(side))
        year_item = QTableWidgetItem(0)
        self.table.setItem(rowCount, 1, year_item)
        self.table.setItem(rowCount, 2, QTableWidgetItem(path))

    def select_file(self, message, label = None):
        path, _ = QFileDialog.getOpenFileName(
                    self, message, "",
                    "HDF5 Files (*.h5);;All Files(*.*)"
                )
        if (path is not None) and (label is not None):
            label.setText(path)
            self.sel1_btn.setEnabled(False)
            self.sel2_btn.setEnabled(False)
            self.interpolate_btn.setEnabled(False)
            indent = dialog_level(self)
            self.status_sections[0].setText("Base Population selected.")
            self.valid_btn.setEnabled(True)
            self.summarize_btn.setEnabled(True)
            self.base_file = path
            self.base_synpop = SyntheticPopulation(self.parent().project_settings['subarea_file'], self.parent().project_settings['lookup_file'], self.base_file, self.parent().project_settings['horizon_year'], indent + 1)
            self.logger.info(f"Selected base synthetic population file: {path}")
        return
    
    def select_block_group_file(self, message, label = None):
        path, _ = QFileDialog.getOpenFileName(
                    self, message, "",
                    "csv Files (*.csv);;All Files(*.*)"
                )
        if (path is not None) and (label is not None):
            label.setText(path)
            self.sel1_btn.setEnabled(True)
            self.sel2_btn.setEnabled(True)
            indent = dialog_level(self)
            self.status_sections[0].setText("Blockgroup file selected.")
            self.valid_btn.setEnabled(True)
            self.summarize_btn.setEnabled(True)
            self.blockgroup_file = path

            self.logger.info(f"Selected block group template file: {path}")
        return
    
    def changeButtonStatus(self, buttons, Enabled):
        if buttons:
            for btn in buttons:
                btn.setEnabled(Enabled)

    def interpolation_btn_clicked(self):
        self.status_sections[0].setText("interpolating")
        btns = self.findChildren(QPushButton)
        self.changeButtonStatus(btns, False)

        self.valid_btn.setEnabled(False)
        self.summarize_btn.setEnabled(False)
        self.interpolate_btn.setEnabled(False)

        if (self.table.item(0,1).text().strip().isdigit() == False) or (self.table.item(1,1).text().strip().isdigit() == False):
            QMessageBox.critical(self, "Error", "Check horizon years for interpolation.")
            return
        
        base_year_dict = {}
        num_row = self.table.rowCount()

        if num_row > 2 or num_row < 2:
            QMessageBox.critical(self, "Error", "Too many files for interpolation.")
            return
        
        for row in range(num_row):
            base_year_dict[self.table.item(row, 0).text()] = {"year": int(self.table.item(row, 1).text()), "path": self.table.item(row, 2).text()}
        
        lower = int(base_year_dict['lower']['year'])
        lower_path = base_year_dict['lower']['path']
        upper = int(base_year_dict['upper']['year'])
        upper_path = base_year_dict['upper']['path']

        self.worker = ThreadWrapper(self.interpolate_two_pop_files, lower_path, upper_path, lower, upper, self.parent().horizon_year)
        self.worker.finished.connect(lambda interpolated_synpop: self._on_interpolation_finished(btns, interpolated_synpop))
        self.worker.error.connect(lambda eobj: self._on_interpolation_error(btns, eobj))
        self.worker.start()

    def interpolate_two_pop_files(self, lower_path, upper_path, lower_year, upper_year, horizon_year):
        import debugpy
        debugpy.breakpoint()
        indent = dialog_level(self)
        left_synpop = SyntheticPopulation(self.parent().project_settings['subarea_file'], self.parent().project_settings['lookup_file'], lower_path, lower_year, indent + 1)
        right_synpop = SyntheticPopulation(self.parent().project_settings['subarea_file'], self.parent().project_settings['lookup_file'], upper_path, upper_year, indent + 1)

        interpolation = LinearSynPopInterpolator(self.parent().output_dir, self.blockgroup_file, indent)

        self.logger.info(f"Interpolating synthetic population data between {lower_year} and {upper_year} for horizon year {horizon_year}")
        self.logger.info(f"Lower boundary file: {lower_path}")
        self.logger.info(f"Upper boundary file: {upper_path}")

        # Parcels DataFrame after interpolation
        interpolated_synpop = interpolation.interpolate(left_synpop, right_synpop, horizon_year)
        return interpolated_synpop

    def _on_interpolation_finished(self, btns, synpop : SyntheticPopulation):
        self.base_synpop = synpop
        self.changeButtonStatus(btns, True)
        self.status_sections[0].setText('Done')
        self.base_filename_label.setText(self.base_synpop.filename)
        self.base_file = self.base_synpop.filename

    def _on_interpolation_error(self, btns, exception_obj):
        # called when the thread encounters an error
        self.changeButtonStatus(btns, True)
        self.status_sections[0].setText("interpolation failed")
        QMessageBox.critical(self, "Error", str(exception_obj))
                             
    def validate_btn_clicked(self):
        self.status_sections[0].setText("running")
        self.valid_btn.setEnabled(False)

        self.worker = ThreadWrapper(self.base_synpop.validate_hhs_persons)
        self.worker.finished.connect(lambda validate_dict: self._on_validation_finished([self.valid_btn, self.summarize_btn], validate_dict))
        self.worker.error.connect(lambda message: self._on_validation_error([self.valid_btn, self.summarize_btn], self.status_sections[0], message))
        self.worker.start()
        
    def _on_validation_finished(self, btns, validate_dict):
        # called when the thread is finished
        for btn in btns:
            btn.setEnabled(True)
        
        self.status_sections[0].setText("Done")
        # Add validation logic here
        valid_dialog = ValidationAndSummary(self, "Validation and Summary of the base parcel data", validate_dict)
        valid_dialog.exec()
        self.status_sections[0].setText("")

    def _on_validation_error(self, btn, status_bar_section, message):
        # called when the thread encounters an error
        btn.setEnabled(True)
        status_bar_section.setText("Error")
        QMessageBox.critical(self, "Error", message)

    def summarize_btn_clicked(self):
        self.status_sections[0].setText("running")
        btns = self.findChildren(QPushButton)
        for btn in btns:
            btn.setEnabled(False)
        
        self.worker = ThreadWrapper(self.base_synpop.summarize_synpop, self.parent().output_dir, 'base', False, True)
        self.worker.finished.connect(lambda summary_dict: self._on_summary_thread_finished(summary_dict))
        self.worker.error.connect(lambda message: self._on_validation_error(self.summarize_btn, self.status_sections[0], message))
        self.worker.start()

    def _on_summary_thread_finished(self, data_dict):
        self.status_sections[0].setText("Done")
        btns = self.findChildren(QPushButton)
        for btn in btns:
            btn.setEnabled(True)

        summary_dialog = ValidationAndSummary(self, "Base Synthetic Population Summary", data_dict)
        summary_dialog.exec()           


    def closeEvent(self, event):
        self.logger.info("Base Parcel Data Generator is closed.")
        self.accept()
        event.accept()   
