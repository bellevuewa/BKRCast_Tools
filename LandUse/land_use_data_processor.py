import sys
import os
import logging
from datetime import datetime
import pandas as pd
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QLabel, QPushButton,
    QFileDialog, QVBoxLayout, QHBoxLayout, QLineEdit, QMessageBox,
    QTableWidget, QTableWidgetItem, QMainWindow, QMenu, QTabWidget
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QAction

import parcel_data_processor
import land_use_data_processor_utilities as LU_utility

class LandUseDataProcessor(QMainWindow):
    """Main window for the Land Use Data Processor application."""
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Land Use Data Processor")
        self.setMinimumWidth(750)
        self.file_inputs = {
            "Bellevue": "",
            "Bellevue Fringe": "",
            "Kirkland": "",            
            "Kirkland Fringe": "",
            "Redmond": "",
            "Redmond Fringe": "",
            "Outside BKR": ""
        }

        self._init_ui() 
        self._init_statusbar()
        
    def _init_ui(self):
        """Initialize the user interface."""
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout()
        central_widget.setLayout(main_layout)

        popsim_button = QPushButton("Process Synthetic Population Data")
        popsim_button.clicked.connect(self.process_synthetic_population_data)
        main_layout.addWidget(popsim_button)

        parcel_button = QPushButton("Assemble a New Parcel Data from Different Parcel Files")
        parcel_button.clicked.connect(self.process_parcel_data)
        main_layout.addWidget(parcel_button)

        new_parcels_button = QPushButton("Process Parcel Data from Partner Cities")
        new_parcels_button.clicked.connect(self.process_parcel_data_from_partner_cities)
        main_layout.addWidget(new_parcels_button)

    def _init_statusbar(self):
        """Initialize status bar with four sections."""
        self.status_section1 = QLabel("")
        self.status_section2 = QLabel("")
        self.status_section3 = QLabel("")
        self.status_section4 = QLabel("")

        self.statusBar().addPermanentWidget(self.status_section1, 1)
        self.statusBar().addPermanentWidget(self.status_section2, 1)
        self.statusBar().addPermanentWidget(self.status_section3, 1)
        self.statusBar().addPermanentWidget(self.status_section4, 1)

    def process_parcel_data(self):
        parcel_processor = parcel_data_processor.ParcelProcessor()
        parcel_processor.show()

    def process_synthetic_population_data(self):
        return
    
    def process_parcel_data_from_partner_cities(self):
        processor = LU_utility.ParcelDataProcessor(self)
        processor.show()
        processor.raise_()
        processor.activateWindow()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = LandUseDataProcessor()
    window.show()
    sys.exit(app.exec())