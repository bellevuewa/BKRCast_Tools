import sys, os
sys.path.append(os.getcwd())
import logging
from datetime import datetime
import pandas as pd
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QLabel, QPushButton,
    QFileDialog, QVBoxLayout, QHBoxLayout, QLineEdit, QMessageBox, QSizePolicy, QSplitter,
    QTableWidget, QTableWidgetItem, QMainWindow, QTabWidget, QListWidget, QDialog
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QIntValidator
from enum import Enum
from GUI_support_utilities import (Shared_GUI_Widgets, NumericTableWidgetItem)
import parcel_data_functions as parcel_func

class Parcel_Data_Format(Enum):
    Processed_Parcel_Data = 0
    BKRCastTAZ_Format = 1
    BKR_Trip_Model_TAZ_Forma = 2
class Data_Scale_Method(Enum):
    Keep_the_Data_from_the_Partner_City = 0
    Scale_by_Job_Category = 1
    Scale_by_Total_Jobs_by_TAZ = 2
class ParcelDataProcessor(QMainWindow, Shared_GUI_Widgets):
    """Main window for the Parcel Data Processor application."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Parcel Data Processor")
        self.setMinimumWidth(750)
        self.base_file = r"Z:\Modeling Group\BKRCast\LandUse\Complan\Complan2044\2044LU\DT_rebalance_btw_job_category\parcels_urbansim.txt"
        self.landuse_rules = []
        self.base_parcel_df = None
        self.final_parcel_df = None
        self.subarea_file = r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\TAZ_subarea.csv"
        self.subarea_df = pd.read_csv(self.subarea_file)

        self.output_dir = r"Z:\Modeling Group\BKRCast\LandUse\test_2044_long_range_planning"
        self.horizon_year = -1

        self._init_ui() 
        # create_status bar from Shared_UI_Widgets
        self.create_status_bar(self, 4)
        
    def _init_ui(self):
        """Initialize the user interface."""
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        self.main_layout = QVBoxLayout()
        central_widget.setLayout(self.main_layout)

        hbox = QHBoxLayout()
        hbox.addWidget(QLabel("Horizon Year"))
        self.year_box = QLineEdit()
        self.year_box.setValidator(QIntValidator(2000, 2100))
        self.year_box.setMaxLength(4)
        hbox.addWidget(self.year_box)
        self.main_layout.addLayout(hbox)

        hbox = QHBoxLayout()
        output_button = QPushButton("Select Output Location")
        output_button.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        self.output_label = QLabel("No Location selected")
        output_button.clicked.connect(self.browse_output_file)
        hbox.addWidget(output_button)
        hbox.addWidget(self.output_label)
        self.main_layout.addLayout(hbox) 
        
        hbox = QHBoxLayout()
        subarea_button = QPushButton("Select Subarea Lookup File")
        subarea_button.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        hbox.addWidget(subarea_button)
        self.subarea_label = QLabel("No files selected")
        subarea_button.clicked.connect(lambda: self.select_files("Select Subarea Definition File", self.subarea_label))
        hbox.addWidget(self.subarea_label)  
        self.main_layout.addLayout(hbox)     

        hbox = QHBoxLayout()    
        base_button = QPushButton("Select Base Parcel Data Files")
        base_button.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        hbox.addWidget(base_button)
        self.base_file_label = QLabel("No files selected") 
        # base_button.clicked.connect(lambda: self.select_files("Select Base Parcel File", self.base_file_label))  
        base_button.clicked.connect(self.select_base_parcel_file)
        hbox.addWidget(self.base_file_label)  
        self.main_layout.addLayout(hbox)

        #### create controls for input data
        groupbox_container = QWidget()
        groupbox_layout = QVBoxLayout(groupbox_container)
        groupbox_layout.setContentsMargins(0, 0, 0, 0)
        splitter = QSplitter(Qt.Orientation.Horizontal)
        partner_container, self.jurisdiction_list_box = self.make_list_panel("Partner Cities", ["Bellevue", "Kirkland", "Redmond"])
        splitter.addWidget(partner_container)

        # Data Format
        format_container, self.method_list_box = self.make_list_panel(
            "Data Format",
            [item.name for item in Parcel_Data_Format]
        )
        splitter.addWidget(format_container)

        # Scale By
        scaleby_container, self.scaleby_list_box = self.make_list_panel(
            "Scale By",
            [item.name for item in Data_Scale_Method],
            v_policy=QSizePolicy.Policy.Minimum
        )
        splitter.addWidget(scaleby_container)

        # Initial splitter sizes
        splitter.setSizes([200, 300, 250])

        groupbox_layout.addWidget(splitter)
        self.main_layout.addWidget(groupbox_container)

        add_rules_button = QPushButton("Add Rules")
        add_rules_button.clicked.connect(self.add_rules)
        self.main_layout.addWidget(add_rules_button)

        vbox = QVBoxLayout()
        vbox.addWidget(QLabel("Processing Rules"))
        self.rule_table = QTableWidget()
        self.rule_table.setColumnCount(4)
        self.rule_table.horizontalHeader().setStretchLastSection(True)
        self.rule_table.setHorizontalHeaderLabels(["Jurisdiction", "File", "Data Format", "Scale Method"])
        vbox.addWidget(self.rule_table)
        self.main_layout.addLayout(vbox)
        
        hbox = QHBoxLayout()
        self.valid_btn = QPushButton("Validate")
        self.valid_btn.clicked.connect(lambda: self.validate_files(self.base_file))
        hbox.addWidget(self.valid_btn)

        self.summarize_btn = QPushButton("Summarize")
        self.summarize_btn.clicked.connect(self.summarize_parcel_data) 
        self.summarize_btn.setEnabled(False)
        hbox.addWidget(self.summarize_btn)
        self.main_layout.addLayout(hbox)
        
        process_btn = QPushButton("Start Processing")
        process_btn.clicked.connect(self.parcel_process)
        self.main_layout.addWidget(process_btn)

    def validate_inputs(self):
        if (self.year_box.text() == "") :
            QMessageBox.critical(self, "Warning", "Please enter the horizon year")
            return False

        if (self.year_box.hasAcceptableInput()):
            self.horizon_year = int(self.year_box.text())
            if self.horizon_year > 2100 or self.horizon_year < 2000:
                QMessageBox.critical(self, "Warning", "Please double check the horizon year input")
                return False
        else:
            return False
            
        if self.output_dir == "":
            QMessageBox.critical(self, "Warning", "Select Output Folder First")
            return False
    
        return True


    def make_list_panel(self, title, items, v_policy=QSizePolicy.Policy.Expanding):
        container = QWidget()
        layout = QVBoxLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(5)

        label = QLabel(title)
        listbox = QListWidget()
        listbox.addItems(items)
        listbox.setSizePolicy(QSizePolicy.Policy.Preferred, v_policy)

        layout.addWidget(label)
        layout.addWidget(listbox)

        return container, listbox

    def browse_output_file(self):
        path = QFileDialog.getExistingDirectory(
            self, "Select Output Folder", os.getcwd(),
        )
        if path:
            self.output_label.setText(path)
            self.output_dir = path

    def select_base_parcel_file(self):
        if self.validate_inputs() == False:
            return
        
        base_dialog = BaseDataGenerator(self, "Base Parcel File Processor")
        if base_dialog.exec() == QDialog.DialogCode.Accepted:
            return

    def select_subarea_file(self):
        filename = self.select_files("Select Subarea Definition File", self.subarea_label)
        if filename:
            self.subarea_file = filename
            self.subarea_df = pd.read_csv(filename, sep = ',')
        
    def select_files(self, msg, label):

        filename, _ = QFileDialog.getOpenFileName(self, msg, "", "Text Files (*.txt);;All Files (*)")
        if not filename:
            return
        else:
            label.setText(filename)
            return filename

    def validate_files(self, filename):
        self.status_sections[0].setText("running")
        self.valid_btn.setEnabled(False)

        self.worker = ValidationThread(self, filename)
        self.worker.finished.connect(lambda validate_dict: self._on_thread_finished([self.valid_btn, self.summarize_btn], validate_dict))
        self.worker.error.connect(lambda message: self._on_thread_error(self.valid_btn, self.status_sections[0], message))
        self.worker.start()
        
    def _on_thread_finished(self, btns, validate_dict):
        # called when the thread is finished
        for btn in btns:
            btn.setEnabled(True)
        
        self.status_sections[0].setText("Done")
        # Add validation logic here
        valid_dialog = ValidationAndSummary(self, "Validation and Summary of the base parcel data", validate_dict)
        valid_dialog.exec()
        self.status_sections[0].setText("")

    def _on_thread_error(self, btn, status_bar_section, message):
        # called when the thread encounters an error
        btn.setEnabled(True)
        status_bar_section.setText("Error")
        QMessageBox.critical(self, "Error", message)

    def validate_thread(self, filename):
        if (filename == ""):
            QMessageBox.warning(self, "Warning", "Please select a parcel data file first.")
            return
        
        validation_dict = {}
        
        output_list = []

        if self.base_parcel_df == None:
            self.base_parcel_df  = pd.read_csv(filename, sep = " ", low_memory = False)
        header = ["Column", "Data Type", "Unique Values", "Missing Values", "Duplicated", "Min", "Max", "Mean"]
        for col in self.base_parcel_df.columns:
            series = self.base_parcel_df[col]
            unique_non_null = series.nunique(dropna = True)
            missing = series.isna().sum()
            duplicates = len(series) - unique_non_null - missing
            is_numeric = pd.api.types.is_numeric_dtype(series)
            min = series.min() if is_numeric else ""
            max = series.max() if is_numeric else ""
            mean = series.mean() if is_numeric else ""

            outputs = {
                "Column": col,
                "Data Type": str(series.dtype),
                "Unique Values": unique_non_null,
                "Missing Values": missing,
                "Duplicated": duplicates,
                "Min": min,
                "Max": max,
                "Mean": mean
            }

            output_list.append(outputs)

        # df: validation of data_df
        df = pd.DataFrame(output_list, columns = header)

        # df2: data_df shape
        df2 = pd.DataFrame([{"Rows": self.base_parcel_df.shape[0], "Columns": self.base_parcel_df.shape[1]}])

        df3 = self.base_parcel_df.head(100)   
        
        validation_dict = {
            "Validation": df,
            "Summary": df2,
            "Raw Data Samples": df3
        }
        
        return validation_dict
    
    def summarize_parcel_data(self):
        summary_dict = parcel_func.summarize_parcel_data(self.final_parcel_df, self.subarea_df, self.output_dir) 
        summary_dialog = ValidationAndSummary(self, "Base Parcel File Summary", summary_dict)
        summary_dialog.exec()         
       
    def add_rules(self):
        if (not self.jurisdiction_list_box.selectedItems()) or (not self.method_list_box.selectedItems()) or (not self.scaleby_list_box.selectedItems()):
            QMessageBox.information(self, "Warning", "You cannot leave these boxes blank")

        city = self.jurisdiction_list_box.currentItem().text()
        method = self.method_list_box.currentItem().text()
        scale_method = self.scaleby_list_box.currentItem().text()

        input_filename, _ = QFileDialog.getOpenFileName(self, f"Select input file from {city}", "", "Text Files (*.txt);;All Files (*)")
        rule_dict = {
            "Jurisdiction": city,
            "File": input_filename,
            "Data Format": method,
            "Scale Method": scale_method    
        }
        self.landuse_rules.append(rule_dict)
        # add to the rule table
        self.rule_table.insertRow(self.rule_table.rowCount())
        row = self.rule_table.rowCount() - 1

        for col, key in enumerate(rule_dict.keys()):
            self.rule_table.setItem(row, col, QTableWidgetItem(str(rule_dict[key])))

    def parcel_process(self):
        return

class ValidationAndSummary(QDialog, Shared_GUI_Widgets):
    def __init__(self, parent=None, msg=None, data_dict=None):
        # data_dict: dictionary containing data to be displayed in the tables
        super().__init__(parent)
        self.setWindowTitle("Validation and Summary")
        self.setMinimumWidth(600)
        self.data_dict = data_dict
        self.msg = msg
        self._init_ui()

        # status bar is created from Shared_GUI_Widgets
        self.create_status_bar(self, 4)

    def _init_ui(self):
        """Initialize the user interface."""
        self.main_layout = QVBoxLayout()
        self.setLayout(self.main_layout)

        info_label = QLabel(self.msg)
        self.main_layout.addWidget(info_label)
        self.tab_pages = {}

        self.tabs = QTabWidget()
        
        for key, value in self.data_dict.items():
            self.tab_pages[key] = QTableWidget() 
            self.tab_pages[key].setSortingEnabled(False)
            self.tab_pages[key].setRowCount(value.shape[0])
            self.tab_pages[key].setColumnCount(len(value.columns))
            self.tab_pages[key].setHorizontalHeaderLabels(value.columns)
            self.tab_pages[key].setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
            self.tab_pages[key].customContextMenuRequested.connect( 
                lambda pos, t=self.tab_pages[key]: self.create_context_menu(t, pos)
            )
            self.tab_pages[key].selectionModel().selectionChanged.connect(
                lambda sel, des, t=self.tab_pages[key]: self.on_table_selection_changed(t)
            )

            # load data into tab. value is a dataframe
            for row in range(value.shape[0]):
                for col in range(len(value.columns)):
                    val = value.iat[row, col]
                    item = NumericTableWidgetItem(val)
                    self.tab_pages[key].setItem(row, col, item)

            self.tab_pages[key].setSortingEnabled(True)
            self.tabs.addTab(self.tab_pages[key], key)

        self.main_layout.addWidget(self.tabs)
        close_button = QPushButton("Close")
        close_button.clicked.connect(self.close)
        self.main_layout.addWidget(close_button)

class ValidationThread(QThread):
    finished = pyqtSignal(dict)
    error = pyqtSignal(str)
    status_update = pyqtSignal(str, str, str, str) #status bar section 1 ~ 4

    def __init__(self, parent, filename):
        super().__init__()
        self.parent = parent
        self.filename = filename

    def run(self):
        try:
            validate_dict = self.parent.validate_thread(self.filename)
        except Exception as e:
            self.error.emit(str(e))
        finally: 
            self.finished.emit(validate_dict)

class ThreadWrapper(QThread):
    finished = pyqtSignal(object)
    error = pyqtSignal(str)
    status_update = pyqtSignal(str, str, str, str) #status bar section 1 ~ 4

    def __init__(self, func, *args, **kwargs):
        super().__init__()
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def run(self):
        try:
            ret = None
            ret = self.func(*self.args, **self.kwargs)
        except Exception as e:
            self.error.emit(str(e))
        finally: 
            self.finished.emit(ret)

class BaseDataGenerator(QDialog, Shared_GUI_Widgets):
    def __init__(self, parent = None, message = None):
        super().__init__(parent)
        self.__init_ui__(message)
        self.create_status_bar(self, 4)
        
        self.base_file = ""
        self.lower_boundary_file = ""
        self.uppfer_boundary_file = ""
        self.base_parcel_df = None

    def __init_ui__(self, msg):
        """Initialize the user interface."""
        self.setWindowTitle("Base Parcel Process")
        self.main_layout = QVBoxLayout()
        self.setLayout(self.main_layout)

        info_label = QLabel(msg)
        self.main_layout.addWidget(info_label)
        hbox = QHBoxLayout()
        op1_label = QLabel("Select a Parcel File as the Base")
        hbox.addWidget(op1_label)
        select_btn = QPushButton("Select a Base File")
        select_btn.clicked.connect(lambda: self.select_file("Select a Base Parcel File", op1_label))
        hbox.addWidget(select_btn)
        self.main_layout.addLayout(hbox)

        self.base_filename_label = QLabel("No File is Selected")
        self.main_layout.addWidget(self.base_filename_label)

        groupbox_layout =  QVBoxLayout()
        groupbox_layout.addWidget(QLabel("Interpolate from Two Parcel Files"))
        hbox = QHBoxLayout()
        self.sel1_btn = QPushButton("Select the Parcel File for the Lower Boundary")
        self.sel1_btn.clicked.connect(lambda: self.select_file_for_interpolation("lower"))
        self.sel2_btn = QPushButton("Select the Parcel File for the Upper Boundary")
        self.sel2_btn.clicked.connect(lambda: self.select_file_for_interpolation("upper"))
        hbox.addWidget(self.sel1_btn)
        hbox.addWidget(self.sel2_btn)
        groupbox_layout.addLayout(hbox)

        self.table = QTableWidget()
        self.table.setColumnCount(3)
        self.table.setHorizontalHeaderLabels(["Side", "Year", "File"])
        self.table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table.customContextMenuRequested.connect(lambda pos: self.create_context_menu(self.table, pos))
        groupbox_layout.addWidget(self.table)

        self.interpolate_btn = QPushButton("Interpolate")
        self.interpolate_btn.clicked.connect(self.interpolation)
        groupbox_layout.addWidget(self.interpolate_btn)
        self.main_layout.addLayout(groupbox_layout)

        hbox = QHBoxLayout()
        self.valid_btn = QPushButton("Validate")
        self.valid_btn.clicked.connect(self.validate_files)
        self.valid_btn.setEnabled(False)
        hbox.addWidget(self.valid_btn)

        self.summarize_btn = QPushButton("Summarize")
        self.summarize_btn.clicked.connect(self.summarize_parcel_data) 
        self.summarize_btn.setEnabled(False)
        hbox.addWidget(self.summarize_btn)
        self.main_layout.addLayout(hbox)

    def select_file_for_interpolation(self, side):
        path, _ = QFileDialog.getOpenFileName(
                    self, f"Please select a parcel file for {side} boundary", "",
                    "Data Files (*.csv *.txt *.*)"
                )
        rowCount = self.table.rowCount()
        self.table.insertRow(rowCount)
        self.table.setItem(rowCount, 0, QTableWidgetItem(side))
        year_item = QTableWidgetItem(0)
        self.table.setItem(rowCount, 1, year_item)
        self.table.setItem(rowCount, 2, QTableWidgetItem(path))

    def select_file(self, message, label = None):
        path, _ = QFileDialog.getOpenFileName(
                    self, message, "",
                    "Data Files (*.csv *.txt *.*)"
                )
        if path and label is not None:
            label.setText(path)
            self.sel1_btn.setEnabled(False)
            self.sel2_btn.setEnabled(False)
            self.interpolate_btn.setEnabled(False)
            self.base_parcel_df = pd.read_csv(path, sep = ' ')
            self.status_sections[0].setText("Base parcel selected.")
            self.valid_btn.setEnabled(True)
            self.summarize_btn.setEnabled(True)
        return
    
    def changeButtonStatus(self, buttons, Enabled):
        if buttons:
            for btn in buttons:
                btn.setEnabled(Enabled)

    def interpolation(self):
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

        self.worker = ThreadWrapper(parcel_func.interpolate_two_parcel_files, lower_path, upper_path, lower, upper, self.parent().horizon_year)
        self.worker.finished.connect(lambda interpolation_df: self._on_interpolation_finished(btns, interpolation_df))
        self.worker.error.connect(lambda message: self._on_interpolation_error(btns, self.status_sections[0], message))
        self.worker.start()

    def _on_interpolation_finished(self, btns,  interpolation_df):
        self.base_parcel_df = interpolation_df
        self.changeButtonStatus(btns, True)
        self.status_sections[0].setText('Done')

    def _on_interpolation_error(self, btns, message):
        # called when the thread encounters an error
        self.changeButtonStatus(btns, True)
        self.status_sections[0].setText("interpolation failed")
        QMessageBox.critical(self, "Error", message)
                             
    def validate_files(self):
        self.status_sections[0].setText("running")
        self.valid_btn.setEnabled(False)

        self.worker = ThreadWrapper(parcel_func.validate_parcel_file, self.base_parcel_df)
        self.worker.finished.connect(lambda validate_dict: self._on_validation_finished([self.valid_btn, self.summarize_btn], validate_dict))
        self.worker.error.connect(lambda message: self._on_validation_error(self.valid_btn, self.status_sections[0], message))
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

    def summarize_parcel_data(self):

        summary_dict = parcel_func.summarize_parcel_data(self.base_parcel_df, self.parent().subarea_df, self.parent().output_dir) 
        summary_dialog = ValidationAndSummary(self, "Base Parcel File Summary", summary_dict)
        summary_dialog.exec()       

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = ParcelDataProcessor()
    window.show()
    sys.exit(app.exec())