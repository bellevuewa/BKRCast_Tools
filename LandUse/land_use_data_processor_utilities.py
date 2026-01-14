import sys
import os
import logging
from datetime import datetime
import pandas as pd
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QLabel, QPushButton,
    QFileDialog, QVBoxLayout, QHBoxLayout, QLineEdit, QMessageBox, QSizePolicy, QSplitter,
    QTableWidget, QTableWidgetItem, QMainWindow, QMenu, QTabWidget, QListWidget, QDialog, QStatusBar
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QAction 


Parcel_Data_Format = {
    0: "Processed Parcel Data",
    1: "Data in BKRCastTAZ Format",
    2: "Data in BKR Trip Model TAZ Format"
}

Data_Scale_Method = {
    0: "Keep the data from the partner city",
    1: "Scale by Job Category",
    2: "Scale by Total Jobs by TAZ"
}

class ParcelDataProcessor(QMainWindow):
    """Main window for the Parcel Data Processor application."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Parcel Data Processor")
        self.setMinimumWidth(750)
        self.base_file = r"Z:\Modeling Group\BKRCast\LandUse\Complan\Complan2044\2044LU\DT_rebalance_btw_job_category\parcels_urbansim.txt"
        self.landuse_rules = []
        self.base_parcel_df = None
        self.subarea_file = r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\TAZ_subarea.csv"
        self.subarea_df = pd.read_csv(self.subarea_file)

        self.output_dir = r"Z:\Modeling Group\BKRCast\LandUse\test_2044_long_range_planning"

        self._init_ui() 
        self._init_statusbar()
        
    def _init_ui(self):
        """Initialize the user interface."""
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout()
        central_widget.setLayout(main_layout)

        hbox = QHBoxLayout()
        base_button = QPushButton("Select Base Parcel Data Files")
        base_button.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        hbox.addWidget(base_button)
        self.base_file_label = QLabel("No files selected")
        base_button.clicked.connect(lambda: self.select_files("Select Base Parcel File", self.base_file_label))  
        hbox.addWidget(self.base_file_label)  
        main_layout.addLayout(hbox)

        hbox = QHBoxLayout()
        subarea_button = QPushButton("Select Subarea Lookup File")
        subarea_button.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        hbox.addWidget(subarea_button)
        self.subarea_label = QLabel("No files selected")
        subarea_button.clicked.connect(lambda: self.select_files("Select Subarea Definition File", self.subarea_label))
        hbox.addWidget(self.subarea_label)  
        main_layout.addLayout(hbox)       

        hbox = QHBoxLayout()
        output_button = QPushButton("Select Output Location")
        output_button.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        self.output_label = QLabel("No Location selected")
        output_button.clicked.connect(self.browse_output_file)
        hbox.addWidget(output_button)
        hbox.addWidget(self.output_label)
        main_layout.addLayout(hbox)

        hbox = QHBoxLayout()
        self.valid_btn = QPushButton("Validate")
        self.valid_btn.clicked.connect(lambda: self.validate_files(self.base_file))
        hbox.addWidget(self.valid_btn)

        self.summarize_btn = QPushButton("Summarize")
        self.summarize_btn.clicked.connect(lambda: self.summarize_parcel_data(self.base_parcel_df, self.subarea_df, self.output_dir)) 
        self.summarize_btn.setEnabled(False)
        hbox.addWidget(self.summarize_btn)
        main_layout.addLayout(hbox)

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
            list(Parcel_Data_Format.values())
        )
        splitter.addWidget(format_container)

        # Scale By
        scaleby_container, self.scaleby_list_box = self.make_list_panel(
            "Scale By",
            list(Data_Scale_Method.values()),
            v_policy=QSizePolicy.Policy.Minimum
        )
        splitter.addWidget(scaleby_container)

        # Initial splitter sizes
        splitter.setSizes([200, 300, 250])

        groupbox_layout.addWidget(splitter)
        main_layout.addWidget(groupbox_container)

        add_rules_button = QPushButton("Add Rules")
        add_rules_button.clicked.connect(self.add_rules)
        main_layout.addWidget(add_rules_button)

        vbox = QVBoxLayout()
        vbox.addWidget(QLabel("Processing Rules"))
        self.rule_table = QTableWidget()
        self.rule_table.setColumnCount(4)
        self.rule_table.horizontalHeader().setStretchLastSection(True)
        self.rule_table.setHorizontalHeaderLabels(["Jurisdiction", "File", "Data Format", "Scale Method"])
        vbox.addWidget(self.rule_table)
        main_layout.addLayout(vbox)

        process_btn = QPushButton("Start Processing")
        process_btn.clicked.connect(self.parcel_process)
        main_layout.addWidget(process_btn)


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

    def browse_output_file(self):
        path = QFileDialog.getExistingDirectory(
            self, "Select Output Folder", os.getcwd(),
        )
        if path:
            self.output_label.setText(path)

    def select_base_parcel_file(self):
        filename = self.select_files("Select Base Parcel File", self.base_file_label)
        if filename:
            self.base_file = filename

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
        self.status_section1.setText("running")
        self.valid_btn.setEnabled(False)

        self.worker = ValidationThread(self, filename)
        self.worker.finished.connect(lambda validate_dict: self._on_thread_finished([self.valid_btn, self.summarize_btn], validate_dict))
        self.worker.error.connect(lambda message: self._on_thread_error(self.valid_btn, self.status_section1, message))
        self.worker.start()
        
    def _on_thread_finished(self, btns, validate_dict):
        # called when the thread is finished
        for btn in btns:
            btn.setEnabled(True)
        
        self.status_section1.setText("Done")
        # Add validation logic here
        valid_dialog = ValidationAndSummary(self, "Validation and Summary of the base parcel data", validate_dict)
        valid_dialog.exec()
        self.status_section1.setText("")

    def _on_thread_error(self, btn, status_bar_section, message):
        # called when the thread encounters an error
        btn.setEnabled(True)
        status_bar_section.setText("Error")
        QMessageBox.critical(self, "Error", message)


    def validate_thread(self, filename):
        if (filename == ""):
            QMessageBox.warning(self, "Warning", "Please select a base parcel data file first.")
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

    def summarize_files(self, df):
        if df is None:
            QMessageBox.Critical(self, "Error", "You need to select the data file")

        if self.subarea_df is None:
            self.subarea_df = pd.read_csv(self.subarea_file)

        
    def summarize_parcel_data(self, parcel_df, subarea_df=None, output_dir=None):
        if self.subarea_df is None:
            self.subarea_df = pd.read_csv(self.subarea_file)

        parcel_df = parcel_df.merge(subarea_df[['BKRCastTAZ', 'Jurisdiction', 'Subarea']], left_on="TAZ_P", right_on = "BKRCastTAZ", how="left")
        cols = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPOTH_P', 'EMPRET_P', 'EMPSVC_P', 'EMPTOT_P', 'STUGRD_P', 'STUHGH_P', 'STUUNI_P', 'HH_P']
        summary_jurisdictions = parcel_df.groupby('Jurisdiction')[cols].sum().reset_index()
        summary_taz = parcel_df.groupby('TAZ_P')[cols].sum().reset_index()
        summary_subarea = parcel_df.groupby('Subarea')[cols].sum().reset_index()
        summary_subarea = summary_subarea.merge(subarea_df[['Subarea', 'SubareaName']].drop_duplicates(), on='Subarea', how='left')

        if output_dir is None:
            output_dir = os.getcwd()
        summary_jurisdictions.to_csv(os.path.join(output_dir, 'parcel_summary_by_jurisdiction.csv'), index=False)
        summary_taz.to_csv(os.path.join(output_dir, 'parcel_summary_by_taz.csv'), index=False)
        summary_subarea.to_csv(os.path.join(output_dir, 'parcel_summary_by_subarea.csv'), index=False)

        summary_dict = {
            "Jurisdiction": summary_jurisdictions,
            "Subarea": summary_subarea,
            "TAZ": summary_taz
        }

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

class ValidationAndSummary(QDialog):
    def __init__(self, parent=None, msg=None, data_dict=None):
        # data_dict: dictionary containing data to be displayed in the tables
        super().__init__(parent)
        self.setWindowTitle("Validation and Summary")
        self.setMinimumWidth(600)
        self.data_dict = data_dict
        self.msg = msg
        self._init_ui()
        self._init_statusbar()

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
                lambda pos, t=self.tab_pages[key]: self.show_table_context_menu(t, pos)
            )
            self.tab_pages[key].selectionModel().selectionChanged.connect(
                lambda sel, des, t=self.tab_pages[key]: self.on_table_selection_changed(t)
            )

            # load data into tab. value is a dataframe
            for row in range(value.shape[0]):
                for col in range(len(value.columns)):
                    val = value.iat[row, col]
                    item = NumbericTableWidgetItem(val)
                    self.tab_pages[key].setItem(row, col, item)

            self.tab_pages[key].setSortingEnabled(True)
            self.tabs.addTab(self.tab_pages[key], key)

        self.main_layout.addWidget(self.tabs)
        close_button = QPushButton("Close")
        close_button.clicked.connect(self.close)
        self.main_layout.addWidget(close_button)


    def _init_statusbar(self):
        """Initialize status bar with four sections."""
        self.status_bar = QStatusBar()
        self.status_section1 = QLabel("")
        self.status_section2 = QLabel("")
        self.status_section3 = QLabel("")
        self.status_section4 = QLabel("")

        self.status_bar.addPermanentWidget(self.status_section1, 1)
        self.status_bar.addPermanentWidget(self.status_section2, 1)
        self.status_bar.addPermanentWidget(self.status_section3, 1)
        self.status_bar.addPermanentWidget(self.status_section4, 1)

        self.main_layout.addWidget(self.status_bar)

    def show_table_context_menu(self, table, position):
        menu = QMenu()
        copy_action = QAction("Copy All to Clipboard", self)
        copy_action.triggered.connect(lambda: self.copy_selected_cells(table))
        menu.addAction(copy_action)
        menu.exec(table.viewport().mapToGlobal(position))

    def copy_selected_cells(self, table):
        rows = table.rowCount()
        cols = table.columnCount()
        headers = [table.horizontalHeaderItem(col).text() for col in range(cols)]

        text = '\t'.join(headers) + '\n'

        for row in range(rows):
            row_data = []
            for col in range(cols):
                item = table.item(row, col)
                row_data.append(item.text() if item else '')
            text += '\t'.join(row_data) + '\n'  

        clipboard = QApplication.clipboard()
        clipboard.setText(text)
        QMessageBox.information(self, 'Copied', 'All data copied to the clipboard including headers')

    def on_table_selection_changed(self, table):
        """compute sum of selected numeric cells"""
        items = table.selectedItems()
        total = 0.0
        found = False

        for it in items:
            txt = (it.text() or '').strip()
            if txt == '':
                continue
            #remove thousnad separator ","
            txt2 = txt.replace(',',  '')
            try:
                val = float(txt2)
                total += val
                found = True
            except Exception: # ignore all non_numeric cells
                continue

        if found:
            self.status_section2.setText(f"Sum: {total}")
            self.status_section3.setText(f"{len(items)} selected")
        else:
            self.status_section2.setText("")
            self.status_section3.setText("")

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

class NumbericTableWidgetItem(QTableWidgetItem):
    """Custom QTableWidgetItem that treats numbers correctly for sorting."""
    def __init__(self, text):
        text = "" if text is None else str(text)
        super().__init__(text)
        try:
            self.numeric_value = float(text)
            self.is_numeric = True
        except ValueError:
            self.numeric_value = text
            self.is_numeric = False

    def __lt__(self, other):
        if isinstance(other, NumbericTableWidgetItem):
            if self.is_numeric and other.is_numeric:
                return self.numeric_value < other.numeric_value
            
            if self.is_numeric != other.is_numeric:
                return self.is_numeric
        return super().__lt__(other)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = ParcelDataProcessor()
    window.show()
    sys.exit(app.exec())