import sys
import os
import logging
from datetime import datetime
import pandas as pd
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QLabel, QPushButton,
    QFileDialog, QVBoxLayout, QHBoxLayout, QLineEdit, QMessageBox, QSizePolicy,
    QTableWidget, QTableWidgetItem, QMainWindow, QMenu, QTabWidget, QListWidget, QDialog, QStatusBar
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QAction 

class ParcelDataProcessor(QMainWindow):
    """Main window for the Parcel Data Processor application."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Parcel Data Processor")
        self.setMinimumWidth(750)
        self.base_file = r"Z:\Modeling Group\BKRCast\LandUse\Complan\Complan2044\2044LU\DT_rebalance_btw_job_category\parcels_urbansim.txt"
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

        hbox = QHBoxLayout()
        base_button = QPushButton("Select Base Parcel Data Files")
        base_button.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        base_button.clicked.connect(self.select_base_files)
        hbox.addWidget(base_button)
        self.base_file_label = QLabel("No files selected")
        hbox.addWidget(self.base_file_label)  
        main_layout.addLayout(hbox)


        hbox = QHBoxLayout()
        self.valid_btn = QPushButton("Validate")
        self.valid_btn.clicked.connect(lambda: self.validate_files(self.base_file))
        hbox.addWidget(self.valid_btn)

        summarize_btn = QPushButton("Summarize")
        summarize_btn.clicked.connect(self.summarize_files) 
        hbox.addWidget(summarize_btn)
        main_layout.addLayout(hbox)

        groupbox_container = QWidget()
        groupbox_layout = QVBoxLayout(groupbox_container)
        groupbox_layout.addWidget(QLabel("Data from Partner Cities"))
        self.list_box = QListWidget()
        self.list_box.addItem("Bellevue")
        self.list_box.addItem("Kirkland")
        self.list_box.addItem("Redmond")
        groupbox_layout.addWidget(self.list_box)
        groupbox_container.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)
        main_layout.addWidget(groupbox_container)



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

    def select_base_files(self):

        filename, _ = QFileDialog.getOpenFileName(self, "Select Base Parcel Data File", "", "Text Files (*.txt);;All Files (*)")
        if not filename:
            return
        else:
            self.base_file = filename
            self.base_file_label.setText(filename)

    def validate_files(self, filename):
        self.status_section1.setText("running")
        self.valid_btn.setEnabled(False)

        self.worker = ValidationThread(self, filename)
        self.worker.finished.connect(lambda validate_dict: self._on_thread_finished(self.valid_btn, validate_dict))
        self.worker.error.connect(lambda message: self._on_thread_error(self.valid_btn, self.status_section1, message))
        self.worker.start()
        
    def _on_thread_finished(self, btn, validate_dict):
        # called when the thread is finished
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

        data_df = pd.read_csv(filename, sep = " ", low_memory = False)
        header = ["Column", "Data Type", "Unique Values", "Missing Values", "Duplicated", "Min", "Max", "Mean"]
        for col in data_df.columns:
            series = data_df[col]
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

        df = pd.DataFrame(output_list, columns = header)

        df2 = pd.DataFrame([{"Rows": data_df.shape[0], "Columns": data_df.shape[1]}])
        validation_dict = {
            "Validation": df,
            "Summary": df2
        }
        
        return validation_dict


    def summarize_files(self):
        if not self.base_file:
            QMessageBox.warning(self, "Warning", "Please select a base parcel data file first.")
            return
        # Add summarization logic here
        QMessageBox.information(self, "Info", "Summarization completed successfully.")


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