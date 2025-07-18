# Single-file Parquet Viewer using Python, Polars, and PySide6

# --- Part 1: Dependency Checker ---
import sys
import signal
import os
import subprocess
import importlib.util

def check_and_install_dependencies():
    """Checks for necessary packages and instructs the user on how to install them."""
    required_packages = {
        'PySide6': 'PySide6',
        'polars': 'polars'
    }
    missing_packages = []

    for package_name, import_name in required_packages.items():
        if importlib.util.find_spec(import_name) is None:
            missing_packages.append(package_name)

    if missing_packages:
        print("--- Missing Required Libraries ---")
        print("This application requires some Python libraries to run.")
        print("Please install them by running the following command in your terminal:")
        print(f"\n  pip install {' '.join(missing_packages)}\n")
        sys.exit(1)

# Run the dependency check immediately
check_and_install_dependencies()


# --- Part 2: Main Application ---
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QTableView, QFileDialog, QSplitter, QListWidget,
    QListWidgetItem, QLabel, QAbstractItemView
)
from PySide6.QtCore import QAbstractTableModel, Qt, QModelIndex, QSettings, QDir
import polars as pl

PAGE_SIZE = 10000

class PolarsTableModel(QAbstractTableModel):
    """A Qt table model to efficiently display a Polars DataFrame."""
    def __init__(self, data: pl.DataFrame, row_offset: int):
        super().__init__()
        self._data = data
        self._row_offset = row_offset

    def rowCount(self, parent=QModelIndex()):
        return self._data.height

    def columnCount(self, parent=QModelIndex()):
        return self._data.width

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid() or role != Qt.DisplayRole:
            return None
        
        row, col = index.row(), index.column()
        value = self._data[row, col]
        
        if value is None:
            return ""
        return str(value)

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal:
                return self._data.columns[section]
            if orientation == Qt.Vertical:
                return str(self._row_offset + section + 1)
        return None

class ParquetViewer(QMainWindow):
    """The main application window."""
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Parquet Viewer")
        # Set a default geometry for the very first run
        self.setGeometry(100, 100, 1200, 800)

        # Data state
        self.df = None
        self.current_offset = 0

        # --- Main Layout ---
        self.central_widget = QWidget()
        self.main_layout = QVBoxLayout(self.central_widget)
        self.setCentralWidget(self.central_widget)

        # --- Top Controls ---
        controls_layout = QHBoxLayout()
        self.btn_open = QPushButton("Open Parquet File")
        self.btn_prev = QPushButton("Previous")
        self.btn_next = QPushButton("Next")
        self.status_label = QLabel("No file loaded.")

        self.btn_open.clicked.connect(self.open_file)
        self.btn_prev.clicked.connect(self.go_previous)
        self.btn_next.clicked.connect(self.go_next)

        controls_layout.addWidget(self.btn_open)
        controls_layout.addWidget(self.btn_prev)
        controls_layout.addWidget(self.btn_next)
        controls_layout.addStretch()
        controls_layout.addWidget(self.status_label)
        self.main_layout.addLayout(controls_layout)

        # --- Splitter for Column List and Table View ---
        self.splitter = QSplitter(Qt.Horizontal)
        self.main_layout.addWidget(self.splitter, 1)

        # Left side: Column List
        self.column_list = QListWidget()
        self.column_list.itemClicked.connect(self.scroll_to_column)
        self.splitter.addWidget(self.column_list)

        # Right side: Table View
        self.table_view = QTableView()
        self.table_view.setAlternatingRowColors(True)
        self.table_view.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table_view.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.splitter.addWidget(self.table_view)
        
        self.update_button_state()

        # NEW: Load window/splitter geometry after UI is constructed
        self.load_settings()

    def load_settings(self):
        """Loads window and splitter geometry from QSettings."""
        settings = QSettings()
        geometry = settings.value("geometry")
        if geometry:
            self.restoreGeometry(geometry)
        
        splitter_state = settings.value("splitterState")
        if splitter_state:
            self.splitter.restoreState(splitter_state)
        else:
            # Provide a default splitter size for the first run
            self.splitter.setSizes([200, 1000])

    def closeEvent(self, event):
        """Saves window and splitter geometry to QSettings on close."""
        # NEW: This method is called automatically when the window is closed.
        settings = QSettings()
        settings.setValue("geometry", self.saveGeometry())
        settings.setValue("splitterState", self.splitter.saveState())
        super().closeEvent(event)

    def open_file(self):
        """Opens a file dialog, remembering the last used directory."""
        settings = QSettings()
        last_dir = settings.value("last_directory", QDir.homePath())

        file_path, _ = QFileDialog.getOpenFileName(
            self, "Open Parquet File", last_dir, "Parquet Files (*.parquet *.pq)"
        )
        
        if file_path:
            directory = os.path.dirname(file_path)
            settings.setValue("last_directory", directory)
            self.load_parquet_data(file_path)

    def load_parquet_data(self, file_path):
        try:
            self.df = pl.read_parquet(file_path)
            self.current_offset = 0
            
            self.column_list.clear()
            for col_name in self.df.columns:
                self.column_list.addItem(QListWidgetItem(col_name))
            
            self.update_table_view()
        except Exception as e:
            self.status_label.setText(f"Error: {e}")
            self.df = None
        
        self.update_button_state()

    def update_table_view(self):
        if self.df is None:
            self.status_label.setText("No file loaded.")
            return

        page_df = self.df.slice(self.current_offset, PAGE_SIZE)
        self.model = PolarsTableModel(page_df, self.current_offset)
        self.table_view.setModel(self.model)

        start_row = self.current_offset + 1
        end_row = min(self.current_offset + PAGE_SIZE, self.df.height)
        self.status_label.setText(
            f"Showing rows {start_row:,} - {end_row:,} of {self.df.height:,}"
        )
        self.update_button_state()

    def go_previous(self):
        self.current_offset = max(0, self.current_offset - PAGE_SIZE)
        self.update_table_view()

    def go_next(self):
        if self.df is not None and (self.current_offset + PAGE_SIZE < self.df.height):
            self.current_offset += PAGE_SIZE
            self.update_table_view()

    def scroll_to_column(self, item: QListWidgetItem):
        if self.df is None:
            return
            
        column_name = item.text()
        try:
            col_index = self.df.columns.index(column_name)
            self.table_view.scrollTo(self.model.index(0, col_index))
            self.table_view.selectColumn(col_index)
        except ValueError:
            pass

    def update_button_state(self):
        has_data = self.df is not None
        self.btn_prev.setEnabled(has_data and self.current_offset > 0)
        self.btn_next.setEnabled(
            has_data and (self.current_offset + PAGE_SIZE < self.df.height)
        )

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    
    app = QApplication(sys.argv)
    
    app.setOrganizationName("io.github.jac241")
    app.setApplicationName("ParquetViewer")
    
    viewer = ParquetViewer()
    viewer.show()
    sys.exit(app.exec())
