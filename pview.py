# pview.py (Main Application)

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
    QListWidgetItem, QLabel, QAbstractItemView, QMessageBox
)
from PySide6.QtGui import QAction, QKeySequence
from PySide6.QtCore import (
    QAbstractTableModel, Qt, QModelIndex, QSettings, QDir, QFileSystemWatcher
)
import polars as pl

PAGE_SIZE = 10000
MAX_RECENT_FILES = 10

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
        self.setGeometry(100, 100, 1200, 800)

        # Data and state
        self.df = None
        self.current_offset = 0
        self.current_file_path = None
        self.current_file_mtime = None

        self.file_watcher = QFileSystemWatcher(self)
        self.file_watcher.fileChanged.connect(self.handle_file_change)

        self.create_menus()
        self.setup_ui()
        self.load_settings()

    def setup_ui(self):
        """Constructs the main UI layout and widgets."""
        self.central_widget = QWidget()
        self.main_layout = QVBoxLayout(self.central_widget)
        self.setCentralWidget(self.central_widget)

        controls_layout = QHBoxLayout()
        self.btn_open = QPushButton("Open Parquet File")
        self.btn_reopen = QPushButton("Reopen Last")
        self.btn_prev = QPushButton("Previous")
        self.btn_next = QPushButton("Next")
        self.status_label = QLabel("No file loaded.")

        self.btn_open.clicked.connect(self.open_file)
        self.btn_reopen.clicked.connect(self.reopen_last_file)
        self.btn_prev.clicked.connect(self.go_previous)
        self.btn_next.clicked.connect(self.go_next)

        controls_layout.addWidget(self.btn_open)
        controls_layout.addWidget(self.btn_reopen)
        controls_layout.addWidget(self.btn_prev)
        controls_layout.addWidget(self.btn_next)
        controls_layout.addStretch()
        controls_layout.addWidget(self.status_label)
        self.main_layout.addLayout(controls_layout)

        self.splitter = QSplitter(Qt.Horizontal)
        self.main_layout.addWidget(self.splitter, 1)

        self.column_list = QListWidget()
        self.column_list.itemClicked.connect(self.scroll_to_column)
        self.splitter.addWidget(self.column_list)

        self.table_view = QTableView()
        self.table_view.setAlternatingRowColors(True)
        self.table_view.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table_view.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.splitter.addWidget(self.table_view)
        
        self.update_button_state()

    def create_menus(self):
        file_menu = self.menuBar().addMenu("&File")
        open_action = QAction("&Open...", self)
        open_action.setShortcut(QKeySequence.Open)
        open_action.triggered.connect(self.open_file)
        file_menu.addAction(open_action)
        file_menu.addSeparator()
        self.recent_files_menu = file_menu.addMenu("&Recent Files")
        self.update_recent_files_menu()

    def update_recent_files_menu(self):
        settings = QSettings()
        recent_files = settings.value("recentFiles", [], type=list)
        self.recent_files_menu.clear()
        if not recent_files:
            no_recent_action = QAction("No Recent Files", self)
            no_recent_action.setEnabled(False)
            self.recent_files_menu.addAction(no_recent_action)
            return
        for file_path in recent_files:
            action = QAction(file_path, self)
            action.triggered.connect(lambda checked=False, path=file_path: self.load_parquet_data(path))
            self.recent_files_menu.addAction(action)
        self.recent_files_menu.addSeparator()
        clear_action = QAction("Clear List", self)
        clear_action.triggered.connect(self.clear_recent_files)
        self.recent_files_menu.addAction(clear_action)

    def clear_recent_files(self):
        settings = QSettings()
        settings.setValue("recentFiles", [])
        self.update_recent_files_menu()

    def add_to_recent_files(self, file_path):
        settings = QSettings()
        recent_files = settings.value("recentFiles", [], type=list)
        if file_path in recent_files:
            recent_files.remove(file_path)
        recent_files.insert(0, file_path)
        del recent_files[MAX_RECENT_FILES:]
        settings.setValue("recentFiles", recent_files)

    def load_settings(self):
        settings = QSettings()
        geometry = settings.value("geometry")
        if geometry:
            self.restoreGeometry(geometry)
        splitter_state = settings.value("splitterState")
        if splitter_state:
            self.splitter.restoreState(splitter_state)
        else:
            self.splitter.setSizes([200, 1000])
        self.update_reopen_button_state()

    def update_reopen_button_state(self):
        settings = QSettings()
        last_file = settings.value("lastFilePath")
        if last_file and os.path.exists(last_file):
            filename = os.path.basename(last_file)
            self.btn_reopen.setText(f"Reopen: {filename}")
            self.btn_reopen.setEnabled(True)
        else:
            self.btn_reopen.setText("Reopen Last")
            self.btn_reopen.setEnabled(False)

    def closeEvent(self, event):
        settings = QSettings()
        settings.setValue("geometry", self.saveGeometry())
        settings.setValue("splitterState", self.splitter.saveState())
        if self.current_file_path:
            settings.setValue("lastFilePath", self.current_file_path)
            settings.setValue("lastFileOffset", self.current_offset)
        super().closeEvent(event)

    def reopen_last_file(self):
        settings = QSettings()
        last_file = settings.value("lastFilePath")
        last_offset = settings.value("lastFileOffset", 0, type=int)
        if last_file and os.path.exists(last_file):
            self.load_parquet_data(last_file, offset=last_offset)
        else:
            QMessageBox.warning(self, "File Not Found", "The last opened file could not be found.")
            self.update_reopen_button_state()

    def handle_file_change(self, path):
        if path != self.current_file_path or self.current_file_path is None:
            return
        try:
            new_mtime = os.path.getmtime(path)
            if new_mtime == self.current_file_mtime:
                return
            reply = QMessageBox.question(
                self, "File Modified",
                f"The file '{os.path.basename(path)}' has been modified.\n\nDo you want to reload it?",
                QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes
            )
            if reply == QMessageBox.Yes:
                self.load_parquet_data(path, offset=self.current_offset)
            else:
                self.current_file_mtime = new_mtime
        except FileNotFoundError:
            self.file_watcher.removePath(path)
            self.status_label.setText(f"File not found: {os.path.basename(path)}")

    def open_file(self):
        settings = QSettings()
        last_dir = settings.value("last_directory", QDir.homePath())
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Open Parquet File", last_dir, "Parquet Files (*.parquet *.pq)"
        )
        if file_path:
            directory = os.path.dirname(file_path)
            settings.setValue("last_directory", directory)
            self.load_parquet_data(file_path)

    def load_parquet_data(self, file_path, offset=0):
        if self.current_file_path and self.current_file_path in self.file_watcher.files():
            self.file_watcher.removePath(self.current_file_path)
        try:
            self.df = pl.read_parquet(file_path)
            self.current_offset = offset
            self.current_file_path = file_path
            self.current_file_mtime = os.path.getmtime(file_path)
            self.file_watcher.addPath(self.current_file_path)
            
            # THE FIX: Immediately save the successfully loaded file as the "last file".
            settings = QSettings()
            settings.setValue("lastFilePath", self.current_file_path)
            
            self.column_list.clear()
            for col_name in self.df.columns:
                self.column_list.addItem(QListWidgetItem(col_name))
            self.add_to_recent_files(file_path)
            self.update_recent_files_menu()
            self.update_table_view()
        except Exception as e:
            self.status_label.setText(f"Error loading {os.path.basename(file_path)}: {e}")
            self.df = None
        
        self.update_button_state()
        self.update_reopen_button_state()

    def update_table_view(self):
        if self.df is None:
            self.status_label.setText("No file loaded.")
            self.table_view.setModel(None)
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
