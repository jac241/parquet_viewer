# tests/test_value_counts.py
import sys
import os
import polars as pl
import pytest
from unittest.mock import patch
from PySide6.QtWidgets import QTableView
from PySide6.QtCore import Qt, QPoint

# Add project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# Import pview AFTER adding to path so it can be patched
import pview

@pytest.fixture
def value_counts_file(tmp_path):
    """Creates a parquet file with a predictable column for testing."""
    file_path = tmp_path / "counts_test.parquet"
    df = pl.DataFrame({"category": ["A", "B", "A"], "numbers": [1, 2, 3]})
    df.write_parquet(file_path)
    return file_path

@patch('pview.ParquetViewer.show_header_context_menu')
def test_right_click_triggers_context_menu_handler(mock_show_menu, qtbot, value_counts_file):
    """
    Tests that a right-click on a header correctly triggers the
    `show_header_context_menu` method with the appropriate position.
    """
    viewer = pview.ParquetViewer()
    qtbot.addWidget(viewer)
    viewer.show()
    
    viewer.load_parquet_data(str(value_counts_file))
    
    header = viewer.table_view.horizontalHeader()
    click_pos = QPoint(header.sectionPosition(0) + 5, 5)
    
    header.customContextMenuRequested.emit(click_pos)
    
    mock_show_menu.assert_called_once_with(click_pos)
    
    mock_show_menu.reset_mock()
    
    click_pos_col2 = QPoint(header.sectionPosition(1) + 5, 5)
    header.customContextMenuRequested.emit(click_pos_col2)
    
    mock_show_menu.assert_called_once_with(click_pos_col2)

def test_value_counts_dialog_logic(qtbot):
    """
    Tests the internal logic of the Value Counts dialog (sorting, data)
    without involving the context menu.
    """
    # 1. Create a sample DataFrame
    df = pl.DataFrame({
        "category": ["C", "A", "B", "A", "C", "A", "B", "A", None, "A"]
    })
    
    # 2. Instantiate the main window to be the parent
    viewer = pview.ParquetViewer()
    viewer.df = df # Manually set the dataframe for the test
    
    # This list will capture the instance of the dialog created inside the function
    captured_instances = []
    
    # Keep a reference to the original __init__ method
    original_init = pview.ValueCountsDialog.__init__
    
    # Define our new constructor that "spies" on the instance
    def new_init(self, *args, **kwargs):
        captured_instances.append(self)      # Capture the instance
        original_init(self, *args, **kwargs) # Call the real constructor

    # 3. Patch the constructor to spy on it, and patch exec to prevent blocking
    with patch('pview.ValueCountsDialog.__init__', new=new_init), \
         patch('pview.ValueCountsDialog.exec') as mock_exec:
        viewer.show_value_counts("category")

    # 4. Assert the dialog's `exec` was called, meaning it was shown
    mock_exec.assert_called_once()
    
    # 5. Assert that our spy captured exactly one dialog instance
    assert len(captured_instances) == 1
    dialog_instance = captured_instances[0]
    
    # 6. Inspect the dialog instance and its data
    results_df = dialog_instance.findChild(QTableView).model()._data
    
    # Add a debugging assertion to be absolutely sure
    assert isinstance(results_df, pl.DataFrame)
    
    # 7. Assert sorting and content
    expected_counts = [5, 2, 2, 1]
    actual_counts = results_df["count"].to_list()
    assert actual_counts == expected_counts, "Data is not sorted by count"
    assert results_df[0, "Value"] == "A"
    assert results_df[3, "Value"] is None
