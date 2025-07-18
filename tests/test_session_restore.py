# tests/test_session_restore.py
import sys
import os
import polars as pl
import pytest
from PySide6.QtCore import QSettings, Qt

# Add project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pview import ParquetViewer, PAGE_SIZE

@pytest.fixture
def large_parquet_file(tmp_path):
    """Creates a parquet file with more than one page of data."""
    file_path = tmp_path / "large_test.parquet"
    # Create enough rows for at least two pages
    num_rows = PAGE_SIZE + 5 
    df = pl.DataFrame({"id": range(num_rows)})
    df.write_parquet(file_path)
    return file_path

def test_reopen_last_file_and_restores_position(qtbot, large_parquet_file):
    """
    Tests that the app saves the last file/offset on close and restores
    it when the "Reopen Last" button is clicked.
    """
    # Clear settings before running the test to ensure a clean state
    QSettings().clear()

    # --- Phase 1: Run, navigate, and close the app to save state ---
    viewer1 = ParquetViewer()
    qtbot.addWidget(viewer1)
    viewer1.show()
    
    # Load data and go to the next page
    viewer1.load_parquet_data(str(large_parquet_file))
    qtbot.mouseClick(viewer1.btn_next, Qt.LeftButton)

    # Assert we are on the second page
    assert viewer1.current_offset == PAGE_SIZE
    
    # Close the viewer, which triggers closeEvent to save settings
    viewer1.close()

    # --- Phase 2: Re-launch and test the "Reopen" functionality ---
    viewer2 = ParquetViewer()
    qtbot.addWidget(viewer2)
    viewer2.show()
    
    # Assert the reopen button is enabled
    assert viewer2.btn_reopen.isEnabled()
    
    # Click the reopen button
    qtbot.mouseClick(viewer2.btn_reopen, Qt.LeftButton)

    # Assert the state was correctly restored
    assert viewer2.current_file_path == str(large_parquet_file)
    assert viewer2.current_offset == PAGE_SIZE
    
    # Check the status label to confirm the UI reflects the restored state
    expected_start_row = PAGE_SIZE + 1
    expected_status_text = f"Showing rows {expected_start_row:,}"
    assert expected_status_text in viewer2.status_label.text()

def test_reopen_button_text_and_state(qtbot, large_parquet_file):
    """
    Tests that the "Reopen Last" button text and enabled state update correctly.
    """
    # Clear settings to ensure a predictable starting state
    QSettings().clear()
    
    # 1. Test initial state: No last file exists
    viewer_initial = ParquetViewer()
    qtbot.addWidget(viewer_initial)
    viewer_initial.show()
    
    assert viewer_initial.btn_reopen.text() == "Reopen Last"
    assert not viewer_initial.btn_reopen.isEnabled()
    viewer_initial.close()

    # 2. Test state after loading a file
    viewer_after_load = ParquetViewer()
    qtbot.addWidget(viewer_after_load)
    viewer_after_load.show()
    
    viewer_after_load.load_parquet_data(str(large_parquet_file))
    
    filename = os.path.basename(str(large_parquet_file))
    assert viewer_after_load.btn_reopen.text() == f"Reopen: {filename}"
    assert viewer_after_load.btn_reopen.isEnabled()
    
    # 3. Test state after closing and relaunching
    viewer_after_load.close() # Saves state
    
    viewer_relaunch = ParquetViewer()
    qtbot.addWidget(viewer_relaunch)
    viewer_relaunch.show()
    
    assert viewer_relaunch.btn_reopen.text() == f"Reopen: {filename}"
    assert viewer_relaunch.btn_reopen.isEnabled()
    viewer_relaunch.close()
    
    # 4. Test state after the file has been deleted
    os.remove(str(large_parquet_file))
    
    viewer_after_delete = ParquetViewer()
    qtbot.addWidget(viewer_after_delete)
    viewer_after_delete.show()

    assert viewer_after_delete.btn_reopen.text() == "Reopen Last"
    assert not viewer_after_delete.btn_reopen.isEnabled()
    viewer_after_delete.close()
