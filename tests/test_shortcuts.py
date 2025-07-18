# tests/test_shortcuts.py
import sys
import os
import polars as pl
import pytest
from PySide6.QtCore import QSettings

# Add project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pview import ParquetViewer, PAGE_SIZE

@pytest.fixture
def multi_page_file(tmp_path):
    """Creates a parquet file with more than one page of data."""
    file_path = tmp_path / "multi_page_test.parquet"
    # Create enough rows for at least two full pages
    num_rows = (PAGE_SIZE * 2) + 5 
    df = pl.DataFrame({"id": range(num_rows)})
    df.write_parquet(file_path)
    return file_path

def test_view_menu_actions_and_state(qtbot, multi_page_file):
    """
    Tests that the View menu actions trigger navigation and that their
    enabled/disabled state is correctly managed.
    """
    # Clear settings to ensure a predictable starting state
    QSettings().clear()
    
    viewer = ParquetViewer()
    qtbot.addWidget(viewer)
    viewer.show()

    # --- 1. Test Initial State (No File Loaded) ---
    assert not viewer.prev_action.isEnabled()
    assert not viewer.next_action.isEnabled()

    # --- 2. Test State on First Page ---
    viewer.load_parquet_data(str(multi_page_file))
    
    assert viewer.current_offset == 0
    assert not viewer.prev_action.isEnabled(), "Prev action should be disabled on first page"
    assert viewer.next_action.isEnabled(), "Next action should be enabled on first page"
    
    # --- 3. Trigger "Next Page" Action ---
    viewer.next_action.trigger()
    
    # Assert navigation occurred
    assert viewer.current_offset == PAGE_SIZE
    assert viewer.prev_action.isEnabled(), "Prev action should be enabled after moving next"
    assert viewer.next_action.isEnabled(), "Next action should still be enabled"
    
    # --- 4. Trigger "Previous Page" Action ---
    viewer.prev_action.trigger()
    
    # Assert navigation back to start
    assert viewer.current_offset == 0
    assert not viewer.prev_action.isEnabled(), "Prev action should be disabled again at the start"
    
    # --- 5. Test State on Last Page ---
    # Manually jump to the last page to test the 'next' disabled state
    viewer.current_offset = PAGE_SIZE * 2
    viewer.update_table_view()

    assert viewer.prev_action.isEnabled(), "Prev action should be enabled on the last page"
    assert not viewer.next_action.isEnabled(), "Next action should be disabled on the last page"
