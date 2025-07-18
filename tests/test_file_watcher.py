# tests/test_file_watcher.py
import sys
import os
import polars as pl
import pytest
from unittest.mock import patch
from PySide6.QtWidgets import QMessageBox

# Add project root to the Python path to allow importing the application
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pview import ParquetViewer # Import the application class

@pytest.fixture
def parquet_file(tmp_path):
    """A pytest fixture to create a temporary parquet file."""
    file_path = tmp_path / "test.parquet"
    df1 = pl.DataFrame({"a": [1, 2, 3]})
    df1.write_parquet(file_path)
    return file_path

@patch('pview.QMessageBox.question', return_value=QMessageBox.Yes)
def test_auto_reload_on_file_change(mock_question, qtbot, parquet_file):
    """
    Tests if the application logic to prompt the user is called and that
    the file is reloaded upon a "Yes" response.
    """
    # 1. Setup the application and load the initial file
    viewer = ParquetViewer()
    qtbot.addWidget(viewer)
    viewer.show()

    viewer.load_parquet_data(str(parquet_file))
    
    # Assert initial state
    assert viewer.df is not None
    assert viewer.df.height == 3
    assert "Showing rows 1 - 3 of 3" in viewer.status_label.text()

    # 2. Modify the file on disk
    # We must wait to ensure the OS modification time is different
    qtbot.wait(1100)
    
    df2 = pl.DataFrame({"a": [1, 2, 3, 4, 5], "b": [6, 7, 8, 9, 10]})
    df2.write_parquet(str(parquet_file))

    # 3. Wait until our mock question function has been called
    # THE FIX: Wrap the check in a lambda to make it a callable function.
    qtbot.waitUntil(lambda: mock_question.called, timeout=3000)

    # 4. Assert that the prompt was indeed shown
    mock_question.assert_called_once()

    # 5. Assert the final state (the file should have been reloaded)
    assert viewer.df.height == 5
    assert viewer.df.width == 2
    assert "Showing rows 1 - 5 of 5" in viewer.status_label.text()
    assert "b" in viewer.df.columns
