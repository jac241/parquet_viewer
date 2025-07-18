# Parquet Viewer

A simple, fast, desktop GUI application for viewing and analyzing Parquet files, built with Python, Polars, and PySide6. This tool is designed to handle large datasets efficiently by loading data in pages and providing a responsive user experience.

## Features

This application combines a powerful data processing backend with a user-friendly and feature-rich graphical interface.

### Core Functionality
*   **Fast Data Loading**: Utilizes the high-performance Polars library to read Parquet files instantly.
*   **Large File Support**: Handles very large datasets by loading and displaying data in configurable pages (defaulting to 10,000 rows at a time).
*   **Pagination Controls**: Navigate through large files with "Next" and "Previous" buttons.

### User Experience & Workflow
*   **Persistent Session**: The application remembers its window size, position, and panel layout between sessions.
*   **File History**: A "File" menu provides quick access to the last 10 unique files you've opened.
*   **Quick Reopen**: A dedicated "Reopen Last" button on the toolbar displays the last viewed filename and allows you to reopen it instantly.
*   **Session Restore**: When reopening the last file, the view is automatically restored to the exact page you were on.
*   **Smart File Dialog**: The "Open..." dialog remembers the last directory you opened a file from.
*   **Live Reload**: Automatically detects when the currently viewed file is modified on disk and prompts you to reload it.
*   **Keyboard Shortcuts**: Navigate pages efficiently with cross-platform shortcuts (`Cmd/Ctrl+N` for Next, `Cmd/Ctrl+P` for Previous).

### Data Analysis & Navigation
*   **Draggable Split View**: A resizable splitter separates the column list and the main data table, allowing you to easily view long column names.
*   **Column Quick-Find**: Click any column in the list on the left to instantly scroll the main table horizontally to that column and highlight it.
*   **Value Counts Analysis**: Right-click any column header to open a modal dialog showing a summary of its unique values, their counts, and their percentage frequency, sorted from most to least common.
*   **Clean Data Display**: Missing or null values are displayed as clean, empty cells instead of "None".

### Self-Contained and Easy to Share
*   **Single-File Script**: The entire application is contained in a single Python file.
*   **Automatic Dependency Check**: If required libraries (`polars`, `PySide6`) are not installed, the script will print a friendly message with the exact command needed to install them.

## Requirements

*   Python 3.8 or newer
*   `pip` (Python's package installer)

## Installation & Usage

1.  **Clone the repository:**
    ```bash
    git clone <your-repository-url>
    cd parquet-viewer
    ```

2.  **Run the application:**
    ```bash
    python pview.py
    ```
    *   If you are missing the required libraries, the script will exit and print a `pip install ...` command. Simply copy, paste, and run that command, then try running the application again.

---

## For Developers: Running Tests

This project includes a comprehensive test suite to ensure functionality and prevent regressions.

1.  **Install testing dependencies:**
    ```bash
    pip install pytest pytest-qt unittest-mock
    ```

2.  **Run the test suite:**
    From the root directory of the project, simply run `pytest`:
    ```bash
    pytest
    ```
    All tests should pass, confirming that the application and its features are working as expected.

## Technology Stack

*   **Backend**: Python 3
*   **Data Processing**: [Polars](https://www.pola.rs/)
*   **GUI Framework**: [PySide6](https://www.qt.io/qt-for-python) (Qt6 bindings)
*   **Testing**: [pytest](https://pytest.org/) and [pytest-qt](https://pytest-qt.readthedocs.io/)

## License

This project is licensed under the MIT License.```
