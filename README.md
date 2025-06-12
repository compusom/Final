# Rep11-06-2025

Reporte 11/06/2025

## Purpose

This repository serves as the starting point for a small application that will generate and store periodic reports. It currently contains only this documentation, but future commits will introduce code to collect data, process it, and publish consolidated reports.

## Intended Functionality

The planned functionality includes:

1. Scripts to gather raw data from manual input or automated sources.
2. Modules that transform and validate the collected information.
3. Commands that output readable summaries in text or other formats.

## Future Structure

Although the project is in its earliest stage, the following directory layout is expected:

```
src/    - core processing code
data/   - sample inputs and generated reports
tests/  - unit tests for each module
```

Developers are encouraged to follow standard Python packaging conventions when adding new modules.

## Usage Instructions

1. Clone the repository:
   ```bash
   git clone <repository-url>
   ```
2. Create and activate a virtual environment (optional but recommended):
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   ```
3. Install the required dependencies from the provided `requirements.txt` file:
   ```bash
   pip install -r requirements.txt
   ```
4. Run the example scripts that will be added under `src/` as the project evolves.

## Running Tests

After installing dependencies you can run the unit tests with:

```bash
pytest
```

When generating a monthly bitácora report through the GUI you can now choose
how many complete months to compare (between 2 and the number of detected
months). Use the spinbox in the Bitácora configuration section to select the
desired number of months.

## Report Format

Generated reports are plain text files where tables use a semicolon (`;`) as the delimiter for each cell. This ensures data can be imported directly into spreadsheet tools. See `docs/report_format.md` for details.

## Contributing

Pull requests that introduce useful scripts, improve documentation, or help define the project structure are welcome. Feel free to open issues to discuss ideas or report problems.
