# src/extract.py
import pandas as pd


class ExcelExtractor:
    """
    Class to extract data from Excel files.
    """

    def __init__(self, file_paths):
        if isinstance(file_paths, str):
            self.file_paths = [file_paths]
        else:
            self.file_paths = file_paths

    def extract_data(self, sheet_name=0):
        """
        Extract data from Excel files.
        """
        all_data = []
        for file_path in self.file_paths:
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                all_data.append(df)
            except FileNotFoundError:
                print(f"Error: File not found at {file_path}")
            except ValueError:
                print(f"Error: Sheet '{sheet_name}' not found in {file_path}")
        return all_data

    # Add more methods for specific extraction needs if required
