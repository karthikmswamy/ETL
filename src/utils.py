# src/utils.py
import configparser
import ast

def read_config(config_file):
    """
    Reads configuration from the specified INI file.

    Args:
        config_file (str): Path to the configuration file.

    Returns:
        dict: A dictionary containing the configuration parameters.
              Returns an empty dictionary if the file is not found.
    """
    config = configparser.ConfigParser()
    try:
        config.read(config_file)
        # Safely evaluate string representations of dictionaries and lists
        configuration = {
            "database": {
                "duckdb_file": config.get("Database", "duckdb_file"),
                "target_table": config.get("Database", "target_table"),
            },
            "files": {
                "excel_files_pattern": config.get("Files", "excel_files_pattern"),
            },
            "transformation": {
                "column_mapping": ast.literal_eval(config.get("Transformation", "column_mapping")),
                "data_types": ast.literal_eval(config.get("Transformation", "data_types")),
                "sheet_name": config.get("Transformation", "sheet_name")
            },
        }
        return configuration
    except (configparser.Error, FileNotFoundError) as e:
        print(f"Error reading configuration: {e}")
        return {}  # Return an empty dictionary to indicate failure
