# src/main.py
import os
from src.extract import ExcelExtractor
from src.transform import DataTransformer
from src.load import DuckDBLoader
from src.utils import read_config

def main():
    """
    Main function to execute the ETL process.
    """
    config = read_config("src/config.ini")  # Read the config file

    if not config:
        print("Configuration file not found or invalid. Exiting.")
        return

    excel_files_pattern = config["files"]["excel_files_pattern"]
    duckdb_file = config["database"]["duckdb_file"]
    target_table = config["database"]["target_table"]
    column_mapping = config["transformation"]["column_mapping"]
    data_types = config["transformation"]["data_types"]
    sheet_name = config["transformation"]["sheet_name"]

    # Extract
    excel_files = [f for f in os.listdir('data') if f.endswith('.xlsx')]
    extractor = ExcelExtractor([os.path.join('data', file) for file in excel_files])
    extracted_data = extractor.extract_data(sheet_name=sheet_name)

    if not extracted_data:
        print("No data extracted. Exiting.")
        return

    # Transform
    transformer = DataTransformer()
    transformed_data = []
    for df in extracted_data:
        df = transformer.clean_column_names(df)
        if column_mapping:
            df = df.rename(columns=column_mapping)
        if data_types:
            df = transformer.convert_data_types(df, data_types)
        transformed_data.append(df)

    if not transformed_data:
        print("No data to load after transformation. Exiting.")
        return

    # Load
    loader = DuckDBLoader(duckdb_file)
    if loader.connect():
        for df in transformed_data:
            loader.load_data(target_table, df, if_exists='append')
        loader.close()
        print(f"Data loaded successfully into '{duckdb_file}', table '{target_table}'.")
    else:
        print("Failed to connect to DuckDB. Data not loaded.")

if __name__ == "__main__":
    main()
