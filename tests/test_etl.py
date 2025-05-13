# tests/test_etl.py
import pandas as pd
from src.extract import ExcelExtractor
from src.transform import DataTransformer
from src.load import DuckDBLoader
import duckdb
import pytest

# --- Tests for Extract ---
def test_excel_extractor_single_file(tmp_path):
    # Create a temporary Excel file for testing
    test_file = tmp_path / "test.xlsx"
    pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']}).to_excel(test_file, index=False)
    extractor = ExcelExtractor(str(test_file))
    data = extractor.extract_data()
    assert len(data) == 1
    assert isinstance(data[0], pd.DataFrame)
    assert list(data[0].columns) == ['col1', 'col2']

def test_excel_extractor_multiple_files(tmp_path):
    file1 = tmp_path / "test1.xlsx"
    file2 = tmp_path / "test2.xlsx"
    pd.DataFrame({'col_a': [10], 'col_b': ['x']}).to_excel(file1, index=False)
    pd.DataFrame({'col_c': [20], 'col_d': ['y']}).to_excel(file2, index=False)
    extractor = ExcelExtractor([str(file1), str(file2)])
    data = extractor.extract_data()
    assert len(data) == 2
    assert isinstance(data[0], pd.DataFrame)
    assert isinstance(data[1], pd.DataFrame)

# --- Tests for Transform ---
def test_data_transformer_clean_column_names():
    transformer = DataTransformer()
    df = pd.DataFrame({'Column One': [1], 'Column Two': ['a']})
    cleaned_df = transformer.clean_column_names(df.copy())
    assert list(cleaned_df.columns) == ['column_one', 'column_two']

def test_data_transformer_convert_data_types():
    transformer = DataTransformer()
    df = pd.DataFrame({'num': ['1', '2'], 'letter': [3, 4]})
    type_mapping = {'num': int, 'letter': str}
    converted_df = transformer.convert_data_types(df.copy(), type_mapping)
    assert converted_df['num'].dtype == int
    assert converted_df['letter'].dtype == object # Pandas stores strings as object

# --- Tests for Load ---
def test_duckdb_loader_connect_and_load():
    loader = DuckDBLoader(':memory:') # Use in-memory for testing
    assert loader.connect()
    test_df = pd.DataFrame({'id': [1, 2], 'value': ['p', 'q']})
    assert loader.load_data('test_table', test_df)
    con = duckdb.connect(':memory:')
    result = con.execute("SELECT * FROM test_table").fetchall()
    assert result == [(1, 'p'), (2, 'q')]
    loader.close()
    con.close()