# src/transform.py
import pandas as pd


class DataTransformer:
    """
    Class to transform data.
    """

    def clean_column_names(self, df: pd.DataFrame):
        """Method to clean column names.


        Args:
            df (pd.DataFrame): Pandas dataframe to be transformed.


        Returns:
            pd.DataFrame: Dataframe with cleaned column names.
        """
        df.columns = df.columns.str.lower().str.replace(" ", "_")
        return df

    def convert_data_types(self, df: pd.DataFrame, type_mapping: dict):
        """Method to convert data types."""
        try:
            df = df.astype(type_mapping)
        except KeyError as e:
            print(f"Warning: Column '{e}' not found in DataFrame for type conversion.")
        except ValueError as e:
            print(f"Error during type conversion: {e}")
        return df

    # Add more transformation methods as needed
