# src/load.py
import duckdb
import pandas as pd


class DuckDBLoader:
    """
    Class to load data into DuckDB.
    """

    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = None

    def connect(self):
        """Method to connect to DuckDB.

        Returns:
            _type_: _description_
        """
        try:
            self.conn = duckdb.connect(database=self.db_path, read_only=False)
            return True
        except Exception as e:
            print(f"Error connecting to DuckDB: {e}")
            return False

    def close(self):
        """Method to close the connection to DuckDB."""
        if hasattr(self, "conn") and self.conn:
            self.conn.close()

    def create_table(self, table_name: str):
        """Method to create a table in DuckDB.


        Args:
            table_name (str): Name of the table to be created.

        Returns: None
        """
        if not hasattr(self, "conn") or not self.conn:
            print("Error: Not connected to DuckDB. Call connect() first.")
            return False
        try:
            # Infer DuckDB schema from Pandas DataFrame
            self.conn.execute(
                f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df"
            )
            return True
        except Exception as e:
            print(f"Error creating table '{table_name}': {e}")
            return False

    def load_data(self, table_name: str, df: pd.DataFrame, if_exists="append"):
        """Method to load data into a table in DuckDB."""
        if not hasattr(self, "conn") or not self.conn:
            print("Error: Not connected to DuckDB. Call connect() first.")
            return False
        try:
            self.conn.register("df", df)  # Register DataFrame for easy querying
            if if_exists == "replace":
                self.conn.execute(f"REPLACE INTO {table_name} SELECT * FROM df")
            elif if_exists == "append":
                self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
            else:
                print(
                    f"Warning: Invalid if_exists value '{if_exists}'. Using 'append'."
                )
                self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
            self.conn.unregister("df")  # Unregister the DataFrame
            return True
        except Exception as e:
            print(f"Error loading data into table '{table_name}': {e}")
            return False
