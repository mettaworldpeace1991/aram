import os
import sys
import logging
import argparse
import pandas as pd
from pathlib import Path
from pandas.core.frame import DataFrame
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.common.settings import LOGGING_CONFIG

logging.basicConfig(**LOGGING_CONFIG)
logger = logging.getLogger(__name__)


class DataFrameProcessor:
    """
    A class to process data frames for JTL (JMeter Test Logs) files.

    This class reads a JTL file, performs data indexing, filtering, and saving the processed data
    into a new file. It handles path validation, file reading, data indexing based on timestamps,
    and saving the filtered data frame in the Feather format.

    Attributes:
        file_path (Path): The file path for the input JTL file.
        output_path (Path): The file path where the processed data frame will be saved.
        data_frame (DataFrame, optional): The pandas DataFrame loaded from the JTL file.
    """

    def __init__(self, file_path: Path, output_path: Path):
        """
        Initialize the DataFrameProcessor with file paths for input and output.

        Args:
            file_path (Path): Path to the input JTL file.
            output_path (Path): Path for saving the processed data frame.
        """
        self.file_path = file_path
        self.output_path = output_path
        self.data_frame = None

        self._validate_paths()

    def _validate_paths(self):
        """
        Validates the existence of the input file and output directory.

        Raises:
            FileNotFoundError: If the input JTL file is not found.
            IsADirectoryError: If the input file path is a directory.
        """
        if not self.file_path.exists():
            logger.error(f"JTL file does not exist: {self.file_path}")
            raise FileNotFoundError(f"JTL file not found: {self.file_path}")

        if not self.output_path.parent.exists():
            logger.info(
                f"Creating directory for output file: {self.output_path.parent}"
            )
            self.output_path.parent.mkdir(parents=True)

    def process_data_frame(self):
        """
        Processes the data frame by reading, indexing, filtering, and saving operations.
        """
        self._read_and_index_data()
        self._filter_data_frame()
        self._save_data_frame()

    def _read_and_index_data(self):
        """
        Reads and indexes the data from the JTL file.

        Raises:
            Exception: If there is an error in reading the file.
        """
        logger.info(f"Reading and indexing data from {self.file_path}")
        if self.file_path.is_dir():
            logger.error(
                f"The provided path is a directory, not a file: {self.file_path}"
            )
            raise IsADirectoryError(
                f"Expected a file, got a directory: {self.file_path}"
            )
        try:
            df = pd.read_csv(self.file_path, on_bad_lines="skip")
            self.data_frame = self._indexing_data(df)
            logger.info("Data read and indexed successfully")
        except Exception as e:
            logger.error(f"Failed to read and index data: {e}")
            raise

    def _indexing_data(self, df: DataFrame) -> DataFrame:
        """
        Indexes the DataFrame based on the 'timeStamp' column.

        Args:
            df (DataFrame): The DataFrame to be indexed.

        Returns:
            DataFrame: The indexed DataFrame.
        """
        logger.info("Indexing data frame")
        df = df.set_index(["timeStamp"])
        try:
            df.index = pd.to_datetime(df.index, unit="ms")
        except ValueError:
            df.index = pd.to_datetime(df.index)
        logger.info("Indexing completed")
        return df

    def _filter_data_frame(self):
        """
        Filters out rows from the DataFrame where the timestamp's year is not 1970.
        """
        logger.info("Filtering data frame")
        self.data_frame = self.data_frame[self.data_frame.index.year != 1970]
        logger.info("Filtering completed")

    def _save_data_frame(self):
        """
        Saves the processed DataFrame to the specified output path in Feather format.

        Raises:
            Exception: If there is an error in saving the file.
        """
        logger.info(f"Saving data frame to {self.output_path}")
        try:
            self.data_frame.to_feather(self.output_path)
            logger.info("Data frame saved successfully")
        except Exception as e:
            logger.error(f"Error saving data frame: {e}")
            raise


def main():
    """
    Main function to parse command line arguments and initiate data frame processing.
    Parses arguments for the JTL file path and the output file path, and then processes
    the JTL file using DataFrameProcessor.
    """
    parser = argparse.ArgumentParser(description="Process and filter JTL files.")
    parser.add_argument(
        "--jtl_file_path",
        type=Path,
        required=True,
        help="Full path to the joined JTL file",
    )
    parser.add_argument(
        "--output_file_path",
        type=Path,
        required=True,
        help="Path to save the processed DataFrame",
    )
    args = parser.parse_args()

    try:
        processor = DataFrameProcessor(args.jtl_file_path, args.output_file_path)
        processor.process_data_frame()
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise


if __name__ == "__main__":
    main()
