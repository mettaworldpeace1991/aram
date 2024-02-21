import os
import sys
import argparse
import logging
import pandas as pd
from pathlib import Path
from pandas.core.frame import DataFrame
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.common.settings import LOGGING_CONFIG

logging.basicConfig(**LOGGING_CONFIG)
logger = logging.getLogger(__name__)


class JTLJoiner:
    """
    A class to join JTL files and save the combined data to a specified file path.
    """
    def __init__(
        self, kpi_files_path: Path, file_mask: str, output_file_path: Path
    ) -> None:
        if not kpi_files_path.exists() or not kpi_files_path.is_dir():
            raise ValueError(
                f"The path '{kpi_files_path}' does not exist or is not a directory."
            )
        if not isinstance(file_mask, str):
            raise ValueError("File mask must be a string.")
        _output_directory = output_file_path.parent
        if not _output_directory.exists() and str(_output_directory) != "":
            _output_directory.mkdir(parents=True, exist_ok=True)

        logger.debug("Initializing JTLJoiner")
        self._kpi_files_path = kpi_files_path
        self._file_mask = file_mask
        self._output_file_path = output_file_path

    @staticmethod
    def _file_stats(file_path: Path, df: DataFrame) -> None:
        file_size = file_path.stat().st_size
        num_rows = df.shape[0]
        logger.info(f"File: {file_path}, Size: {file_size} bytes, Rows: {num_rows}")

    def _join_kpi_jtl(self) -> DataFrame:
        kpi_files = []
        logger.debug("Scanning directory for files.")
        for file in self._kpi_files_path.rglob(f"*{self._file_mask}"):
            logger.info(f"Found file: {file}")
            kpi_files.append(file)

        dataframes = []
        for file in kpi_files:
            try:
                df = pd.read_csv(file)
                self._file_stats(file, df)
                dataframes.append(df)
            except Exception as e:
                logger.error(f"Error reading {file}: {e}")

        combined_data = pd.concat(dataframes, ignore_index=True)
        return combined_data

    def _save_kpi_jtl(self, combined_data: DataFrame) -> Path:
        try:
            combined_data.to_csv(self._output_file_path, index=False)
            logger.info(f"Saved joined data to {self._output_file_path}")
            self._file_stats(self._output_file_path, combined_data)
        except Exception as e:
            logger.error(f"Error saving file: {e}")
            return Path()
        return self._output_file_path

    def process_files(self) -> Path:
        combined_data = self._join_kpi_jtl()
        if combined_data is not None and not combined_data.empty:
            return self._save_kpi_jtl(combined_data)
        else:
            logger.error("No data combined from JTL files.")
            return Path()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--kpi_files_path",
        type=Path,
        default=Path("shared"),
        help="Path to directories with JTL files",
    )
    parser.add_argument(
        "--output_file_path",
        type=Path,
        default=Path("joined_output", "combined.jtl"),
        help="Full file path to save the combined JTL file",
    )
    parser.add_argument(
        "--file_mask", type=str, default="kpi.jtl", help="File mask to search for files"
    )
    args = parser.parse_args()

    try:
        jtl_joiner = JTLJoiner(
            args.kpi_files_path, args.file_mask, args.output_file_path
        )
        result_file = jtl_joiner.process_files()
        if result_file:
            logger.info(
                f"JTL files processed successfully. Combined file: {result_file}"
            )
        else:
            logger.info("JTL file processing failed.")
    except Exception as e:
        logger.error(f"Error in JTLJoiner: {e}")


if __name__ == "__main__":
    main()
