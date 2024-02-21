import os
from pathlib import Path
import pandas as pd
import pytest
from uuid import uuid4
from src.s02_data_frame_compiler import DataFrameProcessor

sample_jtl_file = Path("tests", "test_data", "s02_data_frame_compiler", "sample.jtl")
results_path = Path("tests", "test_data", "s02_data_frame_compiler", "results")

if not results_path.exists():
    os.makedirs(results_path)


@pytest.fixture
def export_file_path():
    return results_path / f"results_{uuid4()}.feather"


def test_e2e(export_file_path):
    processor = DataFrameProcessor(sample_jtl_file, export_file_path)
    processor.process_data_frame()
    assert (
        export_file_path.exists()
    ), f"Expected result file at {export_file_path} not found."
    processed_data = pd.read_feather(export_file_path)

    # Check the number of rows in the processed data
    expected_row_count = 10
    actual_row_count = processed_data.shape[0]
    assert (
        actual_row_count == expected_row_count
    ), f"Expected {expected_row_count} rows, but found {actual_row_count} rows in the processed data."

    os.remove(export_file_path)
