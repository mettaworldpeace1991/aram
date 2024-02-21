import os
from uuid import uuid4
from pathlib import Path
import pytest
from src.s01_jtl_joiner import JTLJoiner


@pytest.fixture
def test_data_path():
    return Path(os.path.dirname(__file__), "test_data", "s01_jtl_joiner", "JTLs")


@pytest.fixture
def export_file_path():
    return Path(
        os.path.dirname(__file__),
        "test_data",
        "s01_jtl_joiner",
        "results",
        f"results-{str(uuid4())}.csv",
    )


def test_e2e(test_data_path, export_file_path):
    joiner = JTLJoiner(test_data_path, "jtl", export_file_path)
    result_file = joiner.process_files()

    assert result_file.exists(), f"Expected result file at {result_file} not found."

    with open(result_file, "r") as file:
        lines = file.readlines()

        # Ensure the combined file has the expected number of lines
        expected_line_count = 44
        actual_line_count = len(lines)
        assert actual_line_count == expected_line_count, (
            f"Expected {expected_line_count} lines in {export_file_path}, "
            f"but found {actual_line_count}. Check the file for details."
        )

        # Extract labels from each line and collect them in a set
        labels = set()
        for line in lines[1:]:  # Skip the header line
            parts = line.strip().split(",")
            if len(parts) > 1:
                labels.add(parts[-1])  # Assume the label is the last element

        # Check that all 5 unique labels are present
        expected_label_count = 5
        actual_label_count = len(labels)
        assert actual_label_count == expected_label_count, (
            f"Expected {expected_label_count} unique labels in {export_file_path}, "
            f"but found {actual_label_count} ({labels}). Check the file for label details."
        )

    # Clean up: remove the result file after test
    os.remove(result_file)
