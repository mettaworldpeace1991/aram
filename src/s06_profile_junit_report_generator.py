import os
import argparse
import pandas as pd
import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from xml.dom import minidom
from pandas.core.frame import DataFrame


def get_xml_report(data_frame: DataFrame):
    """
    Generate an XML report based on the provided DataFrame.

    Args:
        data_frame (DataFrame): The DataFrame containing the performance test data.

    Returns:
        str: The generated XML report as a string.
    """
    root = ET.Element("testsuites")
    testrun = ET.SubElement(
        root,
        "testsuite",
        name="Performance Test",
        tests=str(len(data_frame)),
        failures="0",
        errors="0",
        skipped="0",
    )

    for index, row in data_frame.iterrows():
        label = ET.SubElement(
            testrun,
            "testcase",
            name=row["label"],
            actual_rps=str(row["actual, rps"]),
            target_rps=str(row["target, rps"]),
        )
        if not row["meets_target_profile"]:
            failure = ET.SubElement(label, "failure")
            failure.text = f"The actual transaction's execution intensity is {row['actual, rps']} rps, that lower than the target intensity  - {row['target, rps']} rps"

    xml_str = ET.tostring(root, encoding="unicode", method="xml")
    xml_str = minidom.parseString(xml_str).toprettyxml(indent="    ")

    return xml_str


def main():
    """
    Parse command line arguments and execute the main functionality of the script.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--artifacts_path",
        type=Path,
        default=Path("shared", "test_artifacts"),
        help="Path to save test artifacts",
    )
    parser.add_argument(
        "--profile_dataframe_file_path",
        type=Path,
        default=Path("profile.feather"),
        help="Path to the profile dataframe file",
    )
    parser.add_argument(
        "--profile_junit_report_file_name",
        type=Path,
        default=Path("profile.xml"),
        help="Name of the profile junit report file",
    )
    args = parser.parse_args()

    ARTIFACTS_PATH = args.artifacts_path
    PROFILE_FEATHER = args.profile_dataframe_file_path
    REPORT_NAME = args.profile_junit_report_file_name

    input_path = os.path.join(ARTIFACTS_PATH, PROFILE_FEATHER)
    data_frame = pd.read_feather(input_path)
    report = get_xml_report(data_frame)

    output_path = os.path.join(ARTIFACTS_PATH, REPORT_NAME)
    with open(output_path, "w") as f:
        f.write(report)

    logging.info(
        f"Load profile xml-report successfully saved to feather file: {output_path}"
    )


if __name__ == "__main__":
    main()
