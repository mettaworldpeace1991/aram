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
        data_frame (DataFrame): The DataFrame containing the response times data.

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
            actual_rps=str(row["act_p95"]),
            target_rps=str(row["req_p95"]),
        )
        if not row["meets_required_resp_time"]:
            failure = ET.SubElement(label, "failure")
            failure.text = f"The actual transaction's execution time is {row['act_p95']} ms, that lower than the required execution time - {row['req_p95']} ms"

    xml_str = ET.tostring(root, encoding="unicode", method="xml")
    xml_str = minidom.parseString(xml_str).toprettyxml(indent="    ")

    return xml_str


def main():
    """
    Parse command line arguments and execute the main functionality of the script.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--response_times_dataframe_path",
        type=Path,
        default=Path("resp_times.feather"),
        help="Path to the response times dataframe file",
    )
    parser.add_argument(
        "--response_times_junit_report_file_name",
        type=Path,
        default=Path("response_times.xml"),
        help="Name of the response times junit report file",
    )
    args = parser.parse_args()

    RESPONSE_TIMES_FEATHER = args.response_times_dataframe_path
    REPORT_NAME = args.response_times_junit_report_file_name

    data_frame = pd.read_feather(RESPONSE_TIMES_FEATHER)
    report = get_xml_report(data_frame)

    with open(REPORT_NAME, "w") as f:
        f.write(report)

    logging.info(
        f"Response times xml-report successfully saved to feather file: {REPORT_NAME}"
    )


if __name__ == "__main__":
    main()
