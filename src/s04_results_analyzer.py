import os
import sys
import json
import logging
import argparse
import pandas as pd
from pathlib import Path
from typing import Dict, List
from pandas.core.frame import DataFrame
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.common.range_models import TestTimes, TimeRange
from src.common.range_models import GBEncoder


def calculate_test(
    data_frame: DataFrame, test_times: TestTimes, unique_labels: List[str], freq: str
) -> Dict:
    """
    Perform a comprehensive analysis for the given test data.
    Parameters:
    data_frame (DataFrame): Input DataFrame containing test data.
    test_times (TestTimes): TestTimes object containing test time data.
    unique_labels (List[str]): List of unique labels in the DataFrame.
    freq (str): Frequency string for resampling time-series data.
    Returns:
    Dict: A dictionary containing the analysis results.
    """
    descriptive_analysis_results = {}
    for range_obj in test_times.get_all_ranges():
        descriptive_analysis_results[range_obj.full_range_name] = {}
        range_data = calculate_range(range_obj, data_frame, unique_labels, freq)
        descriptive_analysis_results[range_obj.full_range_name] = range_data
        logging.info(range_obj.full_range_name, "completed")
    return descriptive_analysis_results


def calculate_range(
    range_obj: TimeRange, test_data_frame: DataFrame, unique_labels: str, freq: str
):
    """
    Calculate summary statistics for a given time range within a DataFrame.
    Parameters:
    range_obj (TimeRange): TimeRange object representing the range of interest.
    test_data_frame (DataFrame): Input DataFrame to calculate statistics on.
    unique_labels (str): Unique labels in the DataFrame.
    freq (str): Frequency string for resampling time-series data.
    Returns:
    Dict: A dictionary containing summary statistics.
    """
    range_data_frame = test_data_frame.copy()
    range_data_frame = range_data_frame.loc[
        (range_data_frame.index.asi8 > range_obj.start_time.epoch)
        & (range_data_frame.index.asi8 < range_obj.end_time.epoch)
    ]
    range_data = {}
    range_data["summary_range_results"] = calculate_data_frame(range_data_frame, freq)
    range_data["by_transactions_range_results"] = {}
    for label_name in unique_labels:
        range_data["by_transactions_range_results"][label_name.strip()] = (
            calculate_transaction(range_data_frame, label_name, freq)
        )
    return range_data


def calculate_transaction(range_data_frame: DataFrame, label_name: str, freq: str):
    """
    Calculate summary statistics for a given label within a DataFrame.
    Parameters:
    range_data_frame (DataFrame): Input DataFrame to calculate statistics on.
    label_name (str): Label name to filter the DataFrame.
    freq (str): Frequency string for resampling time-series data.
    Returns:
    Dict: A dictionary containing summary statistics.
    """
    df_label = range_data_frame[range_data_frame["label"].isin([label_name])]
    transaction_data = calculate_data_frame(df_label, freq, label_name)
    return transaction_data


def calculate_data_frame(data_frame: DataFrame, freq: str, label_name=None):
    """
    Calculate summary statistics for a given DataFrame.
    Parameters:
    data_frame (DataFrame): Input DataFrame to calculate statistics on.
    freq (str): Frequency string for resampling time-series data.
    Returns:
    Dict: A dictionary containing summary statistics if the data_frame is not empty, otherwise None.
    """
    summary_count = len(data_frame.index)
    success_data = dict(data_frame["success"].value_counts())
    if summary_count != 0:
        calculated_data = {
            "sampler_count": summary_count,
            "success": success_data.get(True, 0),
            "failures": success_data.get(False, 0),
            "avg-min": round(data_frame["elapsed"].min()),
            "avg-max": round(data_frame["elapsed"].max()),
            "avg-rt": round(data_frame["elapsed"].mean()),
            "p25": round(data_frame["elapsed"].quantile(0.25)),
            "p50": round(data_frame["elapsed"].quantile(0.50)),
            "p75": round(data_frame["elapsed"].quantile(0.75)),
            "p90": round(data_frame["elapsed"].quantile(0.90)),
            "p92": round(data_frame["elapsed"].quantile(0.92)),
            "p95": round(data_frame["elapsed"].quantile(0.95)),
            "p98": round(data_frame["elapsed"].quantile(0.98)),
            "p99": round(data_frame["elapsed"].quantile(0.99)),
        }
        success_data = dict(data_frame["success"].value_counts())
        calculated_data.update(
            {
                "error_percent": round(
                    ((success_data.get(False, 0) / summary_count) * 100), 2
                ),
                "success_percent": round(
                    ((success_data.get(True, 0) / summary_count) * 100), 2
                ),
            }
        )
        if label_name is not None:
            filtered_data_frame = data_frame.drop(labels=["responseCode"], axis=1)
            series = calculate_series(filtered_data_frame, freq)
            calculated_data["series"] = series
        return calculated_data
    else:
        return None


def calculate_series(data_frame: DataFrame, freq: str):
    series_data = data_frame.groupby(
        pd.Grouper(
            freq=freq,
            offset="0s",
            label="right",
        )
    ).mean(numeric_only=True)
    series_data_dict = {}
    for i in range(len(series_data)):
        row = series_data.iloc[i]
        series_data_dict[str(row.name)] = {}
        for column_name in series_data.columns:
            series_data_dict[str(row.name)][column_name] = round(row[column_name], 2)
    return series_data_dict


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--results_file_path",
        type=Path,
        default=Path("results.json"),
        help="Path to the results file",
    )
    parser.add_argument(
        "--data_frame_file_path",
        type=Path,
        default=Path("full_test_data_frame.feather"),
        help="Path to the data_frame file",
    )

    args = parser.parse_args()

    RESULTS_PATH = args.results_file_path
    DATA_FRAME_PATH = args.data_frame_file_path

    with open(RESULTS_PATH, "r") as file:
        results = json.load(file)

    test_times_dict = results.get("test_times", {})

    test_times = TestTimes.from_dict(test_times_dict)
    unique_labels = results.get("unique_labels", [])

    data_frame = pd.read_feather(DATA_FRAME_PATH)

    descriptive_analysis_results = calculate_test(
        data_frame=data_frame,
        test_times=test_times,
        unique_labels=unique_labels,
        freq="30s",
    )

    test_data = {}
    test_data["test_times"] = test_times_dict
    test_data["unique_labels"] = unique_labels
    test_data["descriptive_analysis"] = descriptive_analysis_results

    with open(RESULTS_PATH, "w") as data_file:
        json.dump(test_data, data_file, indent=4, cls=GBEncoder)

    logging.info("Analyzed test data successfully saved to JSON file {RESULTS_PATH}")


if __name__ == "__main__":
    main()
