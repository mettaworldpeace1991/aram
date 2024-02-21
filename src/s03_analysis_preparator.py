import json
import argparse
import logging
import sys
import os
from typing import List
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.common.range_models import *
from pathlib import Path
from pandas.core.frame import DataFrame
from datetime import datetime, timedelta

def get_unique_labels(data_frame: DataFrame) -> List[str]:
    """
    Retrieves a list of unique labels from the 'label' column of the DataFrame.

    Parameters:
    - data_frame (DataFrame): The DataFrame from which to retrieve labels.

    Returns:
    - List[str]: A list of unique labels.
    """
    labels = list(data_frame['label'].unique())
    labels = list(filter(lambda x: x is not None, labels))
    return labels

def get_test_times(
        current_datetime: datetime,
        test_end_datetime: datetime,
        full_test_duration_seconds: int,
        ramp_up_time_seconds: int,
        impact_time_seconds: int,
        ranges_count: int,
        duration_range_seconds: int,
        ramp_down_seconds: int
    ) -> TestTimes:
        """
        Calculates test times based on the DataFrame and provided parameters.

        Parameters:
        - current_datetime (datetime): Test's start time
        - test_end_datetime (datetime): Test's end time
        - full_test_duration_seconds (int): Full test's duration in seconds
        - ramp_up_time_seconds (int): The ramp-up time in seconds.
        - impact_time_seconds (int): The impact time in seconds.
        - ranges_count (int): The number of ranges.
        - duration_range_seconds (int): The duration of each range in seconds.
        - ramp_down_seconds (int): The ramp-down time in seconds.

        Returns:
        - TestTimes: The calculated test times.
        """
        full_test_range = TimeRange(
            duration_in_seconds=full_test_duration_seconds,
            full_range_name='full_test',
            short_range_name='FT',
            start_time=TimeFormat(current_datetime),
            end_time=TimeFormat(test_end_datetime),
        )

        end_ramp_up_datetime = current_datetime + timedelta(seconds=int(ramp_up_time_seconds))

        ramp_up_range = TimeRange(
            duration_in_seconds=ramp_up_time_seconds,
            full_range_name='ramp_up',
            short_range_name='RU',
            start_time=TimeFormat(current_datetime),
            end_time=TimeFormat(end_ramp_up_datetime),
        )

        end_impact_datetime = end_ramp_up_datetime + timedelta(seconds=int(impact_time_seconds))
        current_datetime = end_ramp_up_datetime

        impact_range = TimeRange(
            duration_in_seconds=impact_time_seconds,
            full_range_name='impact',
            short_range_name='IT',
            start_time=TimeFormat(end_ramp_up_datetime),
            end_time=TimeFormat(end_impact_datetime),
        )

        ranges_list: list[AssessmentRange] = []
        assessment_range_step: int = 0
        for range_number in range(1, int(ranges_count) + 1):
            end_range_datetime = current_datetime + timedelta(seconds=int(duration_range_seconds))
            assessment_range_number = str(range_number).zfill(2)
            ranges_list.append(
                AssessmentRange(
                    duration_in_seconds=duration_range_seconds,
                    full_range_name=f'AssessmentRange-{assessment_range_number}',
                    short_range_name=f'R{assessment_range_number}',
                    range_number=range_number,
                    start_time=TimeFormat(current_datetime),
                    end_time=TimeFormat(end_range_datetime),
                )
            )
            current_datetime = end_range_datetime
            assessment_range_step = assessment_range_step + int(duration_range_seconds)

        end_ramp_down_datetime = current_datetime + \
            timedelta(seconds=int(ramp_down_seconds))
        ramp_down_range = TimeRange(
            duration_in_seconds=ramp_down_seconds,
            full_range_name='ramp_down',
            short_range_name='RD',
            start_time=TimeFormat(current_datetime),
            end_time=TimeFormat(end_ramp_down_datetime),
        )

        test_time = TestTimes(
            ramp_up=ramp_up_range,
            impact=impact_range,
            duration_ranges=ranges_list,
            ramp_down=ramp_down_range,
            full_test=full_test_range,
        )
        return test_time

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_frame_file_path", type=Path, default=Path("full_test_data_frame.feather"), help="Path to the data_frame file")
    parser.add_argument("--ramp_up_time_seconds", type=str, default="600", help="Ramp up test time seconds")
    parser.add_argument("--impact_time_seconds", type=str, default="600", help="Impact test time seconds")
    parser.add_argument("--ranges_count", type=str, default="1", help="Test ranges count")
    parser.add_argument("--duration_range_seconds", type=str, default="600", help="Duration range seconds")
    parser.add_argument("--ramp_down_seconds", type=str, default="30", help="Ramp down seconds")
    parser.add_argument("--results_file_path", type=Path, default=Path("results.json"), help="Path to the resulting analysis json file")

    args = parser.parse_args()

    PATH = args.data_frame_file_path
    RAMP_UP_TIME_SECONDS = abs(int(args.ramp_up_time_seconds))
    IMPACT_TIME_SECONDS = abs(int(args.impact_time_seconds))
    RANGES_COUNT = abs(int(args.ranges_count))
    DURATION_RANGE_SECONDS =abs(int(args.duration_range_seconds))
    RAMP_DOWN_SECONDS = abs(int(args.ramp_down_seconds))

    data_frame = pd.read_feather(PATH)
    TEST_START_DATETIME: datetime = data_frame.index.min()
    TEST_END_DATETIME: datetime = data_frame.index.max()
    FULL_TEST_DURATION_SECONDS = int(TEST_END_DATETIME.timestamp()) - int(TEST_START_DATETIME.timestamp())

    RESULTS_FILE = args.results_file_path

    unique_labels = get_unique_labels(data_frame)

    test_times = get_test_times(
        current_datetime=TEST_START_DATETIME,
        test_end_datetime=TEST_END_DATETIME,
        full_test_duration_seconds=FULL_TEST_DURATION_SECONDS,
        ramp_up_time_seconds=RAMP_UP_TIME_SECONDS,
        impact_time_seconds=IMPACT_TIME_SECONDS,
        ranges_count=RANGES_COUNT,
        duration_range_seconds=DURATION_RANGE_SECONDS,
        ramp_down_seconds=RAMP_DOWN_SECONDS,
    )

    test_data = {}
    test_data['test_times'] = test_times
    test_data['unique_labels'] = unique_labels
    result_file_path = Path(RESULTS_FILE)

    with open(result_file_path, 'w') as data_file:
        json.dump(test_data, data_file, indent=4, cls=GBEncoder)
    logging.info("Prepared test data successfully saved to JSON file {RESULTS_FILE}")

if __name__ == "__main__":
    main()