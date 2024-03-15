import os
import json
import argparse
import psycopg2
import pandas as pd
import logging
from pathlib import Path
from pandas.core.frame import DataFrame


def get_target_profile_from_db(
    db_name: str,
    db_user: str,
    db_password: str,
    db_hostname: str,
    db_port: str,
    db_tablename: str,
):
    """
    Retrieve the target load profile data from the specified database table.

    Args:
        db_name (str): The name of the database.
        db_user (str): The username for database authentication.
        db_password (str): The password for database authentication.
        db_hostname (str): The hostname of the database server.
        db_port (str): The port number of the database server.
        db_tablename (str): The name of the table containing the target load profile data.

    Returns:
        list: A list of tuples containing the target load profile data.
    """
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_hostname,
        port=db_port,
    )
    cur = conn.cursor()
    query = f"SELECT url, rph, rpm, rps FROM {db_tablename}"
    cur.execute(query=query)
    rows = cur.fetchall()
    return rows


def calculate_given_test_load_percentage_as_dataframe(
    profile_list: list, profile_pecentage: str
):
    """
    Calculate the target load profile at the specified percentage and return as a DataFrame.

    Args:
        profile_list (list): A list of tuples containing the target load profile data.
        profile_pecentage (str): The percentage of the target load profile to calculate.

    Returns:
        DataFrame: A DataFrame containing the calculated target load profile data.
    """
    columns = ["label", "100%, rph", "100%, rpm", "100%, rps"]
    profile_dataframe = pd.DataFrame(profile_list, columns=columns)
    percentage_multiplier = int(profile_pecentage) / 100
    profile_dataframe[f"target, rph"] = (
        profile_dataframe["100%, rph"] * percentage_multiplier
    )
    profile_dataframe[f"target, rpm"] = (
        profile_dataframe["100%, rpm"] * percentage_multiplier
    )
    profile_dataframe[f"target, rps"] = (
        profile_dataframe["100%, rps"] * percentage_multiplier
    )
    return profile_dataframe


def calculate_actual_label_intensity(sampler_count: int, impact_duration: int):
    """
    Calculate the actual label intensity based on the sampler count and impact duration.

    Args:
        sampler_count (int): The number of samplers.
        impact_duration (int): The duration of the impact in seconds.

    Returns:
        tuple: A tuple containing the calculated intensity values (rph, rpm, rps).
    """
    intensity_rps = sampler_count / impact_duration
    intensity_rpm = intensity_rps * 60
    intensity_rph = intensity_rpm * 60
    return intensity_rph, intensity_rpm, intensity_rps


def collect_general_dataframe(
    descriptive_analysis_results: dict,
    profile_data_frame: DataFrame,
    impact_duration: int,
    acceptable_deviation: float,
):
    """
    Collect and process general data from the descriptive analysis results and target profile DataFrame.

    Args:
        descriptive_analysis_results (dict): The descriptive analysis results.
        profile_data_frame (DataFrame): The target profile DataFrame.
        impact_duration (int): The duration of the impact in seconds.
        acceptable_deviation (float): The acceptable deviation from the load test profile.

    Returns:
        DataFrame: A DataFrame containing the processed data.
    """
    labels = descriptive_analysis_results["impact"]["by_transactions_range_results"]
    intensity_list_rph = []
    intensity_list_rpm = []
    intensity_list_rps = []
    for index, row in profile_data_frame.iterrows():
        logging.info(
            f"Index: {index}, Label: {row['label']}, Profile/rph: {row['target, rph']}, Profile/rpm: {row['target, rpm']}, Profile/rps: {row['target, rps']}"
        )
        label = labels[row["label"]]
        sampler_count = label["success"]
        intensity_rph, intensity_rpm, intensity_rps = calculate_actual_label_intensity(
            sampler_count, impact_duration
        )
        intensity_list_rph.append(intensity_rph)
        intensity_list_rpm.append(intensity_rpm)
        intensity_list_rps.append(intensity_rps)

    profile_data_frame["actual, rph"] = intensity_list_rph
    profile_data_frame["actual, rpm"] = intensity_list_rpm
    profile_data_frame["actual, rps"] = intensity_list_rps

    profile_data_frame = profile_data_frame.round(2)

    profile_data_frame["meets_target_profile"] = profile_data_frame["actual, rps"] > (
        profile_data_frame["target, rps"] * (1.00 - acceptable_deviation)
    )

    return profile_data_frame


def main():
    """
    Parse command line arguments and execute the main functionality of the script.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--results_file_path",
        type=Path,
        default=Path("results.json"),
        help="Path to the results file",
    )
    parser.add_argument(
        "--profile_dataframe_file_path",
        type=Path,
        default=Path("profile.feather"),
        help="Path to the profile dataframe file",
    )
    parser.add_argument("--test_profile", type=str, help="Run's profile percentage")
    parser.add_argument(
        "--artifacts_path",
        type=Path,
        default=Path("shared", "test_artifacts"),
        help="Path to save test artifacts",
    )
    parser.add_argument(
        "--acceptable_deviation",
        type=float,
        default=0.02,
        help="Acceptable deviation from the load test profile",
    )
    parser.add_argument(
        "--db_profile_sla_tablename", type=str, default="load_profile", help="The name of the table with profile SLA"
    )
    args = parser.parse_args()

    RESULTS_PATH = args.results_file_path
    PROFILE_FEATHER = args.profile_dataframe_file_path
    PROFILE_PERCENTAGE = args.test_profile
    ARTIFACTS_PATH = args.artifacts_path
    ACCEPTABLE_DEVIATION = args.acceptable_deviation
    DB_PROFILE_SLA_TABLENAME = args.db_profile_sla_tablename
    DB_HOSTNAME = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_NAME = os.getenv("DB_NAME")

    profile_list = get_target_profile_from_db(
        DB_NAME, DB_USER, DB_PASSWORD, DB_HOSTNAME, DB_PORT, DB_PROFILE_SLA_TABLENAME
    )

    profile_data_frame = calculate_given_test_load_percentage_as_dataframe(
        profile_list, PROFILE_PERCENTAGE
    )

    input_path = os.path.join(ARTIFACTS_PATH, RESULTS_PATH)

    with open(input_path, "r") as file:
        results = json.load(file)

    descriptive_analysis = results.get("descriptive_analysis")
    impact_duration = int(results["test_times"]["impact"]["duration_in_seconds"])

    df = collect_general_dataframe(
        descriptive_analysis, profile_data_frame, impact_duration, ACCEPTABLE_DEVIATION
    )

    output_path = os.path.join(ARTIFACTS_PATH, PROFILE_FEATHER)
    df.to_feather(output_path)

    logging.info(
        f"Load profile summary data successfully saved to feather file: {output_path}"
    )


if __name__ == "__main__":
    main()
