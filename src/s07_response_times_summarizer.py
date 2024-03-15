import os
import json
import argparse
import psycopg2
import logging
import pandas as pd
from pathlib import Path
from pandas.core.frame import DataFrame


def get_required_response_times_from_db(
    db_name: str,
    db_user: str,
    db_password: str,
    db_hostname: str,
    db_port: str,
    db_tablename: str,
):
    """
    Retrieve the required response times from the specified database table.

    Args:
        db_name (str): The name of the database.
        db_user (str): The username for database authentication.
        db_password (str): The password for database authentication.
        db_hostname (str): The hostname of the database server.
        db_port (str): The port number of the database server.
        db_tablename (str): The name of the table containing the required response times.

    Returns:
        list: A list of tuples containing the required response times data.
    """
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_hostname,
        port=db_port,
    )
    cur = conn.cursor()
    query = f"SELECT url, rt_90_percentile, rt_95_percentile, rt_99_percentile FROM {db_tablename}"
    cur.execute(query=query)
    rows = cur.fetchall()
    return rows


def transform_to_dataframe(response_times_list: list):
    """
    Transform the list of required response times to a DataFrame.

    Args:
        response_times_list (list): A list of tuples containing the required response times data.

    Returns:
        DataFrame: A DataFrame containing the required response times data.
    """
    columns = ["label", "req_p90", "req_p95", "req_p99"]
    return pd.DataFrame(response_times_list, columns=columns)


def collect_general_dataframe(
    descriptive_analysis_results: dict,
    reqired_response_times_df: DataFrame,
    acceptable_deviation: float,
):
    """
    Collect and process general data from the descriptive analysis results and required response times DataFrame.

    Args:
        descriptive_analysis_results (dict): The descriptive analysis results.
        reqired_response_times_df (DataFrame): The required response times DataFrame.
        acceptable_deviation (float): The acceptable deviation from the required response times.

    Returns:
        DataFrame: A DataFrame containing the processed data.
    """
    labels = descriptive_analysis_results["impact"]["by_transactions_range_results"]
    response_times_list_p90 = []
    response_times_list_p95 = []
    response_times_list_p99 = []
    for index, row in reqired_response_times_df.iterrows():
        logging.info(
            f"Index: {index}, Label: {row['label']}, p90/ms: {row['req_p90']}, p95/ms: {row['req_p95']}, p99/ms: {row['req_p99']}"
        )
        label = labels[row["label"]]
        percentile_90 = float(label["p90"])
        percentile_95 = float(label["p95"])
        percentile_99 = float(label["p99"])
        response_times_list_p90.append(percentile_90)
        response_times_list_p95.append(percentile_95)
        response_times_list_p99.append(percentile_99)

    reqired_response_times_df["act_p90"] = response_times_list_p90
    reqired_response_times_df["act_p95"] = response_times_list_p95
    reqired_response_times_df["act_p99"] = response_times_list_p99

    reqired_response_times_df = reqired_response_times_df.round(2)

    reqired_response_times_df["meets_required_resp_time"] = reqired_response_times_df[
        "act_p95"
    ] < (reqired_response_times_df["req_p95"] * (1.00 - acceptable_deviation))

    return reqired_response_times_df


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
        "--artifacts_path",
        type=Path,
        default=Path("shared", "test_artifacts"),
        help="Path to save test artifacts",
    )
    parser.add_argument(
        "--response_times_dataframe_path",
        type=Path,
        default=Path("resp_times.feather"),
        help="Path to the response times dataframe file",
    )
    parser.add_argument(
        "--acceptable_deviation",
        type=float,
        default=0.02,
        help="Acceptable deviation from the load test profile",
    )
    parser.add_argument(
        "--db_response_times_sla_tablename", type=str, default="response_times", help="The name of the table with response times SLA"
    )
    parser.add_argument(
        "--db_credentials", type=str, help="Credentials to access to DB"
    )
    args = parser.parse_args()

    RESULTS_PATH = args.results_file_path
    ARTIFACTS_PATH = args.artifacts_path
    RESPONSE_TIMES_FEATHER = args.response_times_dataframe_path
    ACCEPTABLE_DEVIATION = args.acceptable_deviation
    DB_RESPONSE_TIMES_SLA_TABLENAME = args.db_response_times_sla_tablename
    DB_HOSTNAME = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_NAME = os.getenv("DB_NAME")

    response_times_list = get_required_response_times_from_db(
        DB_NAME, DB_USER, DB_PASSWORD, DB_HOSTNAME, DB_PORT, DB_RESPONSE_TIMES_SLA_TABLENAME
    )

    reqired_response_times_df = transform_to_dataframe(response_times_list)

    input_path = os.path.join(ARTIFACTS_PATH, RESULTS_PATH)

    with open(input_path, "r") as file:
        results = json.load(file)

    descriptive_analysis = results.get("descriptive_analysis")

    general_response_times_df = collect_general_dataframe(
        descriptive_analysis, reqired_response_times_df, ACCEPTABLE_DEVIATION
    )

    output_path = os.path.join(ARTIFACTS_PATH, RESPONSE_TIMES_FEATHER)
    general_response_times_df.to_feather(output_path)

    logging.info(
        f"Response times summary data successfully saved to feather file: {output_path}"
    )


if __name__ == "__main__":
    main()
