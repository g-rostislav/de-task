import pandas as pd
from pathlib import Path


def calculate_lowest_avg_prices(
        metrics_json_path: Path,
        input_pharmacies_dir: Path,
        output_path: Path,
        top_n: int = 2
) -> None:

    """
    Calculates the lowest average prices for each pharmacy chain from the provided metrics and pharmacy data.

    This function merges pharmacy data with metrics (task2.py solution), selects the top `top_n` lowest average prices for each `ndc`,
    and stores the result in a JSON file with `ndc` and corresponding pharmacy chains (with their names and prices).

    Args:
        metrics_json_path    : Path to the JSON file containing the metrics data.
        input_pharmacies_dir : Path to the directory containing CSV files with pharmacy data.
        output_path          : Path to the output JSON file where the result will be saved.
        top_n                : The number of lowest average prices to return for each `ndc`. Defaults to 2.

    Returns:
        The function saves the result to a JSON file and does not return any value.
    """

    pharmacies = pd.concat(
        pd.read_csv(csv_path, dtype={"chain": "object", "npi": "object"}) for csv_path in input_pharmacies_dir.iterdir()
    ).drop_duplicates(["chain", "npi"])
    pharmacies.columns = ["name", "npi"]

    metrics = pd.read_json(metrics_json_path, dtype={"npi": "object", "ndc": "object"})

    report = metrics.merge(pharmacies, on="npi", how='inner')

    report = (
        report
        .drop_duplicates(subset=['ndc', 'avg_price'])
        .groupby("ndc").apply(lambda x: x.nsmallest(top_n, 'avg_price'))
        .reset_index(drop=True)
        .groupby("ndc").apply(lambda x: x[['name', 'avg_price']].to_dict(orient='records'))
        .reset_index()
    )
    report.columns = ["ndc", "chain"]
    report.to_json(output_path, orient="records", indent=1)