import time
from pathlib import Path
from multiprocessing import Pool

import pandas as pd
# from .utils import memory_info


def process_pharmacies(input_pharmacies_dir: Path) -> pd.DataFrame:

    pharmacies = pd.concat(
        pd.read_csv(csv_path, dtype={"chain": "object", "npi": "object"})
        for csv_path in input_pharmacies_dir.iterdir()
    ).drop_duplicates(["npi"])[["npi"]]

    return pharmacies


def process_reverts(input_reverts_dir: Path) -> pd.DataFrame:

    union_reverts = pd.concat(
        [pd.read_json(json_path) for json_path in list(input_reverts_dir.iterdir())]
    )
    union_reverts["reverted"] = 1
    union_reverts.set_index("claim_id", inplace=True)

    return union_reverts


def process_claims(claims_json: Path, reverts: pd.DataFrame, pharmacies: pd.DataFrame) -> pd.DataFrame:
    # print(f"WORKER --> {memory_info()}, step='START', json='{claims_json}'")
    dtype = {
        "id": "object",
        "ndc": "str",
        "npi": "str",
        "price": "float64",
        "timestamp": "datetime64[ns]",
        "quantity": "float64",
    }
    claims = pd.read_json(claims_json, dtype=dtype)

    # Exclude NPI which is not from pharmacies list
    claims = claims.merge(pharmacies, on="npi", how="inner")

    # Exclude invalid rows
    invalid_claims = claims[
        (claims["quantity"].isna() | (claims["quantity"] < 0)) |
        (claims["price"].isna() | (claims["price"] < 0))
    ]
    if not invalid_claims.empty:
        invalid_claims.to_json(f"./output/invalid_claims/{claims_json.name}", orient="records")
        claims = claims.drop(index=invalid_claims.index)

    # Enrich data from reverts
    claims_enriched = pd.merge(claims, reverts, left_on="id", right_on="claim_id", how='left')
    claims_enriched["reverted"] = claims_enriched["reverted"].fillna(0)
    claims_enriched.loc[claims_enriched["reverted"] == 1, ["quantity", "price"]] = None

    claims_agg = (
        claims_enriched
        .groupby(["npi", "ndc"])
        .agg(
            reverted=("reverted", "sum"),
            fills=("quantity", "count"),
            sum_qty=("quantity", "sum"),
            total_price=("price", "sum")
        ))
    # print(f"WORKER --> {memory_info()}, step='FINISH', json='{claims_json}'")
    return claims_agg

def calculate_metrics(
        input_claims_dir: Path,
        input_reverts_dir: Path,
        input_pharmacies_dir: Path,
        workers: int,
        output_path: Path
) -> None:

    """
    Processes and aggregates claims data with associated reverts, calculating metrics for each `npi` and `ndc`.

    This function performs preprocessing of claims data in parallel, aggregates the claims by `npi` and `ndc`,
    and calculates key metrics such as reverted counts, total fills, sum of quantities, total price, and average price.

    The resulting aggregated data is saved to a JSON file.

    Args:
        input_claims_dir : Path to the directory containing claims data in JSON format.
        input_reverts_dir: Path to the directory containing reverts data.
        input_pharmacies_dir: Path to the directory container (npi, chain) data.
        workers          : The number of worker processes to use for parallel processing of claims data.
        output_path      : Path where the resulting aggregated data will be saved in JSON format.


    Returns:
        The function saves the result to a JSON file and does not return any value.
    """

    # print(f"MAIN: {memory_info()}")
    reverts: pd.DataFrame = process_reverts(input_reverts_dir)
    pharmacies: pd.DataFrame = process_pharmacies(input_pharmacies_dir)

    t1 = time.time()
    with Pool(workers) as pool:
        preprocessed_claims = pool.starmap(
            process_claims, [(json_path, reverts, pharmacies) for json_path in input_claims_dir.iterdir()]
        )
        print(f"Task2: Initial preprocessing done in {round(time.time() - t1, 4)} sec")

    t2 = time.time()
    result = (
        pd.concat(preprocessed_claims)
        .groupby(["npi", "ndc"])
        .agg(
            reverted=("reverted", "sum"),
            fills=("fills", "size"),
            sum_qty=("sum_qty", "sum"),
            total_price=("total_price", "sum"),
        )
    )
    result["avg_price"] = result["total_price"] / result["sum_qty"]
    result.drop("sum_qty", axis=1, inplace=True)
    result.reset_index(inplace=True)

    result.to_json(output_path, orient="records", indent=1)
    print(f"Task2: Union and save result done in {round(time.time() - t2, 4)} sec")