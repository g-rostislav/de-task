import time
import pandas as pd
from pathlib import Path
from multiprocessing import Pool

def process_claims(claims_json: Path):
    dtype = {
        "id": "object",
        "ndc": "str",
        "npi": "str",
        "price": "float64",
        "timestamp": "datetime64[ns]",
        "quantity": "float64",
    }
    claims = pd.read_json(claims_json, dtype=dtype)
    claims.set_index("id", inplace=True)

    invalid_claims = claims[
        (claims["quantity"].isna() | (claims["quantity"] < 0)) |
        (claims["price"].isna() | (claims["price"] < 0))
    ]

    if not invalid_claims.empty:
        claims = claims.drop(index=invalid_claims.index)

    return (
        claims.groupby(["ndc", "quantity"]).agg(cnt_qty_entries=("quantity", "count"))
    )


def top_prescribed_qty(input_claims_dir: Path, workers: int, top_n: int, output_path: Path) -> None:
    """
    Processes claims data to find the most prescribed quantities for each National Drug Code (NDC).

    This function performs the following steps:
        1. Preprocesses the claim files in the specified input directory using multiple workers in parallel.
        2. Groups the claims by NDC and quantity, aggregating the count of quantity entries.
        3. For each NDC, identifies the top `top_n` most prescribed quantities based on the count of quantity entries.
        4. Saves the results to a JSON file at the specified output path.

    Args:
        input_claims_dir: The directory containing claim files to be processed.
        workers         : The number of worker processes to use for parallel processing.
        top_n           : The number of top prescribed quantities to extract for each NDC.
        output_path     : The path where the resulting data should be saved in JSON format.

    Returns:
        The function does not return any value but saves the results to the specified output path.
    """

    t1 = time.time()
    with Pool(workers) as pool:
        preprocessed_claims = pool.map(process_claims, input_claims_dir.iterdir())
        print(f"Task4: Initial preprocessing done in {round(time.time() - t1, 4)} sec")

    t2 = time.time()
    result = (
        pd.concat(preprocessed_claims)
        .groupby(["ndc", "quantity"])
        .agg(cnt_qty_entries=("cnt_qty_entries", "sum"))
        .reset_index()
        .groupby("ndc")
        .apply(lambda x: x.nlargest(top_n, "cnt_qty_entries")["quantity"].tolist())
        .reset_index(name="most_prescribed_quantity")
    )

    result.to_json(output_path, orient="records", indent=1)
    print(f"Task4: Union and save result done in {round(time.time() - t2, 4)} sec")